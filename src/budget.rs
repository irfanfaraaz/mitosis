//! RAM and disk budgets, the clone cap and the FIFO queue that creates wait in.

use crate::clone_backend::{free_space, private_size};
use crate::ctx::Ctx;
use crate::error::{ErrorCode, MitosisError, Result};
use crate::events;
use crate::proc_util::{process_start_time, rss_bytes};
use crate::state::CloneStatus;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::OwnedMutexGuard;

/// What a new clone is assumed to need when no clone is running to measure.
const DEFAULT_RAM_ESTIMATE: u64 = 64 * 1024 * 1024;
/// A waiting create looks again at least this often, in case a clone exited without a destroy.
const RECHECK: Duration = Duration::from_secs(1);
/// A create queued behind others gives up on the line after at least this long, even with no wait.
const MIN_LINE_WAIT: Duration = Duration::from_millis(500);

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct CloneUsage {
    pub id: String,
    pub name: Option<String>,
    pub template: String,
    pub rss_bytes: Option<u64>,
    pub own_disk_bytes: Option<u64>,
    pub created_at: i64,
    pub expires_at: i64,
    pub protected: bool,
    pub disk_warn: bool,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Usage {
    pub clones: Vec<CloneUsage>,
    /// Memory of the running clones (a clone still starting counts as one estimate).
    pub ram_bytes: u64,
    pub disk_bytes: u64,
    pub free_disk_bytes: u64,
    /// Creates waiting for room.
    pub queued: usize,
    pub max_clones: usize,
    pub ram_budget: Option<u64>,
    pub disk_budget: Option<u64>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Limit {
    Count,
    Ram,
    Disk,
}

impl Limit {
    fn name(self) -> &'static str {
        match self {
            Limit::Count => "count",
            Limit::Ram => "ram",
            Limit::Disk => "disk",
        }
    }
}

/// The limit a new clone would break, if any.
pub fn fits(usage: &Usage, new_ram_estimate: u64) -> Option<Limit> {
    if usage.clones.len() >= usage.max_clones {
        return Some(Limit::Count);
    }
    if usage
        .ram_budget
        .is_some_and(|b| usage.ram_bytes.saturating_add(new_ram_estimate) > b)
    {
        return Some(Limit::Ram);
    }
    if usage.disk_budget.is_some_and(|b| usage.disk_bytes >= b) {
        return Some(Limit::Disk);
    }
    None
}

/// What one more clone is expected to use: the average of the running ones.
pub fn ram_estimate(running_rss: &[u64]) -> u64 {
    if running_rss.is_empty() {
        return DEFAULT_RAM_ESTIMATE;
    }
    running_rss.iter().sum::<u64>() / running_rss.len() as u64
}

fn human(b: u64) -> String {
    match b {
        0..=999 => format!("{b} B"),
        1_000..=999_999 => format!("{:.0} kB", b as f64 / 1e3),
        1_000_000..=999_999_999 => format!("{:.0} MB", b as f64 / 1e6),
        _ => format!("{:.1} GB", b as f64 / 1e9),
    }
}

/// Reads the store and (for the resources asked for) the processes and disk. Blocking.
fn collect(ctx: &Ctx, want_ram: bool, want_disk: bool) -> Result<(Usage, u64)> {
    let cfg = ctx.config();
    let rows: Vec<_> = ctx
        .store()
        .list_clones()?
        .into_iter()
        .filter(|c| c.status != CloneStatus::Stopping)
        .collect();
    let mut clones = Vec::with_capacity(rows.len());
    for r in &rows {
        let rss = r
            .pg_pid
            .filter(|_| want_ram && r.status == CloneStatus::Ready)
            .filter(|pid| process_start_time(*pid) == r.pg_start_time)
            .and_then(rss_bytes);
        let own = want_disk.then(|| private_size(&r.dir)).flatten();
        clones.push(CloneUsage {
            id: r.id.clone(),
            name: r.name.clone(),
            template: r.template.clone(),
            rss_bytes: rss,
            own_disk_bytes: own,
            created_at: r.created_at,
            expires_at: r.expires_at,
            protected: r.protected,
            disk_warn: own.is_some_and(|b| b > cfg.clone_disk_warn),
        });
    }
    let running: Vec<u64> = clones.iter().filter_map(|c| c.rss_bytes).collect();
    let estimate = ram_estimate(&running);
    // A clone that is still starting has no reading yet, but it will use memory.
    let unread = if want_ram {
        (clones.len() - running.len()) as u64
    } else {
        0
    };
    let usage = Usage {
        ram_bytes: running
            .iter()
            .sum::<u64>()
            .saturating_add(estimate.saturating_mul(unread)),
        disk_bytes: clones.iter().filter_map(|c| c.own_disk_bytes).sum(),
        free_disk_bytes: free_space(&ctx.home.clones_dir()).unwrap_or(0),
        queued: ctx.queued.load(Ordering::SeqCst),
        max_clones: cfg.max_clones,
        ram_budget: cfg.ram_budget,
        disk_budget: cfg.disk_budget,
        clones,
    };
    Ok((usage, estimate))
}

async fn snapshot(ctx: &Arc<Ctx>, full: bool) -> Result<(Usage, u64)> {
    let c = ctx.clone();
    let (ram, disk) = {
        let cfg = ctx.config();
        (
            full || cfg.ram_budget.is_some(),
            full || cfg.disk_budget.is_some(),
        )
    };
    tokio::task::spawn_blocking(move || collect(&c, ram, disk))
        .await
        .map_err(|e| {
            MitosisError::new(ErrorCode::Internal, format!("background task failed: {e}"))
        })?
}

/// Everything `mitosis top` shows.
pub async fn usage(ctx: &Arc<Ctx>) -> Result<Usage> {
    snapshot(ctx, true).await.map(|(u, _)| u)
}

/// The error for a create that ran out of wait: names the limit and the biggest clones.
fn exhausted(u: &Usage, limit: Limit, estimate: u64) -> MitosisError {
    let largest = |size: &dyn Fn(&CloneUsage) -> u64| {
        let mut v: Vec<&CloneUsage> = u.clones.iter().collect();
        v.sort_by_key(|c| std::cmp::Reverse(size(c)));
        v.iter()
            .take(3)
            .map(|c| {
                let label = c.name.as_deref().unwrap_or(&c.id);
                format!("{label} ({})", human(size(c)))
            })
            .collect::<Vec<_>>()
            .join(", ")
    };
    match limit {
        Limit::Count => MitosisError::new(
            ErrorCode::CapReached,
            format!(
                "clone limit of {} reached: {}",
                u.max_clones,
                u.clones
                    .iter()
                    .map(|c| c.id.as_str())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
        )
        .with_hint(
            "destroy one with `mitosis destroy <id-or-name>` (or the clone_destroy tool), or raise max_clones (mitosis config set max_clones <n>)",
        ),
        Limit::Ram => MitosisError::new(
            ErrorCode::BudgetFull,
            format!(
                "ram_budget of {} is full: clones use {} and a new one needs about {}; largest: {}",
                human(u.ram_budget.unwrap_or(0)),
                human(u.ram_bytes),
                human(estimate),
                largest(&|c| c.rss_bytes.unwrap_or(0)),
            ),
        )
        .with_hint("destroy a clone, wait for one to expire, or raise it: mitosis config set ram_budget <size>"),
        Limit::Disk => MitosisError::new(
            ErrorCode::BudgetFull,
            format!(
                "disk_budget of {} is full: clones use {}; largest: {}",
                human(u.disk_budget.unwrap_or(0)),
                human(u.disk_bytes),
                largest(&|c| c.own_disk_bytes.unwrap_or(0)),
            ),
        )
        .with_hint("destroy a clone, or raise it: mitosis config set disk_budget <size>"),
    }
}

/// Counts a create as queued while it waits.
struct InQueue<'a>(&'a AtomicUsize);

impl<'a> InQueue<'a> {
    fn new(n: &'a AtomicUsize) -> Self {
        n.fetch_add(1, Ordering::SeqCst);
        InQueue(n)
    }
}

impl Drop for InQueue<'_> {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Permission to reserve one clone row. Hold it until the row exists: the next create is not
/// checked against the limits before then.
pub struct Admission {
    _turn: OwnedMutexGuard<()>,
    pub waited: Duration,
}

tokio::task_local! {
    /// Turns true when the client that sent the running request has hung up.
    pub(crate) static CLIENT_GONE: tokio::sync::watch::Receiver<bool>;
}

/// Resolves when the client of the current request hangs up; never, outside a daemon request.
async fn client_gone() {
    if let Ok(mut rx) = CLIENT_GONE.try_with(|r| r.clone()) {
        if rx.wait_for(|gone| *gone).await.is_ok() {
            return;
        }
    }
    std::future::pending::<()>().await
}

/// Waits in line (first come, first served) until a new clone fits the limits, at most `wait`.
/// Wakes on `ctx.room`, and looks again every second as a fallback. If the client hangs up while
/// it waits, it leaves the line (dropping its place); once admitted it is not cancelled.
pub async fn admit(ctx: &Arc<Ctx>, wait: Duration) -> Result<Admission> {
    tokio::select! {
        r = admit_in_line(ctx, wait) => r,
        () = client_gone() => Err(MitosisError::new(
            ErrorCode::InvalidRequest,
            "the client disconnected while the create was queued",
        )),
    }
}

async fn admit_in_line(ctx: &Arc<Ctx>, wait: Duration) -> Result<Admission> {
    let start = Instant::now();
    let _counted = InQueue::new(&ctx.queued);
    // tokio's mutex is fair: creates get their turn in the order they asked.
    let turn =
        tokio::time::timeout(wait.max(MIN_LINE_WAIT), ctx.admission.clone().lock_owned()).await;
    let Ok(turn) = turn else {
        let (u, est) = snapshot(ctx, false).await?;
        let limit = fits(&u, est);
        events::emit(
            ctx,
            events::QUEUE_TIMEOUT,
            None,
            None,
            serde_json::json!({
                "limit": limit.map(Limit::name),
                "waited_ms": u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
                "behind_other_creates": limit.is_none(),
            }),
        );
        return Err(limit.map_or_else(
            || {
                MitosisError::new(
                    ErrorCode::CapReached,
                    "other creates are still ahead of this one in the queue",
                )
                .with_hint("try again, or raise the wait: mitosis clone --wait <duration>")
            },
            |l| exhausted(&u, l, est),
        ));
    };
    let deadline = start + wait;
    let mut waited_for_room = false;
    loop {
        // Registered before the check, so a destroy right after it is not missed.
        let woken = ctx.room.notified();
        tokio::pin!(woken);
        woken.as_mut().enable();
        let (u, est) = snapshot(ctx, false).await?;
        let Some(limit) = fits(&u, est) else {
            if waited_for_room {
                events::emit(
                    ctx,
                    events::QUEUE_WAITED,
                    None,
                    None,
                    serde_json::json!({
                        "waited_ms": u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
                    }),
                );
            }
            return Ok(Admission {
                _turn: turn,
                waited: start.elapsed(),
            });
        };
        let left = deadline.saturating_duration_since(Instant::now());
        if left.is_zero() {
            events::emit(
                ctx,
                events::QUEUE_TIMEOUT,
                None,
                None,
                serde_json::json!({
                    "limit": limit.name(),
                    "waited_ms": u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX),
                }),
            );
            return Err(exhausted(&u, limit, est));
        }
        waited_for_room = true;
        let _ = tokio::time::timeout(left.min(RECHECK), woken).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn c(id: &str, rss: Option<u64>, disk: Option<u64>) -> CloneUsage {
        CloneUsage {
            id: id.into(),
            name: None,
            template: "main".into(),
            rss_bytes: rss,
            own_disk_bytes: disk,
            created_at: 0,
            expires_at: 0,
            protected: false,
            disk_warn: false,
        }
    }

    fn usage(n: usize, ram: u64, disk: u64) -> Usage {
        Usage {
            clones: (0..n).map(|i| c(&i.to_string(), None, None)).collect(),
            ram_bytes: ram,
            disk_bytes: disk,
            free_disk_bytes: 0,
            queued: 0,
            max_clones: 3,
            ram_budget: None,
            disk_budget: None,
        }
    }

    #[test]
    fn fits_cases() {
        assert_eq!(fits(&usage(2, 0, 0), 10), None);
        assert_eq!(fits(&usage(3, 0, 0), 10), Some(Limit::Count));
        // Budgets off: any RAM or disk figure passes.
        assert_eq!(fits(&usage(1, u64::MAX / 2, u64::MAX), u64::MAX / 2), None);

        let mut u = usage(1, 90, 0);
        u.ram_budget = Some(100);
        assert_eq!(fits(&u, 10), None); // 90 + 10 is exactly the budget
        assert_eq!(fits(&u, 11), Some(Limit::Ram));

        let mut u = usage(1, 0, 99);
        u.disk_budget = Some(100);
        assert_eq!(fits(&u, 10), None);
        u.disk_bytes = 100;
        assert_eq!(fits(&u, 10), Some(Limit::Disk));

        // The count is checked first.
        let mut u = usage(3, 1000, 1000);
        u.ram_budget = Some(1);
        u.disk_budget = Some(1);
        assert_eq!(fits(&u, 10), Some(Limit::Count));
    }

    #[test]
    fn ram_estimate_average_and_default() {
        assert_eq!(ram_estimate(&[]), 64 * 1024 * 1024);
        assert_eq!(ram_estimate(&[100, 200, 300]), 200);
        assert_eq!(ram_estimate(&[7]), 7);
    }

    #[test]
    fn exhausted_names_limit_and_largest_three() {
        let mut u = usage(0, 500, 0);
        u.ram_budget = Some(400);
        u.clones = vec![
            c("a", Some(10), None),
            c("b", Some(400), None),
            c("c", Some(50), None),
            c("d", Some(40), None),
        ];
        let e = exhausted(&u, Limit::Ram, 100);
        assert_eq!(e.code, ErrorCode::BudgetFull);
        assert!(e.message.contains("ram_budget"), "{}", e.message);
        let at = |id: &str| e.message.find(&format!("{id} (")).unwrap_or(usize::MAX);
        assert!(at("b") < at("c") && at("c") < at("d") && at("a") == usize::MAX);
        let e = exhausted(&u, Limit::Disk, 0);
        assert!(e.message.contains("disk_budget"), "{}", e.message);
        assert_eq!(exhausted(&u, Limit::Count, 0).code, ErrorCode::CapReached);
    }
}
