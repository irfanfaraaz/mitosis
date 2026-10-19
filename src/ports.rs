use crate::error::{ErrorCode, MitosisError, Result};
use std::collections::HashSet;
use std::net::TcpListener;
use std::ops::RangeInclusive;

/// Lowest port in `range` that is not leased, not skipped, and binds on 127.0.0.1 right now.
pub fn pick_port(
    range: RangeInclusive<u16>,
    leased: &HashSet<u16>,
    skip: &HashSet<u16>,
) -> Result<u16> {
    let (lo, hi) = (*range.start(), *range.end());
    range
        .filter(|p| !leased.contains(p) && !skip.contains(p))
        .find(|p| TcpListener::bind(("127.0.0.1", *p)).is_ok())
        .ok_or_else(|| {
            MitosisError::new(
                ErrorCode::PortsExhausted,
                format!("no free port in {lo}-{hi}"),
            )
            .with_hint("destroy unused clones, or widen the port range")
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn skips_busy_port_and_exhausts() {
        let busy = TcpListener::bind("127.0.0.1:0").unwrap();
        let p = busy.local_addr().unwrap().port();
        assert_ne!(
            pick_port(p..=p.saturating_add(5), &HashSet::new(), &HashSet::new()).unwrap(),
            p
        );
        assert_eq!(
            pick_port(p..=p, &HashSet::new(), &HashSet::new())
                .unwrap_err()
                .code,
            ErrorCode::PortsExhausted
        );
    }

    #[test]
    fn honours_leased_and_skip() {
        let p = TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let leased: HashSet<u16> = [p].into();
        assert_eq!(
            pick_port(p..=p, &leased, &HashSet::new()).unwrap_err().code,
            ErrorCode::PortsExhausted
        );
        assert_eq!(
            pick_port(p..=p, &HashSet::new(), &leased).unwrap_err().code,
            ErrorCode::PortsExhausted
        );
    }
}
