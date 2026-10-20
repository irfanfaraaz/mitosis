use crate::error::{ErrorCode, MitosisError, Result};
use std::io::Read;
use std::time::Duration;

/// Template and clone names: `^[a-z0-9][a-z0-9_-]{0,62}$`. A new clone name also passes
/// `check_new_clone_name`.
pub fn validate_name(kind: &str, name: &str) -> Result<()> {
    let mut chars = name.chars();
    let ok = name.len() <= 63
        && matches!(chars.next(), Some(c) if c.is_ascii_lowercase() || c.is_ascii_digit())
        && chars.all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if ok {
        return Ok(());
    }
    Err(MitosisError::new(
        ErrorCode::InvalidRequest,
        format!("invalid {kind} name {name:?}"),
    )
    .with_hint("use 1-63 characters: a-z, 0-9, '_' or '-', starting with a letter or digit"))
}

/// A name no clone has yet may not look like a clone id (`c` plus 6 hex digits): the router picks
/// a name before an id, so it would shadow the clone with that id. Clones that 0.2 gave such a
/// name keep it (the create paths skip this check for an existing name).
pub fn check_new_clone_name(name: &str) -> Result<()> {
    if !is_clone_id(name) {
        return Ok(());
    }
    Err(MitosisError::new(
        ErrorCode::InvalidRequest,
        format!("invalid clone name {name:?}: it looks like a clone id"),
    )
    .with_hint("clone ids are 'c' plus 6 hex digits; pick a name that is not, like my-clone"))
}

/// Positive integer followed by one unit: `s`, `m`, `h` or `d`.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let bad = || {
        MitosisError::new(ErrorCode::InvalidRequest, format!("invalid duration {s:?}"))
            .with_hint("use a positive whole number plus a unit, like 90s, 30m, 2h or 1d")
    };
    let unit = s.chars().last().ok_or_else(bad)?;
    let mult: u64 = match unit {
        's' => 1,
        'm' => 60,
        'h' => 3600,
        'd' => 86_400,
        _ => return Err(bad()),
    };
    let digits = &s[..s.len() - 1];
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(bad());
    }
    let n: u64 = digits.parse().map_err(|_| bad())?;
    if n == 0 {
        return Err(bad());
    }
    n.checked_mul(mult).map(Duration::from_secs).ok_or_else(bad)
}

/// A queue wait: a duration, or `0` / `0s` for no wait at all.
pub fn parse_wait(s: &str) -> Result<Duration> {
    if s == "0" || s == "0s" {
        Ok(Duration::ZERO)
    } else {
        parse_duration(s)
    }
}

/// Compound, largest unit first, zero parts skipped: 7200s -> "2h", 90s -> "1m30s", 0 -> "0s".
pub fn format_duration(d: Duration) -> String {
    let mut secs = d.as_secs();
    if secs == 0 {
        return "0s".into();
    }
    let mut out = String::new();
    for (unit, size) in [('d', 86_400), ('h', 3600), ('m', 60), ('s', 1)] {
        if secs >= size {
            out.push_str(&format!("{}{unit}", secs / size));
            secs %= size;
        }
    }
    out
}

/// `bytes` random bytes from `/dev/urandom`, as lowercase hex.
pub fn random_hex(bytes: usize) -> String {
    let mut buf = vec![0u8; bytes];
    std::fs::File::open("/dev/urandom")
        .and_then(|mut f| f.read_exact(&mut buf))
        .expect("read /dev/urandom");
    buf.iter().map(|b| format!("{b:02x}")).collect()
}

/// A fresh clone id: `c` plus 6 lowercase hex digits.
pub(crate) fn new_clone_id() -> String {
    format!("c{}", random_hex(3))
}

/// True for names `new_clone_id` can produce; gc touches nothing else in `clones/` and `sockets/`.
pub(crate) fn is_clone_id(s: &str) -> bool {
    s.len() == 7
        && s.starts_with('c')
        && s[1..]
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names() {
        assert!(validate_name("template", "main").is_ok());
        assert!(validate_name("clone", "feature-x_2").is_ok());
        assert!(validate_name("clone", "cafe123").is_ok());
        assert!(check_new_clone_name("cafe123").is_err());
        assert!(check_new_clone_name("c0ffee1").is_err());
        assert!(validate_name("template", "cafe123").is_ok());
        assert!(check_new_clone_name("cafe1234").is_ok());
        assert!(check_new_clone_name("cafe12g").is_ok());
        assert!(validate_name("clone", &"a".repeat(63)).is_ok());
        for bad in ["", "../x", "a/b", "Main", "-x", &"a".repeat(64)] {
            assert_eq!(
                validate_name("template", bad).unwrap_err().code,
                ErrorCode::InvalidRequest
            );
        }
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("2h").unwrap(), Duration::from_secs(7200));
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("1d").unwrap(), Duration::from_secs(86_400));
        for bad in ["", "0m", "abc", "5", "-1h", "+1h", "1.5h"] {
            assert!(parse_duration(bad).is_err(), "{bad:?}");
        }
        assert_eq!(format_duration(Duration::from_secs(7200)), "2h");
        assert_eq!(format_duration(Duration::from_secs(90)), "1m30s");
        assert_eq!(format_duration(Duration::from_secs(0)), "0s");
    }

    #[test]
    fn hex() {
        assert_eq!(random_hex(12).len(), 24);
    }

    #[test]
    fn clone_ids() {
        for _ in 0..20 {
            assert!(is_clone_id(&new_clone_id()));
        }
        for bad in [
            "c12345", "c1234567", "cABCDEF", "x123456", "c12345g", "zzz", "build-1",
        ] {
            assert!(!is_clone_id(bad), "{bad}");
        }
    }
}
