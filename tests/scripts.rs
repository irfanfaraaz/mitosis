//! The shell scripts under `scripts/` at least parse.
use std::process::Command;

#[test]
fn bench_script_has_valid_syntax() {
    let script = concat!(env!("CARGO_MANIFEST_DIR"), "/scripts/bench.sh");
    let out = Command::new("bash").arg("-n").arg(script).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    // The cleanup must delete for real: a trash hook would keep the big template in ~/.Trash.
    let text = std::fs::read_to_string(script).unwrap();
    assert!(
        text.contains("rm -rf -- \"$BENCH_HOME\""),
        "bench.sh no longer uses rm -rf"
    );
}
