# Validation

CI resolves one current dependency snapshot and uses that lock in all subsequent
checks. Stable Rust runs lint, native and executed Wasm vectors. Nightly LLVM
branch instrumentation gates exact line and branch counts at 100%. There are
no production-code exclusions; test harness files are outside the coverage gate.
Coverage configuration alone is not coverage evidence.

Dependabot checks Cargo and Actions manifests. There is no npm manifest in this
repository. The metadata-only auto-merge workflow requires every substantive
check on the current head and strict administrator-enforced branch protection.
Missing protection refuses auto-merge. It never executes pull-request code with
write permissions. No registry publication is configured.

Private Tor and browser evidence is separate from public Tor qualification and
DHT restore/churn acceptance. Expensive scale and churn runs are external to
the hosted workflow.
