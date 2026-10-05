# Public Tor browser qualification (research branch)

Runs the unchanged `corbet-foss/ctrn` public browser contract
(`experiment/public-browser` at `53d481d`) in GitHub Actions of this
organisation. `ctrn` and its dependency `corbet-foss/cfry` (LGPL-3.0-only WITH
LGPL-3.0-linking-exception, licence texts inside each bundle) are included as
git bundles; git redirects the `cfry` URL to the bundle, so every script and the
committed `Cargo.lock` stay unchanged. No product code; evidence only.
