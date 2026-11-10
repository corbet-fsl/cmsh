#!/usr/bin/env bash
# Build against the newest main of our own crates (cmtymeet policy: Cargo.lock keeps third-party
# pins; internal crates are advanced here, in CI and in the integration workspace, never by lock sweeps).
# The list of our crates is .ci/our-crates.txt, a generated copy of cmeet's ci/our-crates.txt.
# Generated copy of cmeet ci/update-our-crates.sh; change it there and run `cargo xtask ci-sync`.
# Usage: update-our-crates.sh [REPO]   REPO defaults to the repository this copy lives in. The fixture builders of
# cmeet (ci/test-env, xtask) pass the checkout they build, so they run cmeet's current script, never the possibly
# older copy inside that checkout (2026-11-07: cmsg's copy still passed bare names; `-p cmbl` failed "ambiguous").
#
# Real revs only, moved only when the used crates change (revkeep, 2026-11-09; the same rule as `xtask lock-refresh`,
# cmeet tools/xtask/src/ws/integ/refresh/revkeep.rs, and tested against it): a cache key binds the rev of a git
# dependency, so a docs-only commit of an owned repo must not move its rev. After floating, each owned source whose rev
# moved is put back to its pinned rev when no commit in between touches a file the build of the crates this lock uses
# from it can read. Irrelevant: any Cargo.lock; tools/xtask/, README.md, AGENTS.md, LICENSE.md; .ci/ .github/
# .forgejo/ .agent/ docs/ *.md LICENSE* (not .rs, Cargo.toml, build.rs); tests/ benches/ examples/ of a package; the
# sources of a package the lock does not use (unless it holds a used package below it). Such a file is relevant again
# when a source or manifest of a used crate names it (#[path], include*, build.rs strings: its file name is searched).
# Everything else is relevant: a file outside every package, a workspace root manifest, the used crates' own files.
#   update-our-crates.sh --rule GITDIR OLD NEW USED   exit 0: OLD is current for the crates USED (comma list), 1: move
set -euo pipefail

# --- the rule --------------------------------------------------------------------------------------------------------
# relevant GITDIR OLD NEW USED: prints the first relevant file of OLD..NEW (first parents, merges with what they bring),
# or `<not an ancestor>`; prints nothing when OLD stays current.
relevant() {
    local gd="$1" old="$2" new="$3" used=",$4,"
    [ "$old" = "$new" ] && return 0
    if ! git -C "$gd" merge-base --is-ancestor "$old" "$new" 2>/dev/null; then
        echo '<not an ancestor>'
        return 0
    fi
    # package directories and workspace roots at NEW: "<dir>\t<name>" ("." for the repo root), "<manifest>\tWS"
    local pk
    pk=$(git -C "$gd" ls-tree -r --name-only "$new" | grep -E '(^|/)Cargo\.toml$' | while IFS= read -r m; do
        git -C "$gd" show "$new:$m" | awk -v m="$m" '
            /^\[workspace\]/ { print m "\tWS" }
            /^\[/ { sect = $0 }
            sect == "[package]" && /^name *=/ { n = $0; sub(/^name *= *"/, "", n); sub(/".*/, "", n)
                d = m; sub(/\/?Cargo\.toml$/, "", d); if (d == "") d = "."; print d "\t" n; exit }'
    done)
    local useddirs
    useddirs=$(printf '%s\n' "$pk" | awk -F'\t' -v u="$used" '$2 != "WS" && index(u, "," $2 ",") { print $1 }')
    local f
    while IFS= read -r f; do
        [ -n "$f" ] || continue
        case "$f" in Cargo.lock | */Cargo.lock) continue ;; esac
        if printf '%s\n' "$pk" | awk -F'\t' -v f="$f" '$2 == "WS" && $1 == f { found = 1 } END { exit !found }'; then
            echo "$f"; return 0
        fi
        local base="${f##*/}" irrelevant=""
        case "$f" in
            tools/xtask/* | README.md | AGENTS.md | LICENSE.md) irrelevant=1 ;;
        esac
        if [ -z "$irrelevant" ] && ! [[ "$f" == *.rs || "$base" == Cargo.toml || "$base" == build.rs ]]; then
            case "$f" in .ci/* | .github/* | .forgejo/* | .agent/* | docs/* | *.md) irrelevant=1 ;; esac
            case "$base" in LICENSE*) irrelevant=1 ;; esac
        fi
        if [ -z "$irrelevant" ]; then
            # the owning package: the deepest package directory above f
            local own
            own=$(printf '%s\n' "$pk" | awk -F'\t' -v f="$f" '$2 != "WS" {
                d = $1; if (d == "." || index(f, d "/") == 1) { l = (d == "." ? 0 : length(d)); if (l >= best) { best = l; o = $0 } } }
                END { if (o != "") print o }')
            if [ -n "$own" ]; then
                local od="${own%%$'\t'*}" on="${own#*$'\t'}" rel
                if [ "$od" = "." ]; then rel="$f"; else rel="${f#"$od"/}"; fi
                case "$rel" in
                    tests/* | benches/* | examples/*)
                        [ "$base" = Cargo.toml ] || [ "$base" = build.rs ] || irrelevant=1 ;;
                esac
                if [ -z "$irrelevant" ] && [[ "$used" != *",$on,"* ]]; then
                    # an unused package, unless a used package lies below it
                    if ! printf '%s\n' "$useddirs" | awk -v d="$od" '$0 != "" && (d == "." || index($0, d "/") == 1) { f = 1 } END { exit !f }'; then
                        irrelevant=1
                    fi
                fi
            fi
        fi
        if [ -n "$irrelevant" ] && [ -n "$useddirs" ]; then
            # read by a used crate after all? (its file name in a source or manifest of a used crate)
            local specs=() d
            while IFS= read -r d; do
                [ -n "$d" ] || continue
                # the library, binaries, build script and manifest; never the crate's own tests, benches or examples
                if [ "$d" = "." ]; then
                    specs+=('*.rs' '*Cargo.toml' ':(exclude)tests/' ':(exclude)benches/' ':(exclude)examples/')
                else
                    specs+=("$d/*.rs" "$d/*Cargo.toml" ":(exclude)$d/tests/" ":(exclude)$d/benches/" ":(exclude)$d/examples/")
                fi
            done <<<"$useddirs"
            if git -C "$gd" grep -q -F -e "$base" "$new" -- "${specs[@]}" 2>/dev/null; then
                irrelevant=""
            fi
        fi
        if [ -z "$irrelevant" ]; then
            echo "$f"; return 0
        fi
    done < <(git -C "$gd" log --first-parent -m --no-renames --name-only --format= "$old..$new")
    return 0
}

if [ "${1:-}" = "--rule" ]; then
    [ $# -eq 5 ] || { echo 'usage: update-our-crates.sh --rule GITDIR OLD NEW USED' >&2; exit 2; }
    hit=$(relevant "$2" "$3" "$4" "$5")
    [ -z "$hit" ] && exit 0
    echo "$hit"
    exit 1
fi

cd "${1:-$(dirname "${BASH_SOURCE[0]}")/..}"
[ -f Cargo.lock ] || exit 0
[ -f .ci/our-crates.txt ] || { echo 'update-our-crates: .ci/our-crates.txt missing' >&2; exit 1; }
# Our packages in the lock: "<source without #rev>\t<rev>\t<name>\t<version>". Only packages the lock takes from our forge
# and that the shared list names. Each is passed as the full package spec `<source>#<name>@<version>`, never as a bare
# name: a repo's lock can hold the same name twice, e.g. cwlt's own path package `cwlt-engine` and the git copy of it
# that ckmg depends on, and `cargo update -p cwlt-engine` then fails "ambiguous".
ours() {
    awk '
        FNR == NR { if ($0 !~ /^#/ && $0 != "") ours[$0] = 1; next }
        /^\[\[package\]\]/ { cur = ""; ver = "" }
        /^name = / { gsub(/"/, "", $3); cur = $3 }
        /^version = / { gsub(/"/, "", $3); ver = $3 }
        /^source = "git\+https:\/\/git\.corbet\.ch\// {
            if (cur in ours) { src = $3; gsub(/"/, "", src); rev = src; sub(/#.*$/, "", src); sub(/^.*#/, "", rev)
                print src "\t" rev "\t" cur "\t" ver }
        }
    ' .ci/our-crates.txt Cargo.lock | sort -u
}
before=$(ours)
args=()
while IFS=$'\t' read -r src _ name ver; do
    [ -n "$src" ] && args+=(-p "$src#$name@$ver")
done <<<"$before"
if [ "${#args[@]}" -eq 0 ]; then
    echo 'update-our-crates: no internal crates in Cargo.lock'
    exit 0
fi
# 1. float everything: it fetches every owned source and moves it to its newest rev
echo "update-our-crates: advancing $(( ${#args[@]} / 2 )) internal packages to the newest main"
cargo update "${args[@]}"
after=$(ours)
# 2. per source whose rev moved: keep the pinned rev when nothing the used crates build changed in between
db_root="${CARGO_HOME:-$HOME/.cargo}/git/db"
keep=() kept=0 moved=0
while IFS=$'\t' read -r src old; do
    [ -n "$src" ] || continue
    new=$(printf '%s\n' "$after" | awk -F'\t' -v s="$src" '$1 == s { print $2; exit }')
    [ -n "$new" ] && [ "$new" != "$old" ] || continue
    used=$(printf '%s\n' "$before" | awk -F'\t' -v s="$src" '$1 == s { printf "%s%s", sep, $3; sep = "," }')
    url="${src#git+}"; url="${url%%\?*}"; repo="${url##*/}"; repo="${repo%.git}"
    gd=""
    for d in "$db_root/$repo"-*; do
        if [ -d "$d" ] && git -C "$d" cat-file -e "$old^{commit}" 2>/dev/null && git -C "$d" cat-file -e "$new^{commit}" 2>/dev/null; then
            gd="$d"; break
        fi
    done
    if [ -z "$gd" ]; then
        hit="no local history to judge"
    else
        hit=$(relevant "$gd" "$old" "$new" "$used")
    fi
    if [ -n "$hit" ]; then
        echo "update-our-crates: $repo: ${old:0:7} -> ${new:0:7} ($hit)"
        moved=$((moved + 1))
    else
        keep+=("$src"$'\t'"$old")
    fi
done < <(printf '%s\n' "$before" | awk -F'\t' '!seen[$1]++ { print $1 "\t" $2 }')
# 3. back to the pinned rev. Updating a git package unlocks its git dependencies (they follow their branch), so a revert
#    can float a source reverted before it: repeat until every kept source holds its pin (dependents settle first).
for _pass in 1 2 3 4 5 6 7 8; do
    now=$(ours) again=0
    for k in "${keep[@]:-}"; do
        [ -n "$k" ] || continue
        src="${k%%$'\t'*}" old="${k#*$'\t'}"
        line=$(printf '%s\n' "$now" | awk -F'\t' -v s="$src" '$1 == s { print; exit }')
        IFS=$'\t' read -r _ cur name ver <<<"$line"
        [ "$cur" = "$old" ] && continue
        cargo update -q --offline -p "$src#$name@$ver" --precise "$old" 2>/dev/null || cargo update -q -p "$src#$name@$ver" --precise "$old"
        now=$(ours) again=1
    done
    [ "$again" = 1 ] || break
done
now=$(ours)
for k in "${keep[@]:-}"; do
    [ -n "$k" ] || continue
    src="${k%%$'\t'*}" old="${k#*$'\t'}"; url="${src#git+}"; url="${url%%\?*}"; repo="${url##*/}"; repo="${repo%.git}"
    cur=$(printf '%s\n' "$now" | awk -F'\t' -v s="$src" '$1 == s { print $2; exit }')
    if [ "$cur" = "$old" ]; then
        echo "update-our-crates: $repo: kept ${old:0:7} (nothing its used crates build read changed)"
        kept=$((kept + 1))
    else
        echo "update-our-crates: $repo: at ${cur:0:7}, could not stay at ${old:0:7}"
        moved=$((moved + 1))
    fi
done
echo "update-our-crates: $moved source(s) moved to the newest main, $kept kept at their pinned rev"
