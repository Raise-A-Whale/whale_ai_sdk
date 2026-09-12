#!/usr/bin/env bash

set -uo pipefail

# Warning checks below intentionally parse Cargo's stable text prefix. Override
# inherited terminal coloring so ANSI escapes cannot hide a warning from the gate.
export CARGO_TERM_COLOR=never

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
WORK_DIR=$(mktemp -d "${TMPDIR:-/tmp}/whale-rust-packages.XXXXXX")
# Keep direct consumer paths and Cargo patch paths byte-identical on systems
# such as macOS where /var resolves through /private/var.
WORK_DIR=$(cd "$WORK_DIR" && pwd -P)
PACKAGE_TARGET="$WORK_DIR/package-target"
UNPACKED_DIR="$WORK_DIR/unpacked"
FAILURES=0
PACKAGES=(
    whale-protocol
    whale-store
    whale-adapters
    whale-core
    whale-daemon
    whale-sdk-rust
)

cleanup() {
    rm -rf "$WORK_DIR"
}
trap cleanup EXIT
trap 'exit 129' HUP
trap 'exit 130' INT
trap 'exit 143' TERM

record_check() {
    local name=$1
    shift
    local log="$WORK_DIR/${name}.log"
    if "$@" >"$log" 2>&1; then
        if grep -q '^warning:' "$log"; then
            printf 'RED  %s (Cargo warnings)\n' "$name"
            sed -n '1,160p' "$log"
            FAILURES=$((FAILURES + 1))
        else
            printf 'PASS %s\n' "$name"
        fi
    else
        local status=$?
        printf 'RED  %s (exit %s)\n' "$name" "$status"
        sed -n '1,160p' "$log"
        FAILURES=$((FAILURES + 1))
    fi
}

workspace_version() {
    python3 - "$ROOT_DIR/Cargo.toml" <<'PY'
import pathlib
import sys
import tomllib

manifest = pathlib.Path(sys.argv[1])
workspace = tomllib.loads(manifest.read_text())
print(workspace["workspace"]["package"]["version"])
PY
}

check_internal_dependency_versions() {
    python3 - "$ROOT_DIR/Cargo.toml" <<'PY'
import pathlib
import sys
import tomllib

manifest = pathlib.Path(sys.argv[1])
workspace = tomllib.loads(manifest.read_text())
dependencies = workspace.get("workspace", {}).get("dependencies", {})
missing = []
for name, value in dependencies.items():
    if not name.startswith("whale-") or not isinstance(value, dict):
        continue
    if "path" in value and "version" not in value:
        missing.append(name)
if missing:
    print("internal path dependencies without registry versions:")
    for name in sorted(missing):
        print(f"  {name}")
    raise SystemExit(1)
PY
}

check_sdk_doc_include() {
    python3 - "$ROOT_DIR/crates/whale-sdk-rust" <<'PY'
import pathlib
import re
import sys

crate = pathlib.Path(sys.argv[1]).resolve()
source = crate / "src" / "lib.rs"
text = source.read_text()
match = re.search(r'#!\[doc\s*=\s*include_str!\("([^"]+)"\)\]', text)
if match is None:
    print("SDK crate-level include_str! target was not found")
    raise SystemExit(1)
target = (source.parent / match.group(1)).resolve()
try:
    target.relative_to(crate)
except ValueError:
    print(f"SDK rustdoc include escapes package root: {match.group(1)}")
    print(f"resolved target: {target}")
    raise SystemExit(1)
if not target.is_file():
    print(f"SDK rustdoc include target does not exist: {target}")
    raise SystemExit(1)
PY
}

prepare_consumer() {
    local name=$1
    local source_mode=${2:-workspace}
    local destination="$WORK_DIR/consumers/$name"
    mkdir -p "$WORK_DIR/consumers"
    rm -rf "$destination"
    cp -R "$ROOT_DIR/fixtures/rust-consumer/$name" "$destination"
    python3 - "$destination/Cargo.toml" "$ROOT_DIR" "$UNPACKED_DIR" "$WHALE_VERSION" "$source_mode" <<'PY'
import pathlib
import re
import sys

manifest = pathlib.Path(sys.argv[1])
root = pathlib.Path(sys.argv[2]).resolve()
unpacked = pathlib.Path(sys.argv[3]).resolve()
version = sys.argv[4]
mode = sys.argv[5]
text = manifest.read_text()
# Update internal whale-* dependency versions to current target version
text = re.sub(r'(whale-[A-Za-z0-9-]+\s*=\s*\{[^}]*version\s*=\s*)"[^"]+"', rf'\g<1>"{version}"', text)
if mode == "workspace":
    replacement_root = root / "crates"
    text = text.replace('../../../crates/', replacement_root.as_posix() + '/')
elif mode == "unpacked":
    def replace(match):
        package = match.group(1)
        return (unpacked / f"{package}-{version}").as_posix()
    text = re.sub(r'\.\./\.\./\.\./crates/(whale-[A-Za-z0-9-]+)', replace, text)
else:
    raise SystemExit(f"unknown consumer source mode: {mode}")
manifest.write_text(text)
lock = manifest.parent / "Cargo.lock"
if lock.exists():
    lock.unlink()
PY
}

check_workspace_consumer() {
    local name=$1
    prepare_consumer "$name" workspace
    cargo check \
        --manifest-path "$WORK_DIR/consumers/$name/Cargo.toml" \
        --target-dir "$WORK_DIR/workspace-consumer-target"
}

package_and_unpack() {
    local package=$1
    local log="$WORK_DIR/package-command-${package}.log"
    local dependencies=()
    case "$package" in
        whale-protocol) ;;
        whale-store|whale-adapters)
            dependencies=(whale-protocol)
            ;;
        whale-core)
            dependencies=(whale-protocol whale-store whale-adapters)
            ;;
        whale-daemon)
            dependencies=(whale-protocol whale-store whale-adapters whale-core)
            ;;
        whale-sdk-rust)
            dependencies=(
                whale-protocol whale-store whale-adapters whale-core whale-daemon
            )
            ;;
        *)
            printf 'unknown Whale package: %s\n' "$package"
            return 1
            ;;
    esac
    local command=(cargo package \
        --manifest-path "$ROOT_DIR/Cargo.toml" \
        --package "$package" \
        --allow-dirty \
        --no-verify)
    local dependency
    if ((${#dependencies[@]})); then
        for dependency in "${dependencies[@]}"; do
            command+=(
                --config
                "patch.crates-io.${dependency}.path=\"${ROOT_DIR}/crates/${dependency}\""
            )
        done
    fi
    if ! CARGO_TARGET_DIR="$PACKAGE_TARGET" "${command[@]}" >"$log" 2>&1; then
        cat "$log"
        return 1
    fi
    if grep -n '^warning:' "$log"; then
        cat "$log"
        return 1
    fi

    local archive="$PACKAGE_TARGET/package/${package}-${WHALE_VERSION}.crate"
    if [[ ! -f "$archive" ]]; then
        printf 'package archive was not created: %s\n' "$archive"
        return 1
    fi
    mkdir -p "$UNPACKED_DIR"
    tar -xzf "$archive" -C "$UNPACKED_DIR"
    local unpacked="$UNPACKED_DIR/${package}-${WHALE_VERSION}"
    if [[ ! -d "$unpacked" ]]; then
        printf 'package archive has no expected root: %s\n' "$unpacked"
        return 1
    fi
}

inspect_archive() {
    local package=$1
    python3 - \
        "$PACKAGE_TARGET/package/${package}-${WHALE_VERSION}.crate" \
        "$UNPACKED_DIR/${package}-${WHALE_VERSION}" \
        "$ROOT_DIR" <<'PY'
import pathlib
import re
import sys
import tarfile

archive = pathlib.Path(sys.argv[1])
root = pathlib.Path(sys.argv[2])
workspace = pathlib.Path(sys.argv[3]).resolve()
if not archive.is_file() or not root.is_dir():
    print(f"missing archive or unpacked root: {archive}")
    raise SystemExit(1)

for required in ("Cargo.toml", "Cargo.toml.orig", "README.md", "LICENSE"):
    if not (root / required).is_file():
        print(f"archive is missing {required}")
        raise SystemExit(1)

if (root / "LICENSE").read_bytes() != (workspace / "LICENSE").read_bytes():
    print("archive LICENSE does not match the workspace root LICENSE")
    raise SystemExit(1)

for path in root.rglob("*"):
    if not path.is_file():
        continue
    relative = path.relative_to(root).as_posix()
    lowered = relative.lower()
    filename = path.name.lower()
    forbidden_names = (
        "target/", ".git/", ".codegraph/", ".superpowers/",
        "sdks/python/", "sdks/java/", "id_rsa", "credentials",
    )
    if any(name in lowered for name in forbidden_names):
        print(f"archive contains forbidden path: {relative}")
        raise SystemExit(1)
    forbidden_files = (
        ".ds_store", ".env", ".env.local", ".env.production",
    )
    forbidden_suffixes = (
        ".db", ".sqlite", ".sqlite3", ".db-wal", ".db-shm",
        ".key", ".pem", ".p12", ".pfx",
    )
    if filename in forbidden_files or filename.endswith(forbidden_suffixes):
        print(f"archive contains forbidden file: {relative}")
        raise SystemExit(1)
    try:
        contents = path.read_bytes()
    except OSError as error:
        print(f"archive member cannot be inspected: {relative}: {error}")
        raise SystemExit(1)
    absolute_paths = (workspace.as_posix().encode(),)
    if any(value in contents for value in absolute_paths):
        print(f"archive contains an absolute local workspace path: {relative}")
        raise SystemExit(1)
    local_home_patterns = (
        rb"/Users/[^/\s]+/",
        rb"/home/[^/\s]+/",
        rb"(?i:[A-Za-z]:[\\/]+Users[\\/]+[^\\/\s]+[\\/]+)",
    )
    if any(re.search(pattern, contents) for pattern in local_home_patterns):
        print(f"archive contains an absolute local user path: {relative}")
        raise SystemExit(1)
    secret_patterns = (
        rb"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----",
        rb"\bAKIA[0-9A-Z]{16}\b",
        rb"\bgh[pousr]_[A-Za-z0-9]{30,}\b",
        rb"\bsk-[A-Za-z0-9]{20,}\b",
    )
    for pattern in secret_patterns:
        if re.search(pattern, contents):
            print(f"archive contains a credential-shaped value in {relative}")
            raise SystemExit(1)

with tarfile.open(archive, "r:gz") as package:
    members = package.getmembers()
    if any(member.issym() or member.islnk() for member in members):
        print("archive contains symbolic or hard links")
        raise SystemExit(1)
PY
}

check_unpacked_package() {
    local package=$1
    local dependencies=()
    case "$package" in
        whale-protocol) ;;
        whale-store|whale-adapters)
            dependencies=(whale-protocol)
            ;;
        whale-core)
            dependencies=(whale-protocol whale-store whale-adapters)
            ;;
        whale-daemon)
            dependencies=(whale-protocol whale-store whale-adapters whale-core)
            ;;
        whale-sdk-rust)
            dependencies=(
                whale-protocol whale-store whale-adapters whale-core whale-daemon
            )
            ;;
        *)
            printf 'unknown Whale package: %s\n' "$package"
            return 1
            ;;
    esac
    local command=(cargo check \
        --manifest-path "$UNPACKED_DIR/${package}-${WHALE_VERSION}/Cargo.toml" \
        --target-dir "$WORK_DIR/unpacked-target" \
        --all-features)
    local dependency
    # macOS ships Bash 3.2, where expanding an empty array under `set -u`
    # terminates the script. whale-protocol intentionally has no dependencies.
    if ((${#dependencies[@]})); then
        for dependency in "${dependencies[@]}"; do
            command+=(
                --config
                "patch.crates-io.${dependency}.path=\"${UNPACKED_DIR}/${dependency}-${WHALE_VERSION}\""
            )
        done
    fi
    "${command[@]}"
}

check_unpacked_consumer() {
    local name=$1
    prepare_consumer "$name" unpacked
    local command=(cargo check \
        --manifest-path "$WORK_DIR/consumers/$name/Cargo.toml" \
        --target-dir "$WORK_DIR/unpacked-consumer-target")
    local dependency
    for dependency in whale-protocol whale-store whale-adapters whale-core whale-daemon; do
        command+=(
            --config
            "patch.crates-io.${dependency}.path=\"${UNPACKED_DIR}/${dependency}-${WHALE_VERSION}\""
        )
    done
    "${command[@]}"
}

WHALE_VERSION=$(workspace_version)

printf 'temporary verification root: %s\n' "$WORK_DIR"
printf 'toolchain: %s\n' "$(rustc --version)"
printf 'host: %s\n' "$(rustc -vV | sed -n 's/^host: //p')"

record_check internal_dependency_versions check_internal_dependency_versions
record_check sdk_doc_include check_sdk_doc_include
record_check application_consumer_workspace check_workspace_consumer application
record_check extension_consumer_workspace check_workspace_consumer extensions

for package in "${PACKAGES[@]}"; do
    record_check "package_${package}" package_and_unpack "$package"
    record_check "archive_${package}" inspect_archive "$package"
done

printf 'archive sizes (bytes):\n'
for package in "${PACKAGES[@]}"; do
    archive="$PACKAGE_TARGET/package/${package}-${WHALE_VERSION}.crate"
    if [[ -f "$archive" ]]; then
        printf '  %s %s\n' "$package" "$(wc -c <"$archive" | tr -d ' ')"
    else
        printf '  %s missing\n' "$package"
    fi
done

for package in "${PACKAGES[@]}"; do
    record_check "unpacked_${package}" check_unpacked_package "$package"
done

record_check application_consumer_unpacked check_unpacked_consumer application
record_check extension_consumer_unpacked check_unpacked_consumer extensions

if ((FAILURES > 0)); then
    printf 'Rust package verification failed with %s check(s) in RED.\n' "$FAILURES"
    exit 1
fi

printf 'Rust package verification passed.\n'
