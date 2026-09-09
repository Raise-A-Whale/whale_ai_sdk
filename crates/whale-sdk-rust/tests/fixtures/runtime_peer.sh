#!/bin/sh
set -eu
export LC_ALL=C

fixture_dir=
fixture_mode=
fixture_response=
argument_index=0

all_arguments="$#"
for argument in "$@"; do
    argument_index=$((argument_index + 1))
    case "$argument" in
        --fixture-dir)
            expect_fixture_dir=1
            ;;
        --fixture-mode)
            expect_fixture_mode=1
            ;;
        --fixture-response)
            expect_fixture_response=1
            ;;
        *)
            if [ "${expect_fixture_dir-0}" = 1 ]; then
                fixture_dir=$argument
                expect_fixture_dir=0
            elif [ "${expect_fixture_mode-0}" = 1 ]; then
                fixture_mode=$argument
                expect_fixture_mode=0
            elif [ "${expect_fixture_response-0}" = 1 ]; then
                fixture_response=$argument
                expect_fixture_response=0
            fi
            ;;
    esac
done

[ -n "$fixture_dir" ]
[ -n "$fixture_mode" ]
mkdir -p "$fixture_dir"

event() {
    printf '%s\n' "$1" >> "$fixture_dir/events"
}

argument_index=0
for argument in "$@"; do
    argument_index=$((argument_index + 1))
    printf '%s' "$argument" | od -An -v -tx1 | tr -d ' \n' > "$fixture_dir/argv.$argument_index.hex"
done
printf '%s\n' "$all_arguments" > "$fixture_dir/argc"
printf '%s\n' "$$" > "$fixture_dir/pid"
if [ "${WHALE_RUNTIME_TEST_VALUE+x}" = x ]; then
    printf 'present\n' > "$fixture_dir/environment-present"
    printf '%s' "$WHALE_RUNTIME_TEST_VALUE" > "$fixture_dir/environment-value"
fi
printf '%s' "${PATH-}" > "$fixture_dir/inherited-path"
event spawned

trap 'event signal_term; exit 143' TERM
trap 'event signal_int; exit 130' INT
trap 'event signal_hup; exit 129' HUP

if [ "$fixture_mode" = ready_after_release ]; then
    rm -f "$fixture_dir/release"
fi

if ! IFS= read -r initialize_request; then
    event stdin_eof
    event normal_exit
    exit 0
fi
printf '%s' "$initialize_request" > "$fixture_dir/initialize-request"
event initialize_received

if [ "$fixture_mode" = eof ]; then
    event normal_exit
    exit 0
fi

if [ "$fixture_mode" = exit_before_release ]; then
    event release_ready
    event normal_exit
    exit 0
fi

if [ "$fixture_mode" = ready_after_release ]; then
    event release_ready
    release_polls=0
    while [ ! -f "$fixture_dir/release" ]; do
        release_polls=$((release_polls + 1))
        if [ "$release_polls" -ge 10000000 ]; then
            event release_timeout
            exit 70
        fi
    done
    event released
fi

if [ "$fixture_mode" != no_reply ]; then
    request_id=$(printf '%s' "$initialize_request" | sed -n 's/.*"id"[[:space:]]*:[[:space:]]*\([0-9][0-9]*\).*/\1/p')
    [ -n "$request_id" ]
    response=$(printf '%s' "$fixture_response" | sed "s/\"__WHALE_REQUEST_ID__\"/$request_id/g")
    printf '%s\n' "$response"
    event initialize_replied
fi

while IFS= read -r _request; do
    event business_input
done
event stdin_eof

if [ "$fixture_mode" = linger_after_eof ]; then
    rm -f "$fixture_dir/linger-gate"
    mkfifo "$fixture_dir/linger-gate"
    event linger_after_eof
    IFS= read -r _linger < "$fixture_dir/linger-gate"
fi

event normal_exit
