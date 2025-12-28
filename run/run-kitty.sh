#!/usr/bin/env bash

# export SRC_DIR="${SRC_DIR:-"$HOME/p"}"
# export BUILD_DIR="${BUILD_DIR:-"$HOME/b"}"
SHVBROKER_BIN="${SHVBROKER_BIN:-"$HOME/p/shvbroker-rs/target/debug/shvbroker"}"
QXEVENTD_BIN="${QXEVENTD_BIN:-"$HOME/p/qxeventd/target/debug/qxeventd"}"
QXEVENTD_DIR="${QXEVENTD_DIR:-"$HOME/t/qxeventd"}"

#!/usr/bin/env bash

check_path_exists() {
	local var_name="$1"
    declare -n file_path="$var_name"
	if ! [[ -e "$file_path" ]]; then
        echo "Path specified in '$var_name' doesn't exist: ${file_path@Q}" >&2
		exit 1
	fi
}

for var_name in SHVBROKER_BIN QXEVENTD_BIN SHVBROKER_CONFIG; do
	check_path_exists "$var_name"
done

TUNNEL=unix:@qxeventd-remote-kitty-control-${SITE}

kitty --start-as=maximized -o allow_remote_control=yes -o enabled_layouts=grid,stack --listen-on $TUNNEL &
echo kitty is listening on: $TUNNEL
sleep 1 # wait for kitty server init

open_in_kitty() {
	kitty @ --to "$TUNNEL" launch --title "$1" --keep-focus bash
	kitty @ --to "$TUNNEL" send-text --match "title:$1" "$2"
}

open_in_kitty shvbroker "${SHVBROKER_BIN} --config $QXEVENTD_DIR/config.yaml\n"
sleep 1
open_in_kitty qxeventd "${QXEVENTD_BIN} --url 'tcp://localhost?user=test&password=test' -d $QXEVENTD_DIR -m test/qxevent -v RpcMsg\n"

kitty @ --to "$TUNNEL" close-window --match id:1
