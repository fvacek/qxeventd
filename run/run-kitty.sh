#!/usr/bin/env bash

echo "Starting QX"

SHVBROKER_BIN="${SHVBROKER_BIN:-"$HOME/p/shvbroker-rs/target/debug/shvbroker"}"
echo "SHVBROKER_BIN" $SHVBROKER_BIN
QXEVENTD_DIR="${QXEVENTD_DIR:-"$HOME/p/qxeventd"}"
QXEVENTD_BIN="${QXEVENTD_BIN:-"$QXEVENTD_DIR/target/debug/qxeventd"}"
echo "QXEVENTD_BIN " $QXEVENTD_BIN
QX_DATA_DIR="${QX_DATA_DIR:-"$HOME/t/qx"}"
echo "QX_DATA_DIR  " $QX_DATA_DIR

check_path_exists() {
	local var_name="$1"
    declare -n file_path="$var_name"
	if ! [[ -e "$file_path" ]]; then
        echo "Path specified in '$var_name' doesn't exist: ${file_path@Q}" >&2
		exit 1
	fi
}

for var_name in SHVBROKER_BIN QXEVENTD_BIN QX_DATA_DIR; do
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

echo "Starting shvbroker"
open_in_kitty shvbroker "${SHVBROKER_BIN} --config $QXEVENTD_DIR/run/etc/shvbroker/config.yaml --data-directory $QX_DATA_DIR/shvbroker \n"
sleep 1

echo "Starting qxeventd"
open_in_kitty qxeventd "${QXEVENTD_BIN} --config $QXEVENTD_DIR/run/etc/qxeventd/config.yaml --data-directory $QX_DATA_DIR/event\n"

kitty @ --to "$TUNNEL" close-window --match id:1
