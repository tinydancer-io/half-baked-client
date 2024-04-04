#! /bin/bash

slotfull=$(RUST_LOG=info cargo r -r slot)
slot=${slotfull:5}
slot=$(echo $slot | sed -r 's/\x1B\[(;?[0-9]{1,3})+[mGK]//g')
echo $slot
RUST_LOG=info cargo r -r start --slot $slot
