#!/usr/bin/env bash
# Build sukkal's WASM module: lib/sukkal.wasm + lib/sukkal.wasm.mjs.
#
# A COMBINED build, not a chain of standalone ones. binjson-structures
# ships its own build-wasm.sh that links its nested third_party/binjson,
# and this project deliberately leaves that submodule uninitialised (see
# the README) so that one copy of binjson is linked rather than two. Its
# wasm/sources.txt and wasm/exports.txt exist precisely so a consumer can
# read the same two manifests and prefix the paths, instead of mirroring
# them here and drifting when that package gains a structure.
#
# Requires `emcc` on PATH (emsdk).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p lib

BJS=third_party/binjson-structures
BJ=third_party/binjson

for f in "$BJ/include/binjson.h" "$BJS/include/entrylog.h"; do
  [ -f "$f" ] || { echo "error: $f missing — run: git submodule update --init --recursive" >&2; exit 1; }
done

# The same flags binjson-structures' own build uses: the tree traversals
# recurse to BPT_MAX_DEPTH on a corrupt file before erroring out, which is
# what the stack size and overflow check are for.
COMMON_FLAGS=(
  -O3 -flto
  -Iinclude -I"$BJ/include" -I"$BJS/include"
  -sMODULARIZE=1 -sEXPORT_ES6=1
  -sALLOW_MEMORY_GROWTH=1
  -sSTACK_SIZE=1048576 -sSTACK_OVERFLOW_CHECK=1
  -sENVIRONMENT=web,worker,node
  -sEXPORTED_RUNTIME_METHODS=HEAPU8
  -sALLOW_TABLE_GROWTH=0
  -sFILESYSTEM=0
  --no-entry
)

# Read both manifests rather than restating them.
STRUCT_EXPORTS=$(grep -v '^#' "$BJS/wasm/exports.txt" | grep -v '^$' | paste -sd, -)
SUKKAL_EXPORTS=$(grep -v '^#' wasm/exports.txt | grep -v '^$' | paste -sd, -)

EXPORTS='_malloc,_free,'\
'_bjw_enc_reset,_bjw_put_null,_bjw_put_bool,_bjw_put_int,_bjw_put_float,'\
'_bjw_put_date,_bjw_put_pointer,_bjw_put_string,_bjw_put_binary,_bjw_put_oid,'\
'_bjw_put_key,_bjw_begin_array,_bjw_end_array,_bjw_begin_object,_bjw_end_object,'\
'_bjw_enc_finish,_bjw_enc_ptr,_bjw_enc_size,'\
'_bjw_decode,_bjw_events_ptr,_bjw_events_len,_bjw_consumed,_bjw_value_size,'\
"$STRUCT_EXPORTS,$SUKKAL_EXPORTS"

SOURCES=("$BJ/src/binjson.c" "$BJ/src/binjson_wasm.c")
while IFS= read -r line; do
  case "$line" in ''|'#'*) continue ;; esac
  SOURCES+=("$BJS/$line")
done < "$BJS/wasm/sources.txt"

# sukkal's own sources join this list as they stop needing POSIX — see
# docs/wasm-plan.md. Phase 0 links none of them: the point is to prove the
# substrate carries an entry log before anything is ported onto it.
while IFS= read -r line; do
  case "$line" in ''|'#'*) continue ;; esac
  SOURCES+=("$line")
done < wasm/sources.txt

emcc "${SOURCES[@]}" \
  "${COMMON_FLAGS[@]}" \
  -sEXPORT_NAME=createSukkalModule \
  -sEXPORTED_FUNCTIONS="$EXPORTS" \
  -o lib/sukkal.mjs

mv lib/sukkal.mjs lib/sukkal.wasm.mjs
echo "built lib/sukkal.wasm.mjs ($(wc -c < lib/sukkal.wasm) bytes wasm)"
