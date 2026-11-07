# Storage Format

How trades are stored on disk. The code is
`src/main/java/com/tsdb/storage/RecordCodec.java`.

## Record (32 bytes)

Every trade is stored as exactly 32 bytes:

| Offset | Size | Field | Encoding |
|---|---|---|---|
| 0 | 8 | timestamp | signed 64-bit integer, epoch milliseconds |
| 8 | 8 | price | IEEE-754 double, raw bits (`Double.doubleToRawLongBits`) |
| 16 | 4 | quantity | signed 32-bit integer |
| 20 | 1 | side | `0` = BUY, `1` = SELL |
| 21 | 7 | symbol | ASCII, 1–7 printable characters (`!` to `~`, no spaces), padded with zero bytes |
| 28 | 4 | checksum | CRC32C of bytes 0–27 |

All multi-byte numbers are **little-endian** (lowest byte first).

Example: a BUY of 250 AAPL at $187.42.

```
00  35 5a e5 ff 92 01 00 00   timestamp 1730870073909
08  3d 0a d7 a3 70 6d 67 40   price 187.42
10  fa 00 00 00               quantity 250
14  00                        side BUY
15  41 41 50 4c 00 00 00      symbol "AAPL"
1c  26 f9 ed a5               checksum
```

(Offsets on the left are in hex; these bytes are the actual output of
`RecordCodec.encode`.)

### Why it's designed this way

- **Fixed width.** Record *i* starts at byte `32 × i` (plus the file
  header), so a reader can jump straight to any record and binary-search by
  timestamp without a separate index.
- **32 bytes.** It's a power of two, so a record never spans two 4 KB
  disk pages.
- **Raw double bits.** Prices come back bit-for-bit identical, with no
  rounding from a text conversion.
- **Checksum.** It detects records that were torn or damaged by a crash.
  CRC32C catches every error confined to 32 consecutive bits, so any change to
  a single byte is always detected.
- **No ID stored.** A record's ID is its position: the file's first ID plus
  the record's index. Storing it would be redundant.

### What counts as a valid record

A record is valid when the checksum matches, the side byte is 0 or 1, and the
symbol field holds 1–7 printable characters followed only by zero bytes. An
all-zero record is never valid. `RecordCodec.isValid` checks all of these;
crash recovery uses it to find where the good data in a file ends.

The codec stores any timestamp, price and quantity exactly as given. It does
not judge whether those values make sense.

## Segment file

*To be defined together with `Segment`: header layout, file naming
(`seg-<first ID>.tsd`), and the rule that timestamps never decrease.*
