# Contents daemon wire format

## Requests

A request consists of one or more bytes in a ZeroMQ message. The first
byte is an opcode indicating the request type, as below:

  - `0x00` `ContentsListRequest`
  - `0x01` `GenerateNewAddress`
  - `0x02` `UpdateSourceTag`
  - `0x03` `RemoveSourceTag`

For `ContentsListRequest` and `GenerateNewAddress`, the first byte is
the entire request.

For `UpdateSourceTag` and `RemoveSourceTag`, requests consist of at
least seventeen bytes, as follows:

 - First byte is the opcode, as above;
 - Next word64 (eight bytes) is the source address;
 - Next word64 (eight bytes) is the length of the next segment, in
   bytes (little-endian);
 - The remainder of the message is a series of UTF8-coded tags, each tag
   consisting of a `key`, a colon (':'), a `value` and a comma (',').
   The `key` and `value` may contain any UTF8 character except the colon
   and the comma.
