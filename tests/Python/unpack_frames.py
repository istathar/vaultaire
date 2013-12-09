#!/usr/bin/env python

import sys
import struct

import dataframe

class FrameReader(object):
    '''iterate over frames from a stream that are delimited by uint32 length headers
    '''
    def __init__(self, source=sys.stdin, max_frame_length=10**6):
        self.source = sys.stdin
        self.max_frame_length = max_frame_length

    def __iter__(self):
        while True:
            head = self.source.read(4)
            if len(head) != 4: break

            to_read, = struct.unpack('>I', head)
            if to_read > self.max_frame_length:
                raise IOError("Got bogus frame length to read:",to_read,"bytes")

            buf = self.source.read(to_read)
            if len(buf) != to_read:
                raise IOError("Truncated frame read. Expected",to_read,"bytes" \
                        " from header. Only got",len(buf),"bytes")
            yield buf

if __name__ == '__main__':
    for buf in FrameReader():
        frame = dataframe.DataFrame()
        frame.ParseFromString(buf)
        
        print frame.source, '@', frame.timestamp, '=', frame.value
        print "\n\t"+str(frame).replace('\n','\n\t')
