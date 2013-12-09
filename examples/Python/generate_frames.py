#!/usr/bin/env python

import sys
import struct
import time

from DataFrame_pb2 import DataFrame

def make_length_header(frame_length):
    '''returns a network byte ordered uint32_t for the frame length
    '''
    return struct.pack('>I', frame_length)

def write_frame(df, stream=sys.stdout):
    assert df.IsInitialized()

    buf = df.SerializeToString()
    print >> sys.stderr, "Outputting", len(buf) ,"byte frame:"
    print >> sys.stderr, str(df), "\n"

    stream.write(make_length_header(len(buf)))
    stream.write(buf)

if __name__ == '__main__':
    df = DataFrame()
    df.source = "test:source:number"
    df.timestamp = int(time.time() * 1000000000)
    df.payload = DataFrame.NUMBER
    df.value_numeric = 4242
    write_frame(df)

    df = DataFrame()
    df.source = "test:source:text"
    df.timestamp = int(time.time() * 1000000000)
    df.payload = DataFrame.TEXT
    df.value_textual = 'test string'
    write_frame(df)

    df = DataFrame()
    df.source = "test:source:measurement"
    df.timestamp = int(time.time() * 1000000000)
    df.payload = DataFrame.REAL
    df.value_measurement = 3.141592
    write_frame(df)

    df = DataFrame()
    df.source = "test:source:empty"
    df.timestamp = int(time.time() * 1000000000)
    df.payload = DataFrame.EMPTY
    write_frame(df)
