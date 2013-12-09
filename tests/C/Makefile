CC=gcc
CFLAGS=-g -Wall -O2
LDFLAGS=-lprotobuf-c -lrt
PROTOCC=protoc-c
PROTO_PATH=../../protobuf/
PROTO_SRC=${PROTO_PATH}DataFrame.proto

.PHONY: default
default: all test

DataFrame.pb-c.c: ${PROTO_SRC}
	${PROTOCC} --proto_path=${PROTO_PATH} ${PROTO_SRC} --c_out .

generate_frames: DataFrame.pb-c.o generate_frames.o
	${CC}  ${CFLAGS} ${LDFLAGS} -o $@ $^

unpack_frames: DataFrame.pb-c.o unpack_frames.o

framecat: DataFrame.pb-c.c framecat.c

framegrep: LDFLAGS:=$(LDFLAGS) -lpcre
framegrep: DataFrame.pb-c.c framegrep.c

tags:
	ctags .

.PHONY: clean
clean:
	rm -f unpack_frames.o generate_frames.o framecat.o DataFrame.pb-c.[coh] generate_frames unpack_frames framecat

.PHONY: all
all: generate_frames unpack_frames framecat framegrep

.PHONY: test
test: all
	./generate_frames 2>/dev/null | ./unpack_frames | tail -15
	./generate_frames 2>/dev/null | ./framecat | tail
