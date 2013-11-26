CC=gcc
CFLAGS=-O2 -g -Wall
LDFLAGS=-lprotobuf-c
PROTOCC=protoc-c
PROTO_PATH=../../protobuf/
PROTO_SRC=${PROTO_PATH}DataFrame.proto

.PHONY: default
default: clean all test

DataFrame.pb-c.c: ${PROTO_SRC}
	${PROTOCC} --proto_path=${PROTO_PATH} ${PROTO_SRC} --c_out .

generate_frames: DataFrame.pb-c.o generate_frames.o
	${CC}  ${CFLAGS} ${LDFLAGS} -o $@ $^

unpack_frames: DataFrame.pb-c.o unpack_frames.o

tags:
	ctags .

.PHONY: clean
clean:
	rm -f unpack_frames.o generate_frames.o DataFrame.pb-c.[coh] generate_frames unpack_frames

.PHONY: all
all: generate_frames unpack_frames

.PHONY: test
test: all
	./generate_frames | ./unpack_frames
