CC=gcc
CFLAGS=-O2 -g -Wall
LDFLAGS=-lprotobuf-c
PROTOCC=protoc-c
PROTO_PATH=../../protobuf/
PROTO_SRC=${PROTO_PATH}DataFrame.proto

all: generate_frames unpack_frames

DataFrame.pb-c.c: ${PROTO_SRC}
	${PROTOCC} --proto_path=${PROTO_PATH} ${PROTO_SRC} --c_out .

generate_frames: DataFrame.pb-c.o generate_frames.o
	${CC}  ${CFLAGS} ${LDFLAGS} -o $@ $^

unpack_frames: DataFrame.pb-c.o

clean:
	rm -f DataFrame*.o generate_frames.o DataFrame.pb-c.[coh] tags

tags:
	ctags .

test: all
	./generate_frames | ./unpack_frames
