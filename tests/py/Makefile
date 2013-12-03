PROTOBUF_DIR=../../protobuf

default: test


DataFrame_pb2.py:
	protoc -I=$(PROTOBUF_DIR) --python_out=. $(PROTOBUF_DIR)/DataFrame.proto

test: DataFrame_pb2.py
	./generate_frames.py | ./unpack_frames.py

clean:
	rm dataframe.pyc DataFrame_pb2.py DataFrame_pb2.pyc

