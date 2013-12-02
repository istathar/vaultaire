/*
 * framecat: output data from frames, one line per frame
 *
 * This is pretty simplistic; We don't bother checking for whitespace
 * in any of the data
 *
 * format is "source timestamp value"
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>

#include "DataFrame.pb-c.h"

#define BUFFER_SIZE	16384

/* Really blunt way to stop invalid data from protobuf-c libraries
 * letting us overrun our buffer.
 */
#define MAX_STRING_LEN	8000

/* Make sure that the strings in the frame are null
 * terminated (at least up to MAX_STRING
 */
int check_frame_bounds(DataFrame *frame){
	if (strnlen(frame->source, MAX_STRING_LEN) >= MAX_STRING_LEN)
		return 1;
	if (frame->payload == DATA_FRAME__TYPE__TEXT && 
		strnlen(frame->value_textual, MAX_STRING_LEN) >= MAX_STRING_LEN)
		return 1;
	return 0;
}

void dump_frame(DataFrame *frame) {
	printf("%s %lu ", frame->source, frame->timestamp);
	switch (frame->payload) {
		case DATA_FRAME__TYPE__NUMBER:
			printf("%lu\n", frame->value_numeric); break;
		case DATA_FRAME__TYPE__REAL:
			printf("%f\n", frame->value_measurement); break;
		case DATA_FRAME__TYPE__TEXT:
			printf("%s\n", frame->value_textual); break;
		case DATA_FRAME__TYPE__BINARY:
			printf("(BINARY DATA)\n"); break;
		case DATA_FRAME__TYPE__EMPTY:
			printf("(EMPTY)\n"); break;
		default: 
			printf("(UNKNOWN PAYLOAD TYPE)\n");
			;;
	}
}

int main(int argc, char **argv) {
	DataFrame *frame;
	uint8_t *buf;
	size_t b;

	buf = malloc(BUFFER_SIZE);
	if (buf == NULL) { perror("malloc"); return 1; }

	while (1) {
		/* network ordered uint32_t leads saying how many bytes to read
		 * for the next frame
		 */
		uint32_t prelude;
		b = fread(&prelude, sizeof(prelude), 1, stdin);
		if (b < 1) break;

		prelude = ntohl(prelude);
		if (prelude > BUFSIZ) {
			fprintf(stderr, "header said frame was %u bytes, but our buffer is only %u bytes. Bailing\n",prelude, BUFSIZ);
			return 1;
		}
		b = fread(buf, prelude, 1, stdin);
		if (b < 1) { perror("fread didn't return frame"); return 1;}

		frame = data_frame__unpack(NULL,prelude,buf);
		if (frame == NULL) { perror("data_frame__unpack"); return 1; }

		if (check_frame_bounds(frame)) { perror("frame string overflow"); return 1; }

		dump_frame(frame);
		data_frame__free_unpacked(frame, NULL);
	}

	free(buf);

	return 0;
}
