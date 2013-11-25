#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>

#include "DataFrame.pb-c.h"

uint8_t buf[BUFSIZ];

/* This is not a good idea to use normally. 
 * No reason to bound it like this
 */
#define MAX_STRING	(BUFSIZ/4)

/* Make sure that the strings in the frame are null
 * terminated (at least up to MAX_STRING
 */
int check_frame_bounds(DataFrame *frame){
	if (strnlen(frame->source, MAX_STRING) >= MAX_STRING)
		return 1;
	if (frame->payload == DATA_FRAME__TYPE__TEXT && 
		strnlen(frame->value_textual, MAX_STRING) >= MAX_STRING)
		return 1;
	return 0;
}

void dump_frame(DataFrame *frame) {
	printf("frame - %lu bytes on the wire\n", data_frame__get_packed_size(frame));
	printf("\tsource:\t\t%s\n", frame->source);
	printf("\ttimestamp:\t%lu\n", frame->timestamp);

	switch (frame->payload) {
		case DATA_FRAME__TYPE__NUMBER:
			printf("\tnumber:\t\t%lu\n", frame->value_numeric); break;
		case DATA_FRAME__TYPE__REAL:
			printf("\treal:\t\t%f\n", frame->value_measurement); break;
		case DATA_FRAME__TYPE__TEXT:
			printf("\ttext:\t\t\'%s\'\n", frame->value_textual); break;
		case DATA_FRAME__TYPE__BINARY:
		case DATA_FRAME__TYPE__EMPTY:
		default: ;;
	}
}

int main(int argc, char **argv) {
	DataFrame *frame;
	size_t b;

	while (1) {
		/* network ordered uint32_t leads saying how many bytes to read
		 * for the next frame
		 */
		uint32_t prelude;
		b = fread(&prelude, sizeof(prelude), 1, stdin);
		if (b < 1) break;

		prelude = ntohl(prelude);

		if (prelude > BUFSIZ) {
			fprintf(stderr, "Prelude said frame was %u bytes, but our buffer is only %u bytes. Bailing\n",prelude, BUFSIZ);
			return 1;
		}
		printf("Prelude said frame is %u bytes on the wire\n", prelude);
		b = fread(&buf, prelude, 1, stdin);
		if (b < 1) { perror("fread didn't return frame"); return 1;}

		frame = data_frame__unpack(NULL,prelude,buf);
		if (frame == NULL) { perror("data_frame__unpack"); return 1; }

		if (check_frame_bounds(frame)) { perror("frame string overflow"); return 1; }

		dump_frame(frame);
	}

	return 0;
}
