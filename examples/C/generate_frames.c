#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>
#include <arpa/inet.h>

#include "DataFrame.pb-c.h"

uint64_t timestamp_now() {
	struct timespec ts;
	if (clock_gettime(CLOCK_REALTIME, &ts)) { perror("clock_gettime"); exit(2); }
	return (ts.tv_sec*1000000000) + ts.tv_nsec;
}

/** init a data frame with specific values
 * the frame copies the pointer to source, not the string itself.
 * caller is responsible for copying first if they need
 */
void data_frame_init_numeric(DataFrame *frame,
		char *source, uint64_t timestamp, uint64_t value_numeric) {
	data_frame__init(frame);
	frame->source = source;
	frame->timestamp = timestamp;
	frame->payload = DATA_FRAME__TYPE__NUMBER;
	frame->value_numeric = value_numeric;
	frame->has_value_numeric = 1;
	frame->has_value_measurement = 0;
	frame->has_value_blob = 0;
}
void data_frame_init_real(DataFrame *frame,
		char *source, uint64_t timestamp, double value_measurement) {
	data_frame__init(frame);
	frame->source = source;
	frame->timestamp = timestamp;
	frame->payload = DATA_FRAME__TYPE__REAL;
	frame->value_measurement = value_measurement;
	frame->has_value_numeric = 0;
	frame->has_value_measurement = 1;
	frame->has_value_blob = 0;
}
void data_frame_init_text(DataFrame *frame,
		char *source, uint64_t timestamp, char * value_textual) {
	data_frame__init(frame);
	frame->source = source;
	frame->timestamp = timestamp;
	frame->payload = DATA_FRAME__TYPE__TEXT;
	frame->value_textual = value_textual;
	frame->has_value_numeric = 0;
	frame->has_value_measurement = 0;
	frame->has_value_blob = 0;
}

uint8_t buf[BUFSIZ];

void write_frame(FILE *fp, DataFrame *frame) {
	size_t packed_size;

	packed_size = data_frame__get_packed_size(frame);
	if (packed_size > BUFSIZ) { perror("BUFSIZ"); exit(3); }
	data_frame__pack(frame, buf);

	fprintf(stderr, "generator: writing %lu byte frame plus 4 byte length header\n", packed_size);

	/* Protobufs doesn't delineate messages. Write out the size
	 * as a uint32 first.  Guess our tests better not send any
	 * frames bigger than 4 gig :)
	 */
	uint32_t prelude;
	prelude = htonl((uint32_t)(packed_size & 0xffffffff));
	fwrite(&prelude,sizeof(prelude), 1, fp);
	fwrite(buf, packed_size, 1, fp);
	fflush(fp);
}

int main(int argc, char **argv) {
	DataFrame *frame;
	int i;

	if ((frame = malloc(sizeof(DataFrame))) == NULL) { perror("malloc"); return 1; }
	
	for (i=0; i<55; ++i)  {
		data_frame_init_numeric(frame, "test_source", timestamp_now(), 424242 );
		write_frame(stdout, frame);
		data_frame_init_real(frame, "test_source", timestamp_now(), 3.141592);
		write_frame(stdout, frame);
		data_frame_init_text(frame, "test_source", timestamp_now(), "It's big. It's heavy. It's wood.");
		write_frame(stdout, frame);
	}


	free(frame);
	return 0;
}
