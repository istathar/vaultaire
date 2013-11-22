#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

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

uint8_t buf[BUFSIZ];

int main(int argc, char **argv) {
	DataFrame *frame;
	size_t packed_size;

	if ((frame = malloc(sizeof(DataFrame))) == NULL) { perror("malloc"); return 1; }
	
	data_frame_init_numeric(frame, "test source for testing ftw.", timestamp_now(), 424242 );

	packed_size = data_frame__get_packed_size(frame);
	if (packed_size > BUFSIZ) { perror("BUFSIZ"); return 3; }
	data_frame__pack(frame, buf);

	fwrite(buf, packed_size, 1, stdout);

	free(frame);
	return 0;
}
