/*
 * filter frames matching regular expressions
 */
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <unistd.h>
#include <string.h>
#include <arpa/inet.h>

#include <pcre.h>

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

#define RE_OVECCOUNT 3 /* Multiple of 3. We don't care but libpcre does */

int main(int argc, char **argv) {
	DataFrame *frame;
	uint8_t *buf;
	size_t b;
	pcre *re;
	const char *re_errormsg;
	int re_erroffset;
	int re_ovector[RE_OVECCOUNT];


	if (argc < 2) {
		fprintf(stderr, "%s <perl compat regular expression>\n",argv[0]);
		return 1;
	}


	/* pre-compile regexp */
	re = pcre_compile(argv[1], 0, &re_errormsg, &re_erroffset, NULL);
	if (re == NULL) {
		fprintf(stderr, "regexp error at character %d: %s\n\n%s\n", re_erroffset, re_errormsg, argv[1]);
		while(re_erroffset--) 
			fputc(' ', stderr);
		fputs("^\n",stderr);
		return 1;
	}

	buf = malloc(BUFFER_SIZE);
	if (buf == NULL) { perror("malloc"); return 1; }

	while (1) {
		uint32_t wire_prelude;
		uint32_t prelude;
		int ret;

		/* network ordered uint32_t leads saying how many bytes to read
		 * for the next frame
		 */
		b = fread(&wire_prelude, sizeof(wire_prelude), 1, stdin);
		if (b < 1) break;

		prelude = ntohl(wire_prelude);
		if (prelude > BUFSIZ) {
			fprintf(stderr, "header said frame was %u bytes, but our buffer is only %u bytes. Bailing\n",prelude, BUFSIZ);
			return 1;
		}
		b = fread(buf, prelude, 1, stdin);
		if (b < 1) { perror("fread didn't return frame"); return 1;}

		frame = data_frame__unpack(NULL,prelude,buf);
		if (frame == NULL) { perror("data_frame__unpack"); return 1; }

		if (check_frame_bounds(frame)) { perror("frame string overflow"); return 1; }


		/* Now that it looks like the frame might actually be sane, check if the source matches the regexp */
		ret = pcre_exec(re, NULL, frame->source, strlen(frame->source), 0, 0, re_ovector, RE_OVECCOUNT);

		data_frame__free_unpacked(frame, NULL);

		if (ret < 0)
			continue;  /* no match */


		/* Matches supplied regexp. throw it back out on the wire */
		if (fwrite(&wire_prelude, sizeof(wire_prelude), 1, stdout) != 1) {
			perror("fwrite");
			return 1;
		}
		if (fwrite(buf, prelude, 1, stdout) != 1) {
			perror("fwrite");
			return 1;
		}
	}

	pcre_free(re);
	free(buf);

	return 0;
}
