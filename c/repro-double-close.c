// go:build ignore
/*
Watch gcc -Wall repro-double-close.c -o /exper/bin/repro-double-close
repro-double-close README
*/
#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

int
main(int argc, char **argv)
{
	int fd = open(argv[1], 0);
	assert(fd>=0);
	assert(close(fd)==0);
	assert(close(fd)==-1);
	assert(errno == EBADF);
	return 0;
}
