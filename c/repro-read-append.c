// go:build ignore
/*
Watch 'gcc -Wall repro-read-append.c -o /exper/bin/repro-read-append && cd /tmp && repro-read-append'
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
	int fd = open("testfile", O_CREAT|O_TRUNC|O_WRONLY, 0666);
	assert(fd>=0);
	assert(write(fd, "Initial contents.\n", 18) == 18);
	assert(close(fd) == 0);

	fd = open("testfile", O_APPEND|O_RDWR);
	assert(fd>=0);
	char buf[4];

	// Open does not move file offset.
	assert(read(fd, buf, 4) == 4);
	assert(strncmp(buf, "Init", 4) == 0);

	// Write moves file offset to EOF.
	assert(write(fd, "Second line.\n", 13) == 13);
	assert(read(fd, buf, 4) == 0);

	// But we're free to seek back and read.
	assert(lseek(fd, 18, 0) == 18);
	assert(read(fd, buf, 4) == 4);
	assert(strncmp(buf, "Seco", 4) == 0);

	// 0-byte write does not move offset.
	assert(syscall(SYS_write, fd, "", 0) == 0);
	assert(read(fd, buf, 4) == 4);
	assert(strncmp(buf, "nd l", 4) == 0);

	// 1-byte write does not move offset.
	assert(write(fd, "\n", 1) == 1);
	assert(read(fd, buf, 4) == 0);
	assert(close(fd) == 0);

	return 0;
}
