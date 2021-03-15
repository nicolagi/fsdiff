// go:build ignore
/*
gcc -Wall sysrename.c -o /exper/bin/sysrename
*/
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/syscall.h>
#include <unistd.h>

int
main(int argc, char **argv)
{
	if(argc!=3) exit(1);
	int ret = syscall(SYS_rename, argv[1], argv[2]);
	fprintf(stderr, "ret=%d errno=%d errstr=%s\n", ret, errno, strerror(errno));
}
