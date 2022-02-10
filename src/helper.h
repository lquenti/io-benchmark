//
// Created by lquenti on 27.11.21.
//

#ifndef IO_BENCHMARK_HELPER_H
#define IO_BENCHMARK_HELPER_H

#define MEMINFO "/proc/meminfo"

/* See: https://unix.stackexchange.com/q/17936 */
#define DROP_PAGE_CACHE "/proc/sys/vm/drop_caches"

/* See: https://stackoverflow.com/a/70370002/9958281 */
const unsigned long MAX_IO_SIZE = 0x7ffff000;

#include <stdbool.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <ctype.h>

/** Helper function to "handle" malloc failure.
 *
 * In this case, handling means logging failure and killing the whole program.
 * This is fine because we have a job-based architecture and this job obviously failed.
 */
void *malloc_or_die(size_t size)
{
  void *res;
  if (size == 0)
  {
    fprintf(stderr, "ERROR: malloc() called with length zero. Exiting...\n");
    exit(1);
  }
  res = malloc(size);
  if (res == 0)
  {
    fprintf(stderr, "ERROR: malloc() failed.\n");
    exit(1);
  }
  return res;
}

/** Helper function to "handle" open failure.
 *
 * In this case, handling means logging failure and killing the whole program.
 * This is fine because we have a job-based architecture and this job obviously failed.
 */
int open_or_die(const char *pathname, int flags, mode_t mode)
{
  int res = open(pathname, flags, mode);
  if (res == -1)
  {
    fprintf(stderr, "ERROR: opening %s failed with '%s'\n", pathname, strerror(errno));
    exit(1);
  }
  return res;
}

/** Helper function to "handle" read failure.
 *
 * In this case, handling means logging failure and killing the whole program.
 * This is fine because we have a job-based architecture and this job obviously failed.
 *
 * Also: the maximum size which is checked aginst is defined in Linux man 2 read
 */
ssize_t read_or_die(int fd, void *buf, size_t count)
{
  if (count > MAX_IO_SIZE)
  {
    fprintf(stderr, "ERROR: Linux just supports reading up to 0x%lx bytes per read\n", MAX_IO_SIZE);
    exit(1);
  }
  ssize_t res = read(fd, buf, count);
  if (res == -1)
  {
    fprintf(stderr, "ERROR: failed to read with '%s'\n", strerror(errno));
    exit(1);
  }
  if (((size_t)res) != count)
  {
    fprintf(stderr, "ERROR: Wrong number of bytes read. Expected: %zu Actual: %zu\n", count, res);
    exit(1);
  }
  return res;
}

/** Helper function to "handle" write failure.
 *
 * In this case, handling means logging failure and killing the whole program.
 * This is fine because we have a job-based architecture and this job obviously failed.
 *
 * Also: the maximum size which is checked aginst is defined in Linux man 2 write
 */
ssize_t write_or_die(int fd, void *buf, size_t count)
{
  if (count > MAX_IO_SIZE)
  {
    fprintf(stderr, "ERROR: Linux just supports writing up to 0x%lx bytes per read\n", MAX_IO_SIZE);
    exit(1);
  }
  ssize_t res = write(fd, buf, count);
  if (res == -1)
  {
    fprintf(stderr, "ERROR: failed to write with '%s'\n", strerror(errno));
    exit(1);
  }
  if (((size_t)res) != count)
  {
    fprintf(stderr, "ERROR: Wrong number of bytes writen. Expected: %zu Actual: %zu\n", count, res);
    exit(1);
  }
  return res;
}

/** Helper function to "handle" close failure.
 *
 * In this case, handling means logging failure and killing the whole program.
 * This is fine because we have a job-based architecture and this job obviously failed.
 */
int close_or_die(int fd)
{
  int res = close(fd);
  if (res == -1)
  {
    fprintf(stderr, "ERROR: falied to close file with '%s'\n", strerror(errno));
    exit(1);
  }
  return res;
}

/** Helper function to "handle" lseek failure.
 *
 * In this case, handling means logging failure and killing the whole program.
 * This is fine because we have a job-based architecture and this job obviously failed.
 */
off_t lseek_or_die(int fd, off_t offset, int whence)
{
  off_t res = lseek(fd, offset, whence);
  if (res == -1)
  {
    fprintf(stderr, "ERROR: lseek failed with '%s'\n", strerror(errno));
    exit(1);
  }
  return res;
}

/** Helper function to "handle" fsync failure.
 *
 * In this case, handling means logging failure and killing the whole program.
 * This is fine because we have a job-based architecture and this job obviously failed.
 */
int fsync_or_die(int fd)
{
  int res = fsync(fd);
  if (res == -1)
  {
    fprintf(stderr, "ERROR: fsync failed with '%s'\n", strerror(errno));
    exit(1);
  }
  return res;
}

/** Helper function to "handle" fstat failure.
 *
 * In this case, handling means logging failure and killing the whole program.
 * This is fine because we have a job-based architecture and this job obviously failed.
 */
int fstat_or_die(int fd, struct stat *st)
{
  int res = fstat(fd, st);
  if (res == -1)
  {
    fprintf(stderr, "ERROR: fstat failed with '%s'\n", strerror(errno));
    exit(1);
  }
  return res;
}

/** Checks for io-failure outside of benchmark.
 *
 * This is done in order to minimize branching in the actual io-read, thus
 * giving the best possible benchmark data.
 */
void io_op_worked_or_die(int res, bool is_read_operation)
{
  if (res == -1)
  {
    fprintf(stderr, "ERROR: %s failed with '%s'\n", (is_read_operation) ? "read" : "write", strerror(errno));
    exit(1);
  }
}

void strn_to_lower(char *str, size_t n)
{
  /* If n unspecified (i.e. 0) we use the full string. */
  if (n == 0)
    n = strlen(str);
  for (size_t i = 0; i < n; ++i)
    str[i] = tolower(str[i]);
}

long parse_from_meminfo(char *key)
{
  long res = -1;
  size_t keylen = strlen(key);
  strn_to_lower(key, keylen);

  /* Find the correct line */
  char buf[100];
  FILE *fp = fopen(MEMINFO, "r");
  while (fgets(buf, sizeof(buf), fp))
  {
    strn_to_lower(buf, 0);
    if (strncmp(buf, key, keylen))
      continue;

    char *colon = strchr(buf, ':');
    res = atol(colon + 1);
    break;
  }
  fclose(fp);

  return res;
}

size_t get_available_mem()
{
  /* Needed to allow case insensitive comparison */
  char free[] = "MemFree", cached[] = "Cached", buffers[] = "Buffers";
  return parse_from_meminfo(free) + parse_from_meminfo(cached) +
         parse_from_meminfo(buffers);
}

void allocate_memory_until(size_t space_left_in_kib)
{
  size_t current_available = get_available_mem();
  while (current_available > space_left_in_kib)
  {
    size_t delta = current_available - space_left_in_kib;
    size_t n = (delta < 500 ? delta : 500) * 1024;

    char *p = malloc(n);
    if (!p)
    {
      fprintf(stderr, "dummy malloc failed. available: %zu. Tried to alloc %zu. Quitting...",
              current_available, n / 1024);
      exit(1);
    }
    memset(p, '1', n);
    current_available = get_available_mem();
  }
}

/* See: https://unix.stackexchange.com/q/17936 */
void drop_page_cache() {
  /* sync first */
  sync();
  /* Write magic value */
  int fd = open(DROP_PAGE_CACHE, O_WRONLY);
  /* Check whether we had the permissions */
  if (fd == -1) {
    if (errno == EACCES) {
      fprintf(stderr, "In order to clear the cache, we need permissions to open" DROP_PAGE_CACHE "\n");
      exit(1);
    } else {
      fprintf(stderr, "Unknown Error while opening" DROP_PAGE_CACHE ".\nError: %s\n", strerror(errno));
      exit(1);
    }
  }
  char magic_value = '3';
  write_or_die(fd, &magic_value, sizeof(char));
  /* In case the OS does it non-blockingly */
  sleep(5);
  close(fd);
}

#endif //IO_BENCHMARK_HELPER_H
