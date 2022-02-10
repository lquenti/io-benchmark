//
// Created by lquenti on 22.11.21.
//

#ifndef IO_BENCHMARK_IO_BENCHMARK_H
#define IO_BENCHMARK_IO_BENCHMARK_H

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <time.h>

#include "helper.h"

// Notes:
// Which clocks to use:
//   - https://stackoverflow.com/a/12480485/9958281

/** All possible access patterns. */
typedef enum access_pattern_t
{
  ACCESS_PATTERN_CONST = 0,      /**< always read at the same place */
  ACCESS_PATTERN_SEQUENTIAL = 1, /**< read sequentially through the file (like an intuitive read) */
  ACCESS_PATTERN_RANDOM = 2,     /**< go to a random position after every read */
} access_pattern_t;

/** All possible configuration options for a benchmark run
 * This will get parsed from the command line options.
 */
typedef struct benchmark_config_t
{
  /**< The path to the file from which will be read from or written to */
  const char *filepath;

  /**< The size of the memory buffer to/from which the information will be written/read to*/
  const size_t memory_buffer_in_bytes;

  /**< The size of the file specified by filepath. Ignored is prepare_file_size is set to false. */
  const size_t file_size_in_bytes;

  /**< The size of each I/O request. */
  const size_t access_size_in_bytes;

  /**< The number of tests done for this execution. */
  const size_t number_of_io_op_tests;

  /**< Which access pattern should be used aka how the memory pointer should be moved.*/
  const access_pattern_t access_pattern_in_memory;

  /**< Which access pattern should be used aka how the file pointer should be seeked.*/
  const access_pattern_t access_pattern_in_file;

  /**< Whether the benchmaked I/O-operation is read or write. */
  const bool is_read_operation;

  /** Whether the file should be bloated up to file_size_in_bytes.
   *
   * In most cases, this should be true.
   * The only expections are special "files" that can't be made bigger like
   * special devices.
   */
  const bool prepare_file_size;

  const bool use_o_direct;

  const bool drop_cache_first;

  const bool do_reread;

  const size_t restrict_free_ram_to;
} benchmark_config_t;

typedef ssize_t (*io_op_t)(int fd, void *buf, size_t count);

/** The current state of the program, wrapped into a struct to declutter global state.
 *
 * In order to not pollute global state we keep our state local to the functions.
 * We wrap it all into a struct to not have functions with 10+ parameters.
 *
 * The state is basically a singleton for each thread, created by the benchmark itself.
 */
typedef struct benchmark_state_t
{
  /**< The memory buffer on which the io-ops read/write from/to */
  void *buffer;

  /**< The file descriptor of the benchmarked file specified by the config struct. */
  int fd;

  /**< The memory offset after the last io-operation, needed to specify the next one according to access pattern. */
  size_t last_mem_offset;

  /**< The file offset after the last io-operation, needed to specify the next one according to access pattern. */
  size_t last_file_offset;
  io_op_t io_op;
} benchmark_state_t;

/** The results returned after the benchmark.
 *
 * Each test, defined by its starting and end time, is for a single io-operation (i.e. a single read or write).
 * No statistical accumulation or means.
 */
typedef struct benchmark_results_t
{
  /**< The number of io-ops measured in this benchmark */
  size_t length;

  /**< An array of durations. The i-th double corresponds to the starting time of the i-th io-operation */
  double *durations;

} benchmark_results_t;

typedef struct timespec timespec_t;

/** Initializes the file used for reading/writing.
 *
 * Since write needs a buffer argument, we use our benchmark buffer to fill it, thus
 * we possibly need multiple iterations to fill it.
 * The size is defined by config->file_size_in_bytes.
 */
static void init_file(const benchmark_config_t *config, benchmark_state_t *state)
{
  /* is it externally managed? */
  if (!config->prepare_file_size)
    return;

  state->fd = open_or_die(config->filepath, O_CREAT | O_RDWR, 0644);

  /* Does it already have the correct size? */
  struct stat st;
  fstat_or_die(state->fd, &st);
  close_or_die(state->fd);
  if ((size_t)st.st_size == config->file_size_in_bytes)
    return;

  /* If not, we just truncate it to zero and fill it up */
  state->fd = open_or_die(config->filepath, O_RDWR | O_TRUNC, 0644);
  size_t count = (MAX_IO_SIZE <= config->memory_buffer_in_bytes) ? MAX_IO_SIZE : config->memory_buffer_in_bytes;
  size_t iterations = config->file_size_in_bytes / count;
  for (; iterations; --iterations)
  {
    write_or_die(state->fd, state->buffer, count);
  }
  /* Now allocate the rest which is less than 1 buffer size. */
  size_t rest = config->file_size_in_bytes % count;
  write_or_die(state->fd, state->buffer, rest);

  /* Did it work? */
  fsync_or_die(state->fd);
  fstat_or_die(state->fd, &st);
  if ((size_t)st.st_size != config->file_size_in_bytes)
  {
    fprintf(stderr, "ERROR: File size does not match. Expected: %zu Actual: %zu\n", config->file_size_in_bytes,
            (size_t)st.st_size);
    exit(1);
  }
}

/** Initializes the benchmark_state_t struct with proper values from the config. */
static void init_state(const benchmark_config_t *config, benchmark_state_t *state)
{
  state->buffer = malloc_or_die(config->memory_buffer_in_bytes);
  memset(state->buffer, '1', config->memory_buffer_in_bytes);
  state->last_mem_offset = 0;
  state->last_file_offset = 0;
  state->io_op = config->is_read_operation ? read : (io_op_t)write;
}

/** Sane initialization of the benchmark_results_t struct based on config values. */
static void init_results(const benchmark_config_t *config, benchmark_results_t *results)
{
  results->length = config->number_of_io_op_tests;
  results->durations = malloc_or_die(sizeof(double) * config->number_of_io_op_tests);
}

/** Initialzes the memory pointer for the working memory buffer.
 *
 * Depending on the access patterns.
 */
static inline void init_memory_position(const benchmark_config_t *config, size_t *res)
{
  switch (config->access_pattern_in_memory)
  {
  case ACCESS_PATTERN_CONST:
  case ACCESS_PATTERN_SEQUENTIAL:
  {
    *res = 0;
    return;
  }
  case ACCESS_PATTERN_RANDOM:
  {
    /* TODO: Possible Alignment and Rewrite */
    *res = ((size_t)rand() * 128) % (config->memory_buffer_in_bytes - config->access_size_in_bytes);
    return;
  }
  }
}

/** Initializes the file pointer location.
 *
 * Depending on the access patterns.
 * In our program state we also track the current state
 */
static inline void init_file_position(const benchmark_config_t *config, benchmark_state_t *state)
{
  switch (config->access_pattern_in_file)
  {
  case ACCESS_PATTERN_CONST:
  case ACCESS_PATTERN_SEQUENTIAL:
  {
    // We start at the beginning
    state->last_file_offset = 0;
    return;
  }
  case ACCESS_PATTERN_RANDOM:
  {
    // TODO: Possible alignment and rewrite
    size_t random_offset = ((off_t)rand() * 128) % (config->file_size_in_bytes - config->access_size_in_bytes);
    lseek_or_die(state->fd, random_offset, SEEK_CUR);
    state->last_file_offset = random_offset;
    return;
  }
  }
}

/** Reset all state values before the run. */
static void prepare_run(const benchmark_config_t *config, benchmark_state_t *state)
{
  if (config->use_o_direct)
    state->fd = open_or_die(config->filepath, O_RDWR | O_DIRECT, 0644);
  else
    state->fd = open_or_die(config->filepath, O_RDWR, 0644);
  if (config->restrict_free_ram_to != 0)
    allocate_memory_until(config->restrict_free_ram_to/1024);
  init_memory_position(config, &state->last_mem_offset);
  init_file_position(config, state);
}

/** Choose the next memory position after each io-op according to the access pattern. */
static inline void pick_next_mem_position(const benchmark_config_t *config, benchmark_state_t *state)
{
  switch (config->access_pattern_in_memory)
  {
  case ACCESS_PATTERN_CONST:
    /* After one io-op the pointer does not get moved like the fd-state for the file */
    return;
  case ACCESS_PATTERN_SEQUENTIAL:
  {
    state->last_mem_offset += config->access_size_in_bytes;
    return;
  }
  case ACCESS_PATTERN_RANDOM:
    state->last_mem_offset = ((size_t)rand() * 128) % (config->memory_buffer_in_bytes - config->access_size_in_bytes);
    return;
  }
}

/** Choose the next file position after each io-op according to the access pattern */
static inline void pick_next_file_position(const benchmark_config_t *config, benchmark_state_t *state)
{
  switch (config->access_pattern_in_file)
  {
  case ACCESS_PATTERN_CONST:
    lseek_or_die(state->fd, 0, SEEK_SET);
    return;
  case ACCESS_PATTERN_SEQUENTIAL:
  {
    state->last_file_offset = state->last_file_offset + config->access_size_in_bytes;
    /* we don't have to lseek since the pointer moves naturally */
    return;
  }
  case ACCESS_PATTERN_RANDOM:
  {
    // TODO: Refactor align....
    size_t new_file_pos = ((size_t)rand() * 128) % (config->file_size_in_bytes - config->access_size_in_bytes);
    lseek_or_die(state->fd, new_file_pos, SEEK_SET);
    state->last_file_offset = new_file_pos;
    return;
  }
  }
}

/** Extracts the number of seconds from a struct timespec defined by time.h */
static inline double timespec_to_double(const timespec_t *time)
{
  return time->tv_sec + 0.001 * 0.001 * 0.001 * time->tv_nsec;
}

/** Update the tracked values after each io-operation */
static double get_duration(const timespec_t *start, const timespec_t *end)
{
  return timespec_to_double(end) - timespec_to_double(start);
}

static void do_reread_if_needed(const benchmark_config_t *config, benchmark_state_t *state) {
  if (!config->do_reread)
    return;
  state->io_op(state->fd, state->buffer, config->access_size_in_bytes);
  /* Seek back so that we read it twice */
  lseek_or_die(state->fd, state->last_file_offset, SEEK_SET);
}

/** The actual benchmark function.
 *
 * After preparing, it gets the time before, does the io-op and then gets the time afterwards and updates it.
 */
static void do_benchmark(const benchmark_config_t *config, benchmark_state_t *state, benchmark_results_t *results)
{
  timespec_t start, end;
  int res;
  prepare_run(config, state);
  for (size_t i = 0; i < config->number_of_io_op_tests; ++i)
  {
    do_reread_if_needed(config, state);
    clock_gettime(CLOCK_MONOTONIC, &start);
    res = state->io_op(state->fd, state->buffer, config->access_size_in_bytes);
    clock_gettime(CLOCK_MONOTONIC, &end);
    io_op_worked_or_die(res, config->is_read_operation);
    pick_next_mem_position(config, state);
    pick_next_file_position(config, state);
    results->durations[i] = get_duration(&start, &end);
  }
  close_or_die(state->fd);
}

/** Wrapper-function.
 *
 * The only non-static function. Creates the state and wraps the benchmark.
 */
benchmark_results_t *benchmark_file(const benchmark_config_t *config)
{
  benchmark_state_t state;
  benchmark_results_t *results = malloc_or_die(sizeof(benchmark_results_t));

  if (config->drop_cache_first)
    drop_page_cache();
  init_state(config, &state);
  init_file(config, &state);
  init_results(config, results);

  do_benchmark(config, &state, results);
  return results;
}

#endif //IO_BENCHMARK_IO_BENCHMARK_H
