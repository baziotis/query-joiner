#include <cstring>
#include "command_interpreter.h"
#include "utils.h"

static constexpr char *DONE = (char *const) "Done";
static constexpr size_t DONE_LEN = 4;

CommandInterpreter::CommandInterpreter(int fd) : fd{fd}, command_buffer(100) {}
CommandInterpreter::CommandInterpreter(FILE *fp) : CommandInterpreter(fp->_fileno) {}

bool CommandInterpreter::read_relation_filenames() {
  command_buffer.reset();
  bool done_encountered{false};
  while (!done_encountered) {
    ssize_t bytes_read = read_line_from_stream(command_buffer, this->fd);
    // If we read at least DONE_LEN + 1 characters (+1 for newline)
    // then check if the last DONE_LEN characters before newline are equal to string "done"
    if (bytes_read == -1) return false;
    done_encountered = bytes_read == DONE_LEN + 1 &&
        !strncasecmp(command_buffer.data + command_buffer.len - bytes_read, DONE, DONE_LEN);
  }
  command_buffer.len -= DONE_LEN + 1;
}

bool CommandInterpreter::read_query_batch() {
  command_buffer.reset();
  bool f_encountered{false};
  while (!f_encountered) {
    ssize_t bytes_read = read_line_from_stream(command_buffer, this->fd);
    if (bytes_read == -1) return false;
    // Check if we read exactly 2 characters (+1 for newline)
    // and then check if read 'F' character which indicates the end of the query batch
    f_encountered = bytes_read == 2 && command_buffer[command_buffer.len - 2] == 'F';
  }
  command_buffer.len -= 2;
}

CommandInterpreter::CommandIterator CommandInterpreter::begin() {
  Tokenizer tok{command_buffer.data, command_buffer.len, '\n'};
  return CommandInterpreter::CommandIterator(tok);
}

const CommandInterpreter::CommandIterator CommandInterpreter::begin() const {
  Tokenizer tok{command_buffer.data, command_buffer.len, '\n'};
  return CommandInterpreter::CommandIterator(tok);
}

CommandInterpreter::CommandIterator CommandInterpreter::end() {
  return CommandInterpreter::CommandIterator(Tokenizer::emtpy_tokenizer());
}

const CommandInterpreter::CommandIterator CommandInterpreter::end() const {
  return CommandInterpreter::CommandIterator(Tokenizer::emtpy_tokenizer());
}

size_t CommandInterpreter::remaining_commands() {
  Tokenizer tok{command_buffer.data, command_buffer.len, '\n'};
  return tok.remaining_tokens();
}
