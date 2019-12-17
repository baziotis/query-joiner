#ifndef SORT_MERGE_JOIN__COMMAND_INTERPRETER_H_
#define SORT_MERGE_JOIN__COMMAND_INTERPRETER_H_

#include "stretchy_buf.h"
#include "tokenizer.h"
#include <cstdio>
#include <cstring>
#include <unistd.h>

struct CommandInterpreter {
  explicit CommandInterpreter(int fd = STDIN_FILENO);
  explicit CommandInterpreter(FILE *fp);

  struct CommandIterator {
    explicit CommandIterator(Tokenizer tokenizer)
        : tokenizer_{tokenizer}, command_len_{0U} {
      command_ = tokenizer_.next_token();
      if (command_ != nullptr) {
        command_len_ = strlen(command_);
      }
    }

    CommandIterator &operator++() {
      command_ = tokenizer_.next_token();
      command_len_ = command_ ? strlen(command_) : 0U;
      return *this;
    }

    char *operator*() const {
      return command_;
    }

    char *operator->() const {
      return command_;
    }

    bool operator==(CommandIterator &rhs) const {
      bool res = command_len_ == rhs.command_len_ && !strncmp(command_, rhs.command_, command_len_);
      return res;
    }

    bool operator!=(CommandIterator &rhs) const {
      bool res = command_len_ != rhs.command_len_ || strncmp(command_, rhs.command_, command_len_) != 0;
      return res;
    }

    char *command_;
    size_t command_len_;
    Tokenizer tokenizer_;
  };

  /**
   * Read the next group of filenames from the stream provided to the interpreter
   * until the string DONE is encountered. These filenames get stored in
   * the internal buffer of the interpreter which can be accessed using the command iterator
   * @return True if relation_filenames read (read Done), False if EOF encountered
   */
  bool read_relation_filenames();

  /**
   * Read the next group of queries from the stream provided to the interpreter
   * until the character 'F' is encountered. These queries get stored in
   * the internal buffer of the interpreter. These batches can be accessed using the command iterator
   * @return True if query batch read (read 'F'), False if EOF encountered
   */
  bool read_query_batch();

  /**
   * @return An iterator which provides commands lazily
   */
  CommandIterator begin();

  /**
   * @return A const iterator which provides commands lazily
   */
  const CommandIterator begin() const;

  /**
   * @return An iterator which indicates the end of commands
   */
  CommandIterator end();

  /**
   * @return A const iterator which indicates the end of commands
   */
  const CommandIterator end() const;

  /**
   * @return The remaining number of commands to be consumed
   */
  size_t remaining_commands();

 private:
  int fd;
  StretchyBuf<char> command_buffer;
};

#endif //SORT_MERGE_JOIN__COMMAND_INTERPRETER_H_
