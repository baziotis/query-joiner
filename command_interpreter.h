//
// Created by aris on 25/11/19.
//

#ifndef SORT_MERGE_JOIN__COMMAND_INTERPRETER_H_
#define SORT_MERGE_JOIN__COMMAND_INTERPRETER_H_

#include <cstdio>
#include "relation_storage.h"

class CommandInterpreter {
 public:
  explicit CommandInterpreter(FILE* fp = stdin);
  /**
   * Allocates the Relation Storage data structure.
   */
  RelationStorage *get_relation_storage(int index = 0);
  /**
   * Reads the next filename from the file.
   * @param filename Allocated string for the filename.
   * @return CommandRelationFilesEnd if there is no filename, non-zero otherwise.
   */
  int next_relation_filename(char *filename);
  /**
   * Allocates a new string holding the next query.
   * If no more queries, or end of query group, the string is not allocated.
   * @param query Allocated string for the filename.
   * @return 0 if there is no next query, non-zero otherwise.
   */
  int next_query(char **query_string);
  static const int CommandEndOfInput = 0;
  static const int CommandReadNext = 1;
  static const int CommandRelationFilesEnd = 420;
  static const int CommandQueryGroupEnd = 666;
 private:
  FILE *fp;
};

#endif //SORT_MERGE_JOIN__COMMAND_INTERPRETER_H_
