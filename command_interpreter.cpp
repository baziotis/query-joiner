//
// Created by aris on 25/11/19.
//

#include <cstring>
#include "command_interpreter.h"
#include "file_manager.h"

CommandInterpreter::CommandInterpreter(FILE *fp) : fp(fp) {}

int CommandInterpreter::next_relation_filename(char *filename) {
  char command_string[256];
  fscanf(this->fp, "%s\n", command_string);
  if (!strncmp("Done", command_string, sizeof("Done")))
    return CommandRelationFilesEnd;
  strcpy(filename, command_string);
  return CommandReadNext;
}

RelationStorage *CommandInterpreter::get_relation_storage(int index) {
  char relation_filename[256];
  int command = next_relation_filename(relation_filename);
  if (command == CommandInterpreter::CommandRelationFilesEnd)
    return new RelationStorage(index);
  auto *rs = get_relation_storage(index+1);
  RelationData *r = FileManager::create_relation(relation_filename);
  rs->set_relation(index, r);
  return rs;
}

bool is_empty_or_whitespace(char *str) {
  size_t len = strlen(str);
  if (len == 0)
    return true;
  for (size_t i = 0; i < len; i++) {
    if (str[i] != ' ' && str[i] != '\n' && str[i] != '\t')
      return false;
  }
  return true;
}

int CommandInterpreter::next_query(char **query_string) {
  char command_string[1024];
  char *status;

  do {
    status = fgets(command_string, 1024, this->fp);
    if (status == nullptr)
      return CommandEndOfInput;
    command_string[strlen(command_string)-1] = '\0';
  } while (is_empty_or_whitespace(command_string));

  if (!strncmp("F", command_string, sizeof("F")))
    return CommandQueryGroupEnd;
  *query_string = new char[strlen(command_string) + 1];
  strcpy(*query_string, command_string);
  return CommandReadNext;
}