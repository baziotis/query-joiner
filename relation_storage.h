#ifndef SORT_MERGE_JOIN__RELATIONSTORAGE_H_
#define SORT_MERGE_JOIN__RELATIONSTORAGE_H_

#include <cstdint>
#include "relation_data.h"
#include "common.h"
#include "array.h"
#include "command_interpreter.h"

struct RelationStorage : public Array<RelationData> {
  explicit RelationStorage(size_t relation_n);
  void print_relations(uint64_t start_index = 0, uint64_t end_index = 0);
  /**
   * Inserts relation data objects to the relation storage by reading the filenames and allocating the required memory
   * @param start: An iterator to the start of the filename sequence
   * @param end: An iterator to the end of the filename sequence
   */
  void insert_from_filenames(CommandInterpreter::CommandIterator start, CommandInterpreter::CommandIterator end);
};

#endif //SORT_MERGE_JOIN__RELATIONSTORAGE_H_
