#include <zconf.h>
#include "relation_storage.h"
#include "utils.h"
#include "report_utils.h"

RelationStorage::RelationStorage(size_t relation_n) : Array(relation_n) {}

void RelationStorage::print_relations(uint64_t start_index, uint64_t end_index) {
  for (uint64_t index = start_index; index <= end_index; index++)
    (*this)[index].print();
}

void RelationStorage::insert_from_filenames(CommandInterpreter::CommandIterator start,
                                            CommandInterpreter::CommandIterator end) {
  for (; start != end; ++start) {
    char *filename = *start;
    if (!file_exists(filename)) {
      report_error(R"(File "%s" does not exist. Aborting...)", filename);
      return;
    }
    this->push(RelationData::from_binary_file(filename));
  }
}

