#include <zconf.h>
#include "relation_storage.h"

RelationStorage::RelationStorage(size_t relation_n) : Array(relation_n) {}

void RelationStorage::print_relations(uint64_t start_index, uint64_t end_index) {
  for (uint64_t index = start_index; index <= end_index; index++)
    (*this)[index].print();
}

