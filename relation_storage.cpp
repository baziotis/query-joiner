#include "relation_storage.h"

RelationStorage::RelationStorage(u64 relation_n) : relations(relation_n) {}

RelationData &RelationStorage::operator[](u64 index) {
  return relations[index];
}

size_t RelationStorage::size() const {
  return relations.size;
}

void RelationStorage::print_relations(uint64_t start_index, uint64_t end_index) {
  for (uint64_t index = start_index; index <= end_index; index++)
    relations[index].print(STDOUT_FILENO);
}
