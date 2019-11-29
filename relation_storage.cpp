#include "relation_storage.h"

RelationStorage::RelationStorage(uint64_t relation_n) : relation_n(relation_n) {
  this->relations = new RelationData*[relation_n];
}

RelationStorage::~RelationStorage() {
  delete this->relations;
  for (size_t i = 0; i < this->relation_n; i++)
    delete this->relations[i];
}

RelationData *RelationStorage::get_relation(uint64_t index) {
  return this->relations[index];
}
uint64_t RelationStorage::relation_count() {
  return relation_n;
}
void RelationStorage::set_relation(uint64_t index, RelationData *relation_data) {
  this->relations[index] = relation_data;
}
void RelationStorage::print_relations(uint64_t start_index, uint64_t end_index) {
  for (uint64_t index = start_index; index <= end_index; index++)
    this->get_relation(index)->print();
}
