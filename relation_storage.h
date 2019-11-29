#ifndef SORT_MERGE_JOIN__RELATION_STORAGE_H_
#define SORT_MERGE_JOIN__RELATION_STORAGE_H_

#include <cstdint>
#include "relation_data.h"
class RelationStorage {
 public:
  explicit RelationStorage(uint64_t relation_n);
  ~RelationStorage();
  RelationData *get_relation(uint64_t index);
  void set_relation(uint64_t index, RelationData *relation_data);
  void print_relations(uint64_t start_index = 0, uint64_t end_index = 0);
  /**
   * @return The number of relations in this storage.
   */
  uint64_t relation_count();
 private:
  RelationData **relations;
  uint64_t relation_n;
};

#endif //SORT_MERGE_JOIN__RELATION_STORAGE_H_
