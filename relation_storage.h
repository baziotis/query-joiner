#ifndef SORT_MERGE_JOIN__RELATIONSTORAGE_H_
#define SORT_MERGE_JOIN__RELATIONSTORAGE_H_

#include <cstdint>
#include "relation_data.h"

struct RelationStorage {
  explicit RelationStorage(u64 relation_n);
  RelationData &operator[](u64 index);
  size_t size() const;
  void print_relations(uint64_t start_index = 0, uint64_t end_index = 0);
 private:
  Array<RelationData> relations;
};

#endif //SORT_MERGE_JOIN__RELATIONSTORAGE_H_
