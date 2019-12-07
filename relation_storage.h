#ifndef SORT_MERGE_JOIN__RELATIONSTORAGE_H_
#define SORT_MERGE_JOIN__RELATIONSTORAGE_H_

#include <cstdint>
#include "relation_data.h"
#include "common.h"
#include "array.h"

struct RelationStorage : public Array<RelationData> {
  explicit RelationStorage(size_t relation_n);
  void print_relations(uint64_t start_index = 0, uint64_t end_index = 0);
};

#endif //SORT_MERGE_JOIN__RELATIONSTORAGE_H_
