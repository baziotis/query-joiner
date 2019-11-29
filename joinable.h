//
// Created by aris on 29/11/19.
//

#ifndef SORT_MERGE_JOIN__JOINABLE_H_
#define SORT_MERGE_JOIN__JOINABLE_H_

#include <cstdint>
#include "array.h"

class Joinable {
 public:
  struct Entry {
    uint64_t key;
    uint64_t row_id;
  };
  Entry &get_entry(size_t index);

 private:
  Array<Entry> entries;
};

#endif //SORT_MERGE_JOIN__JOINABLE_H_
