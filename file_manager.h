//
// Created by aris on 23/11/19.
//

#ifndef SORT_MERGE_JOIN__FILE_MANAGER_H_
#define SORT_MERGE_JOIN__FILE_MANAGER_H_

#include "relation_data.h"
class FileManager {
 public:
  static RelationData *create_relation(const char *filename);

};

#endif //SORT_MERGE_JOIN__FILE_MANAGER_H_
