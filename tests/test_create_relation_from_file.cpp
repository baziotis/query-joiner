#include "../file_manager.h"
#include "../relation_data.h"

int main() {
  auto *relation_data = FileManager::create_relation("../workloads/small/r0");
  FILE* fp = fopen("test.r0", "w");
  relation_data->print(fp, '|');
  fclose(fp);
  return 0;
}