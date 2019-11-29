#include "joinable.h"

Joinable::Entry &Joinable::get_entry(size_t index) {
  return this->entries[index];
}
