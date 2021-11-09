#include "generic_store.h"


GenericStore::GenericStore(const fs::path& config_path) : DefaultStore(config_path) {
  set_kind("generic");
}

