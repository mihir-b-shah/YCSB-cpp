//
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "sharkdb_db.h"
#include "core/db_factory.h"

#include <cassert>

using std::cout;
using std::endl;

static const char* FIELD_NAME = "field0";

namespace ycsbc {

void SharkDB::Init() {
    this->db_impl = sharkdb_init();
}

DB::Status SharkDB::Read(const std::string &table, const std::string &key,
                         const std::vector<std::string> *fields, std::vector<Field> &result) {
  assert(key.size() == SHARKDB_KEY_SIZE && fields == NULL && result.size() == 0);
  result.emplace_back();
  result[0].name = FIELD_NAME;
  result[0].value.reserve(SHARKDB_VAL_SIZE);
  sharkdb_read(this->db_impl, key.data(), result[0].value.data());
  return kOK;
}

DB::Status SharkDB::Scan(const std::string &table, const std::string &key, int len,
                         const std::vector<std::string> *fields,
                         std::vector<std::vector<Field>> &result) {
  return kNotImplemented;
}

DB::Status SharkDB::Update(const std::string &table, const std::string &key,
                           std::vector<Field> &values) {
  assert(key.size() == SHARKDB_KEY_SIZE && values.size() == 1 && values[0].name == FIELD_NAME && values[0].value.size() == SHARKDB_VAL_SIZE);
  sharkdb_update(this->db_impl, key.data(), values[0].value.data()); 
  return kOK;
}

DB::Status SharkDB::Insert(const std::string &table, const std::string &key,
                           std::vector<Field> &values) {
  assert(key.size() == SHARKDB_KEY_SIZE && values.size() == 1 && values[0].name == FIELD_NAME && values[0].value.size() == SHARKDB_VAL_SIZE);
  sharkdb_insert(this->db_impl, key.data(), values[0].value.data()); 
  return kOK;
}

DB::Status SharkDB::Delete(const std::string &table, const std::string &key) {
  assert(key.size() == SHARKDB_KEY_SIZE);
  sharkdb_delete(this->db_impl, key.data());
  return kOK;
}

DB *NewSharkDB() {
  return new SharkDB;
}

const bool registered = DBFactory::RegisterDB("sharkdb", NewSharkDB);

} // ycsbc
