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
  this->buf = new char[SHARKDB_VAL_BYTES];
  this->db_impl = sharkdb_init();
}

void SharkDB::Cleanup() {
  std::pair<bool, sharkdb_cqev> pr;
  size_t n_cq_polled = 0;
  do  {
    n_cq_polled += 1;
  } while ((pr = sharkdb_cpoll_cq(this->db_impl)).second != SHARKDB_CQEV_FAIL);
  printf("n_cq_polled: %lu\n", n_cq_polled);

  sharkdb_drain(this->db_impl);
  delete[] this->buf;
  sharkdb_free(this->db_impl);
}

DB::Status SharkDB::Read(const std::string &table, const std::string &key,
                         const std::vector<std::string> *fields, std::vector<Field> &results) {
  assert(key.size() == SHARKDB_KEY_BYTES && fields == NULL && results.size() == 0);
  results.emplace_back();
  results[0].name = FIELD_NAME;
  results[0].value = std::string(this->buf, SHARKDB_VAL_BYTES);
  /*    Technically have sharkdb read into this buffer, that way I don't have to manage string
        object mallocs. */
  sharkdb_read_async(this->db_impl, key.data(), this->buf);
  return kOK;
}

DB::Status SharkDB::Scan(const std::string &table, const std::string &key, int len,
                         const std::vector<std::string> *fields,
                         std::vector<std::vector<Field>> &result) {
  return kNotImplemented;
}

DB::Status SharkDB::Update(const std::string &table, const std::string &key, std::vector<Field> &values) {
  assert(key.size() == SHARKDB_KEY_BYTES && values.size() == 1 && values[0].name == FIELD_NAME && values[0].value.size() == SHARKDB_VAL_BYTES);
  sharkdb_write_async(this->db_impl, key.data(), values[0].value.data());
  return kOK;
}

DB::Status SharkDB::Insert(const std::string &table, const std::string &key,
                           std::vector<Field> &values) {
  assert(key.size() == SHARKDB_KEY_BYTES && values.size() == 1 && values[0].name == FIELD_NAME && values[0].value.size() == SHARKDB_VAL_BYTES);
  sharkdb_write_async(this->db_impl, key.data(), values[0].value.data());
  return kOK;
}

DB *NewSharkDB() {
  return new SharkDB;
}

const bool registered = DBFactory::RegisterDB("sharkdb", NewSharkDB);

} // ycsbc
