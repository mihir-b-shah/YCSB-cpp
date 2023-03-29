
#ifndef YCSB_C_SHARKDB_DB_H_
#define YCSB_C_SHARKDB_DB_H_

#include "core/db.h"
#include "core/properties.h"

#include <iostream>
#include <string>

#include <sharkdb.h>

namespace ycsbc {

// just ignore table for now.
class SharkDB : public DB {
 public:
  void Init();

  Status Read(const std::string &table, const std::string &key,
              const std::vector<std::string> *fields, std::vector<Field> &result);

  Status Scan(const std::string &table, const std::string &key, int len,
              const std::vector<std::string> *fields, std::vector<std::vector<Field>> &result);

  Status Update(const std::string &table, const std::string &key, std::vector<Field> &values);

  Status Insert(const std::string &table, const std::string &key, std::vector<Field> &values);

  Status Delete(const std::string &table, const std::string &key);
 
 private:
  sharkdb_p db_impl;
};

DB *NewSharkDB();

} // ycsbc

#endif // YCSB_C_SHARKDB_DB_H_

