//
//  YCSB-C
//
//  Created by Jinglei Ren on 12/17/14.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//

#include "sharkdb_db.h"
#include "core/db_factory.h"

#include <sharkdb.h>

using std::cout;
using std::endl;

namespace ycsbc {

void SharkDB::Init() {
}

DB::Status SharkDB::Read(const std::string &table, const std::string &key,
                         const std::vector<std::string> *fields, std::vector<Field> &result) {
  return kOK;
}

DB::Status SharkDB::Scan(const std::string &table, const std::string &key, int len,
                         const std::vector<std::string> *fields,
                         std::vector<std::vector<Field>> &result) {
  return kOK;
}

DB::Status SharkDB::Update(const std::string &table, const std::string &key,
                           std::vector<Field> &values) {
  return kOK;
}

DB::Status SharkDB::Insert(const std::string &table, const std::string &key,
                           std::vector<Field> &values) {
  return kOK;
}

DB::Status SharkDB::Delete(const std::string &table, const std::string &key) {
  
  return kOK;
}

DB *NewSharkDB() {
  return new SharkDB;
}

const bool registered = DBFactory::RegisterDB("sharkdb", NewSharkDB);

} // ycsbc
