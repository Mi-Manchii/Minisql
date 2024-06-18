#include "catalog/catalog.h"

void CatalogMeta::SerializeTo(char *buf) const {
  ASSERT(GetSerializedSize() <= PAGE_SIZE, "Failed to serialize catalog metadata to disk.");
  MACH_WRITE_UINT32(buf, CATALOG_METADATA_MAGIC_NUM);
  buf += 4;
  MACH_WRITE_UINT32(buf, table_meta_pages_.size());
  buf += 4;
  MACH_WRITE_UINT32(buf, index_meta_pages_.size());
  buf += 4;
  for (auto iter : table_meta_pages_) {
    MACH_WRITE_TO(table_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
  for (auto iter : index_meta_pages_) {
    MACH_WRITE_TO(index_id_t, buf, iter.first);
    buf += 4;
    MACH_WRITE_TO(page_id_t, buf, iter.second);
    buf += 4;
  }
}

CatalogMeta *CatalogMeta::DeserializeFrom(char *buf) {
  // check valid
  uint32_t magic_num = MACH_READ_UINT32(buf);
  buf += 4;
  ASSERT(magic_num == CATALOG_METADATA_MAGIC_NUM, "Failed to deserialize catalog metadata from disk.");
  // get table and index nums
  uint32_t table_nums = MACH_READ_UINT32(buf);
  buf += 4;
  uint32_t index_nums = MACH_READ_UINT32(buf);
  buf += 4;
  // create metadata and read value
  CatalogMeta *meta = new CatalogMeta();
  for (uint32_t i = 0; i < table_nums; i++) {
    auto table_id = MACH_READ_FROM(table_id_t, buf);
    buf += 4;
    auto table_heap_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->table_meta_pages_.emplace(table_id, table_heap_page_id);
  }
  for (uint32_t i = 0; i < index_nums; i++) {
    auto index_id = MACH_READ_FROM(index_id_t, buf);
    buf += 4;
    auto index_page_id = MACH_READ_FROM(page_id_t, buf);
    buf += 4;
    meta->index_meta_pages_.emplace(index_id, index_page_id);
  }
  return meta;
}

/**
 * TODO: Student Implement
 */
uint32_t CatalogMeta::GetSerializedSize() const {
  return sizeof(uint32_t) * 3  // magic num + table size + index size
         + (table_meta_pages_.size() + index_meta_pages_.size()) 
         * sizeof(uint32_t) * 2; // each (key, value) pair
}

CatalogMeta::CatalogMeta() {}

/**
 * TODO: Student Implement
 */
CatalogManager::CatalogManager(BufferPoolManager *buffer_pool_manager, LockManager *lock_manager,
                               LogManager *log_manager, bool init)
    : buffer_pool_manager_(buffer_pool_manager), lock_manager_(lock_manager), log_manager_(log_manager) {
  if (init) {
    // 如果是初始化,创建新的CatalogMeta
    catalog_meta_ = CatalogMeta::NewInstance();
    next_table_id_ = catalog_meta_->GetNextTableId();
    next_index_id_ = catalog_meta_->GetNextIndexId();
  } else {
    // 否则从buffer pool中加载CatalogMeta
    Page* page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
    catalog_meta_ = CatalogMeta::DeserializeFrom(page->GetData());
    buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, false);
    
    // 加载所有表和索引
    for (auto &item : *catalog_meta_->GetTableMetaPages()) {
      LoadTable(item.first, item.second);
    }
    for (auto &item : *catalog_meta_->GetIndexMetaPages()) {
      LoadIndex(item.first, item.second);
    }
  }
}

CatalogManager::~CatalogManager() {
  FlushCatalogMetaPage();
  delete catalog_meta_;
  for (auto iter : tables_) {
    delete iter.second;
  }
  for (auto iter : indexes_) {
    delete iter.second;
  }
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateTable(const string &table_name, TableSchema *schema, Txn *txn, TableInfo *&table_info) {
  if (table_names_.find(table_name) != table_names_.end()) {
    return DB_TABLE_ALREADY_EXIST;
  }

  table_id_t table_id = next_table_id_++;
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  
  // 创建table heap和table meta data
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, page_id, schema, log_manager_, lock_manager_);
  TableMetadata *table_meta = TableMetadata::Create(table_id, table_name, page_id, schema);

  // 创建table info
  table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);
  
  // 更新catalog meta
  catalog_meta_->table_meta_pages_[table_id] = page_id;
  table_names_[table_name] = table_id;
  tables_[table_id] = table_info;

  // 将table meta data序列化到page中
  table_meta->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const string &table_name, TableInfo *&table_info) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  table_id_t table_id = table_names_[table_name];
  table_info = tables_[table_id];
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTables(vector<TableInfo *> &tables) const {
  tables.clear();
  for (auto &pair : tables_) {
    tables.push_back(pair.second);
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::CreateIndex(const std::string &table_name, const string &index_name,
                                    const std::vector<std::string> &index_keys, Txn *txn, IndexInfo *&index_info,
                                    const string &index_type) {
  // 检查表是否存在
  TableInfo *table_info;
  if (GetTable(table_name, table_info) != DB_SUCCESS) {
    return DB_TABLE_NOT_EXIST;
  }

  // 检查索引是否已存在
  if (index_names_[table_name].find(index_name) != index_names_[table_name].end()) {
    return DB_INDEX_ALREADY_EXIST;
  }

  // 获取索引的列
  std::vector<uint32_t> key_map;
  auto schema = table_info->GetSchema();
  for (const auto &key : index_keys) {
    uint32_t index;
    if (schema->GetColumnIndex(key, index) != DB_SUCCESS) {
      return DB_COLUMN_NOT_EXIST;
    }
    key_map.push_back(index);
  }

  // 创建索引元数据和索引信息
  index_id_t index_id = next_index_id_++;
  IndexMetadata *index_meta = IndexMetadata::Create(index_id, index_name, table_info->GetTableId(), key_map);
  index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);

  // 将索引元数据序列化到新页面
  page_id_t page_id;
  Page *page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) {
    return DB_FAILED;
  }
  index_meta->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(page_id, true);

  // 更新目录元数据
  catalog_meta_->index_meta_pages_[index_id] = page_id;
  index_names_[table_name][index_name] = index_id;
  indexes_[index_id] = index_info;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetIndex(const std::string &table_name, const std::string &index_name,
                                 IndexInfo *&index_info) const {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  if (index_names_.at(table_name).find(index_name) == index_names_.at(table_name).end()) {
    return DB_INDEX_NOT_FOUND;
  }
  
  index_id_t index_id = index_names_.at(table_name).at(index_name);
  index_info = indexes_.at(index_id);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTableIndexes(const std::string &table_name, std::vector<IndexInfo *> &indexes) const {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  indexes.clear();
  for (const auto &pair : index_names_.at(table_name)) {
    index_id_t index_id = pair.second;
    indexes.push_back(indexes_.at(index_id));
  }
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropTable(const string &table_name) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }

  table_id_t table_id = table_names_[table_name];
  TableInfo *table_info = tables_[table_id];

  // 删除该表的所有索引
  std::vector<IndexInfo *> table_indexes;
  GetTableIndexes(table_name, table_indexes);
  for (auto index_info : table_indexes) {
    DropIndex(table_name, index_info->GetIndexName());
  }

  // 删除表元数据和堆
  page_id_t table_meta_page_id = table_info->GetRootPageId();
  buffer_pool_manager_->DeletePage(table_meta_page_id);
  delete table_info->GetTableHeap();

  // 从CatalogManager和CatalogMeta中移除表信息
  table_names_.erase(table_name);
  tables_.erase(table_id);
  catalog_meta_->table_meta_pages_.erase(table_id);

  // 刷新CatalogMeta
  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::DropIndex(const string &table_name, const string &index_name) {
  if (table_names_.find(table_name) == table_names_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  if (index_names_[table_name].find(index_name) == index_names_[table_name].end()) {
    return DB_INDEX_NOT_FOUND;
  }

  index_id_t index_id = index_names_[table_name][index_name];
  IndexInfo *index_info = indexes_[index_id];

  // 删除索引
  index_info->GetIndex()->Destroy();
  delete index_info;

  // 从CatalogManager和CatalogMeta中移除索引信息
  catalog_meta_->index_meta_pages_.erase(index_id);
  index_names_[table_name].erase(index_name);
  indexes_.erase(index_id);

  // 如果表没有其他索引了,也要删除表在index_names_中的入口
  if (index_names_[table_name].empty()) {
    index_names_.erase(table_name);
  }

  // 刷新CatalogMeta
  FlushCatalogMetaPage();

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::FlushCatalogMetaPage() const {
  auto page = buffer_pool_manager_->FetchPage(CATALOG_META_PAGE_ID);
  if (page == nullptr) {
    return DB_FAILED;
  }
  catalog_meta_->SerializeTo(page->GetData());
  buffer_pool_manager_->UnpinPage(CATALOG_META_PAGE_ID, true);
  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadTable(const table_id_t table_id, const page_id_t page_id) {
  // 获取表元数据页
  Page* page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    return DB_FAILED;
  }

  // 反序列化表元数据
  TableMetadata *table_meta = nullptr;
  TableMetadata::DeserializeFrom(page->GetData(), table_meta);
  buffer_pool_manager_->UnpinPage(page_id, false);

  // 创建表堆和表信息
  TableHeap *table_heap = TableHeap::Create(buffer_pool_manager_, table_meta->GetFirstPageId(), table_meta->GetSchema(), 
                                            log_manager_, lock_manager_);
  TableInfo *table_info = TableInfo::Create();
  table_info->Init(table_meta, table_heap);

  // 将表信息加入到映射中
  tables_[table_id] = table_info;
  table_names_[table_meta->GetTableName()] = table_id;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::LoadIndex(const index_id_t index_id, const page_id_t page_id) {
  // 获取索引元数据页
  Page* page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    return DB_FAILED;
  }

  // 反序列化索引元数据
  IndexMetadata *index_meta = nullptr;
  IndexMetadata::DeserializeFrom(page->GetData(), index_meta);
  buffer_pool_manager_->UnpinPage(page_id, false);

  // 获取对应的表信息
  TableInfo *table_info = nullptr;
  if (GetTable(index_meta->GetTableId(), table_info) != DB_SUCCESS) {
    return DB_FAILED;
  }

  // 创建索引信息
  IndexInfo *index_info = IndexInfo::Create();
  index_info->Init(index_meta, table_info, buffer_pool_manager_);

  // 将索引信息加入到映射中
  indexes_[index_id] = index_info;
  index_names_[table_info->GetTableName()][index_meta->GetIndexName()] = index_id;

  return DB_SUCCESS;
}

/**
 * TODO: Student Implement
 */
dberr_t CatalogManager::GetTable(const table_id_t table_id, TableInfo *&table_info) {
  if (tables_.find(table_id) == tables_.end()) {
    return DB_TABLE_NOT_EXIST;
  }
  table_info = tables_[table_id];
  return DB_SUCCESS;
}