#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
bool TableHeap::InsertTuple(Row &row, Txn *txn) { 
  Page *page;
  uint32_t page_id;
  uint32_t slot_num;

  // 找到第一个能容纳该记录的TablePage
  for (page_id = first_page_id_; page_id != INVALID_PAGE_ID; page_id = pages_[page_id]->GetNextPageId()) {
    page = pages_[page_id];
    if (page->InsertTuple(row, slot_num)) {
      row->rid_.Set(page_id, slot_num);
      return DB_SUCCESS;
    }
  }

  // 如果没有合适的TablePage,则创建新的TablePage
  page = buffer_pool_manager_->NewPage(&page_id);
  if (page == nullptr) {
    return DB_FAILED;
  }

  page->InsertTuple(row, slot_num);
  row->rid_.Set(page_id, slot_num);

  pages_[page_id] = reinterpret_cast<TablePage *>(page->GetData());
  pages_[page_id]->SetNextPageId(INVALID_PAGE_ID);
  if (first_page_id_ == INVALID_PAGE_ID) {
    first_page_id_ = page_id;
  } else {
    pages_[last_page_id_]->SetNextPageId(page_id);
  }
  last_page_id_ = page_id;

  return DB_SUCCESS;
}

bool TableHeap::MarkDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  // If the page could not be found, then abort the recovery.
  if (page == nullptr) {
    return false;
  }
  // Otherwise, mark the tuple as deleted.
  page->WLatch();
  page->MarkDelete(rid, txn, lock_manager_, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
  return true;
}

/**
 * TODO: Student Implement
 */
bool TableHeap::UpdateTuple(Row &row, const RowId &rid, Txn *txn) {
  Page *page = buffer_pool_manager_->FetchPage(rid->GetPageId());
  if (page == nullptr) {
    return DB_FAILED;
  }

  TablePage *table_page = reinterpret_cast<TablePage *>(page->GetData());
  bool update_success = table_page->UpdateTuple(new_row, rid->GetSlotNum());
  buffer_pool_manager_->UnpinPage(rid->GetPageId(), update_success);

  if (update_success) {
    new_row->rid_ = *rid;
    return DB_SUCCESS;
  } else {
    return DB_FAILED;
  }

}

/**
 * TODO: Student Implement
 */
void TableHeap::ApplyDelete(const RowId &rid, Txn *txn) {
  Page *page = buffer_pool_manager_->FetchPage(rid->GetPageId());
  if (page == nullptr) {
    return DB_FAILED;
  }

  TablePage *table_page = reinterpret_cast<TablePage *>(page->GetData());
  bool delete_success = table_page->ApplyDelete(rid->GetSlotNum());
  buffer_pool_manager_->UnpinPage(rid->GetPageId(), delete_success);

  return delete_success ? DB_SUCCESS : DB_FAILED;
}

void TableHeap::RollbackDelete(const RowId &rid, Txn *txn) {
  // Find the page which contains the tuple.
  auto page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(rid.GetPageId()));
  assert(page != nullptr);
  // Rollback to delete.
  page->WLatch();
  page->RollbackDelete(rid, txn, log_manager_);
  page->WUnlatch();
  buffer_pool_manager_->UnpinPage(page->GetTablePageId(), true);
}

/**
 * TODO: Student Implement
 */
bool TableHeap::GetTuple(Row *row, Txn *txn) {
  Page *page = buffer_pool_manager_->FetchPage(row->rid_.GetPageId());
  if (page == nullptr) {
    return DB_FAILED;
  }

  TablePage *table_page = reinterpret_cast<TablePage *>(page->GetData());
  bool get_success = table_page->GetTuple(row, row->rid_.GetSlotNum());
  buffer_pool_manager_->UnpinPage(row->rid_.GetPageId(), false);

  return get_success ? DB_SUCCESS : DB_FAILED;
}

void TableHeap::DeleteTable(page_id_t page_id) {
  if (page_id != INVALID_PAGE_ID) {
    auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(page_id));  // 删除table_heap
    if (temp_table_page->GetNextPageId() != INVALID_PAGE_ID)
      DeleteTable(temp_table_page->GetNextPageId());
    buffer_pool_manager_->UnpinPage(page_id, false);
    buffer_pool_manager_->DeletePage(page_id);
  } else {
    DeleteTable(first_page_id_);
  }
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::Begin(Txn *txn) {
  auto temp_table_page = reinterpret_cast<TablePage *>(buffer_pool_manager_->FetchPage(first_page_id_));
  RowId rid;temp_table_page->GetFirstTupleRid(&rid);
  return TableIterator(this,rid);
}

/**
 * TODO: Student Implement
 */
TableIterator TableHeap::End() {
  return TableIterator(this,INVALID_ROWID);
}
