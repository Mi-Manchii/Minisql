#include "storage/table_iterator.h"

#include "common/macros.h"
#include "storage/table_heap.h"

/**
 * TODO: Student Implement
 */
TableIterator::TableIterator(TableHeap *table_heap, RowId rid, Txn *txn) {
    this->table_heap_ = table_heap;
    this->rid_ = rid;
    this->txn_ = txn;
}

TableIterator::TableIterator(const TableIterator &other) {
  this->table_heap_ = other.table_heap_;
  this->rid_ = other.rid_;
  this->txn_ = other.txn_;
}

TableIterator::~TableIterator() {

}

bool TableIterator::operator==(const TableIterator &itr) const {
  return (this->table_heap_ == itr.table_heap_) && (this->rid_ == itr.rid_) && (this->txn_ == itr.txn_);
}

bool TableIterator::operator!=(const TableIterator &itr) const {
  return !(*this == itr);
}

const Row &TableIterator::operator*() {
  ASSERT(tableHeap_, "TableHeap is nullptr.");
  Row* row = new Row(currentRowID_);
  tableHeap_->GetTuple(row, nullptr);
  ASSERT(row, "Invalid row.");
  return *row;
}

Row *TableIterator::operator->() {
  ASSERT(tableHeap_, "TableHeap is nullptr.");
  Row* row = new Row(currentRowID_);
  tableHeap_->GetTuple(row, nullptr);
  ASSERT(row, "Invalid row.");
  return row;
}

TableIterator &TableIterator::operator=(const TableIterator &itr) noexcept {
    if (this != &itr) {
        this->table_heap_ = itr.table_heap_;
        this->rid_ = itr.rid_;
        this->txn_ = itr.txn_;
    }
    return *this;
}

// ++iter
TableIterator &TableIterator::operator++() {
  ASSERT(tableHeap_, "TableHeap is nullptr.");
  Row cur_row;
  cur_row.SetRowId(currentRowID_);
  tableHeap_->GetTuple(&cur_row, nullptr);
  currentRowID_ = tableHeap_->GetNextRowId(&cur_row, nullptr);
  return *this;
}

// iter++
TableIterator TableIterator::operator++(int) {
  ASSERT(tableHeap_, "TableHeap is nullptr.");
  Row cur_row;
  cur_row.SetRowId(currentRowID_);
  tableHeap_->GetTuple(&cur_row, nullptr);
  currentRowID_ = tableHeap_->GetNextRowId(&cur_row, nullptr);
  return *this;
}
