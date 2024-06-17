#include "page/b_plus_tree_internal_page.h"

#include "index/generic_key.h"

#define pairs_off (data_)
#define pair_size (GetKeySize() + sizeof(page_id_t))
#define key_off 0
#define val_off GetKeySize()

/**
 * TODO: Student Implement
 */
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
void InternalPage::Init(page_id_t page_id, page_id_t parent_id, int key_size, int max_size) {
  page_id = page_id;
  key_size = key_size;
  parent_id = parent_id;
  max_size = max_size;
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
GenericKey *InternalPage::KeyAt(int index) {
  return reinterpret_cast<GenericKey *>(pairs_off + index * pair_size + key_off);
}

void InternalPage::SetKeyAt(int index, GenericKey *key) {
  memcpy(pairs_off + index * pair_size + key_off, key, GetKeySize());
}

page_id_t InternalPage::ValueAt(int index) const {
  return *reinterpret_cast<const page_id_t *>(pairs_off + index * pair_size + val_off);
}

void InternalPage::SetValueAt(int index, page_id_t value) {
  *reinterpret_cast<page_id_t *>(pairs_off + index * pair_size + val_off) = value;
}

int InternalPage::ValueIndex(const page_id_t &value) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (ValueAt(i) == value)
      return i;
  }
  return -1;
}

void *InternalPage::PairPtrAt(int index) {
  return KeyAt(index);
}

void InternalPage::PairCopy(void *dest, void *src, int pair_num) {
  memcpy(dest, src, pair_num * (GetKeySize() + sizeof(page_id_t)));
}
/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 * 用了二分查找
 */
page_id_t InternalPage::Lookup(const GenericKey *key, const KeyManager &KM) {
    // 从第二个键开始查找
    for (int i = 1; i < GetSize(); i++) {
        int cmp = KM.CompareKeys(KeyAt(i), key);
        if (cmp >= 0) {
            // 找到了包含该键的子页面
            return ValueAt(i);
        }
    }
    
    // 如果没找到,返回最后一个子页面
    return ValueAt(GetSize());
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
void InternalPage::PopulateNewRoot(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    // 设置第一个键为无效键
    SetKeyAt(0, nullptr);
    
    // 设置第一个值为旧值
    SetValueAt(0, old_value);
    
    // 设置第二个键为新键
    SetKeyAt(1, new_key);
    
    // 设置第二个值为新值
    SetValueAt(1, new_value);
    
    // 设置当前大小为2
    SetSize(2);
}

/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
int InternalPage::InsertNodeAfter(const page_id_t &old_value, GenericKey *new_key, const page_id_t &new_value) {
    // 找到旧值所在的位置
    int old_index = ValueIndex(old_value);
    
    // 如果没找到,返回0
    if (old_index == -1) {
        return 0;
    }
    
    // 否则,将后面的键值对向后移动一位,插入新的键值对
    int size = GetSize();
    for (int i = size; i > old_index + 1; i--) {
        SetKeyAt(i, KeyAt(i - 1));
        SetValueAt(i, ValueAt(i - 1));
    }
    SetKeyAt(old_index + 1, new_key);
    SetValueAt(old_index + 1, new_value);
    
    // 更新大小并返回
    IncreaseSize(1);
    return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 * buffer_pool_manager 是干嘛的？传给CopyNFrom()用于Fetch数据页
 */
void InternalPage::MoveHalfTo(InternalPage *recipient, BufferPoolManager *buffer_pool_manager) {
    int total_size = GetSize();
    int half_size = total_size / 2;

    CopyNFrom(recipient->PairPtrAt(0), half_size, buffer_pool_manager);
    recipient->SetSize(half_size);
    SetSize(total_size - half_size);
}

/* Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 *
 */
void InternalPage::CopyNFrom(void *src, int size, BufferPoolManager *buffer_pool_manager) {
    void *dest = PairPtrAt(GetSize());
    PairCopy(dest, src, size);
    for (int i = 0; i < size; i++) {
        page_id_t child_page_id = ValueAt(GetSize() + i);
        buffer_pool_manager->UpdatePageParentId(child_page_id, GetPageId());
    }
    IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
void InternalPage::Remove(int index) {
    int size = GetSize();
    for (int i = index + 1; i < size; i++) {
        SetKeyAt(i - 1, KeyAt(i));
        SetValueAt(i - 1, ValueAt(i));
    }
    IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
page_id_t InternalPage::RemoveAndReturnOnlyChild() {
    page_id_t child_page_id = ValueAt(0);
    SetSize(0);
    return child_page_id;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveAllTo(InternalPage *recipient, GenericKey *middle_key, BufferPoolManager *buffer_pool_manager) {
    recipient->CopyNFrom(PairPtrAt(0), GetSize(), buffer_pool_manager);
    recipient->SetKeyAt(recipient->GetSize(), middle_key);
    recipient->IncreaseSize(1);
    SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
void InternalPage::MoveFirstToEndOf(InternalPage *recipient, GenericKey *middle_key,
BufferPoolManager *buffer_pool_manager) {
      recipient->CopyLastFrom(KeyAt(0), ValueAt(0), buffer_pool_manager);
    recipient->SetKeyAt(recipient->GetSize(), middle_key);
    recipient->IncreaseSize(1);
    Remove(0);
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyLastFrom(GenericKey *key, const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    SetKeyAt(GetSize(), key);
    SetValueAt(GetSize(), value);
    buffer_pool_manager->UpdatePageParentId(value, GetPageId());
    IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
void InternalPage::MoveLastToFrontOf(InternalPage *recipient, GenericKey *middle_key,
                                     BufferPoolManager *buffer_pool_manager) {
    int last_index = GetSize() - 1;
    recipient->CopyFirstFrom(ValueAt(last_index), buffer_pool_manager);
    recipient->SetKeyAt(0, KeyAt(last_index));
    recipient->SetKeyAt(1, middle_key);
    recipient->IncreaseSize(1);
    Remove(last_index);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
void InternalPage::CopyFirstFrom(const page_id_t value, BufferPoolManager *buffer_pool_manager) {
    for (int i = GetSize(); i > 0; i--) {
        SetKeyAt(i, KeyAt(i - 1));
        SetValueAt(i, ValueAt(i - 1));
    }
    SetValueAt(0, value);
    buffer_pool_manager->UpdatePageParentId(value, GetPageId());
    IncreaseSize(1);
}