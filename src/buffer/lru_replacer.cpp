#include "buffer/lru_replacer.h"
vector<frame_id_t> lru_list; // LRU列表
unordered_set<frame_id_t> fixed_pages; // 固定页面集合

LRUReplacer::LRUReplacer(size_t num_pages){}

LRUReplacer::~LRUReplacer() = default;

/**
 * TODO: Student Implement
 */
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  if (lru_list.empty()) {
        return false;
    }
    
    // 从LRU列表头部取出最近最少使用的页帧
    *frame_id = lru_list.front();
    lru_list.erase(lru_list.begin());
    
    return true;
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Pin(frame_id_t frame_id) {
  // 如果页帧已经在固定集合中,直接返回
    if (fixed_pages.count(frame_id) > 0) {
        return;
    }
    
    // 从LRU列表中移除页帧
    auto iter = find(lru_list.begin(), lru_list.end(), frame_id);
    if (iter != lru_list.end()) {
        lru_list.erase(iter);
    }
    
    // 加入固定集合
    fixed_pages.insert(frame_id);
}

/**
 * TODO: Student Implement
 */
void LRUReplacer::Unpin(frame_id_t frame_id) {
  // 如果页帧不在固定集合中,直接返回
    if (fixed_pages.count(frame_id) == 0) {
        return;
    }
    
    // 从固定集合中移除页帧
    fixed_pages.erase(frame_id);
    
    // 加入LRU列表尾部
    lru_list.push_back(frame_id);
}

/**
 * TODO: Student Implement
 */
size_t LRUReplacer::Size() {
  return lru_list.size();
}