#include "page/bitmap_page.h"

#include "glog/logging.h"

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::AllocatePage(uint32_t &page_offset) {
  // 遍历位图,查找第一个未分配的页
  for (uint32_t i = 0; i < PageSize; i++) {
    uint8_t byte = data_[i];
    if (byte != 0xFF) { // 该字节还有未分配的位
      for (uint8_t j = 0; j < 8; j++) {
        if (!(byte & (1 << j))) { // 该位未分配
          page_offset = i * 8 + j; // 计算页偏移量
          data_[i] |= (1 << j); // 标记该位已分配
          return true;
        }
      }
    }
  }
  // 位图已满,无法分配新页
  return false;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::DeAllocatePage(uint32_t page_offset) {
  // 检查页偏移是否越界
  if (page_offset >= PageSize * 8) {
    return false;
  }

  // 计算该页所在的字节和位偏移
  uint32_t byte_index = page_offset / 8;
  uint32_t bit_index = page_offset % 8;

  // 检查该位是否已经被分配
  uint8_t byte = data_[byte_index];
  if (!(byte & (1 << bit_index))) {
    // 该位未分配,无需释放
    return false;
  }

  // 释放该位
  data_[byte_index] &= ~(1 << bit_index);
  return true;
}

/**
 * TODO: Student Implement
 */
template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFree(uint32_t page_offset) const {
  // 检查页偏移是否越界
  if (page_offset >= PageSize * 8) {
    return false;
  }

  // 计算该页所在的字节和位偏移
  uint32_t byte_index = page_offset / 8;
  uint8_t bit_index = page_offset % 8;

  return IsPageFreeLow(byte_index, bit_index);
}

template <size_t PageSize>
bool BitmapPage<PageSize>::IsPageFreeLow(uint32_t byte_index, uint8_t bit_index) const {
  // 获取该字节的值
  uint8_t byte = data_[byte_index];

  // 检查该位是否为0
  return !(byte & (1 << bit_index));
}

template class BitmapPage<64>;

template class BitmapPage<128>;

template class BitmapPage<256>;

template class BitmapPage<512>;

template class BitmapPage<1024>;

template class BitmapPage<2048>;

template class BitmapPage<4096>;