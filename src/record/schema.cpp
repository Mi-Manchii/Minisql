#include "record/schema.h"

/**
 * TODO: Student Implement
 */
uint32_t Schema::SerializeTo(char *buf) const {
  uint32_t offset = 0;

  // 写入魔数
  *(uint32_t *)(buf + offset) = SCHEMA_MAGIC_NUM;
  offset += sizeof(uint32_t);

  // 写入列数量
  uint32_t col_count = static_cast<uint32_t>(columns_.size());
  *(uint32_t *)(buf + offset) = col_count;
  offset += sizeof(uint32_t);

  // 写入每个列的序列化数据
  for (const auto &col : columns_) {
    uint32_t col_size = col->GetSerializedSize();
    char *col_buf = buf + offset;
    offset += col->SerializeTo(col_buf);
  }

  return offset;
}

uint32_t Schema::GetSerializedSize() const {
  uint32_t size = sizeof(uint32_t); // 魔数
  size += sizeof(uint32_t); // 列数量

  // 累加每个列的序列化大小
  for (const auto &col : columns_) {
    size += col->GetSerializedSize();
  }

  return size;
}

uint32_t Schema::DeserializeFrom(char *buf, Schema *&schema) {
  uint32_t offset = 0;

  // 检查魔数
  uint32_t magic_num = *(uint32_t *)(buf + offset);
  offset += sizeof(uint32_t);
  if (magic_num != SCHEMA_MAGIC_NUM) {
    return 0; // 魔数错误
  }

  // 读取列数量
  uint32_t col_count = *(uint32_t *)(buf + offset);
  offset += sizeof(uint32_t);

  // 创建列向量
  std::vector<Column *> cols;
  cols.reserve(col_count);

  // 反序列化每个列
  for (uint32_t i = 0; i < col_count; i++) {
    Column *col;
    uint32_t col_size = Column::DeserializeFrom(buf + offset, col);
    if (col_size == 0) {
      // 反序列化失败
      for (const auto &c : cols) {
        delete c;
      }
      return 0;
    }
    offset += col_size;
    cols.push_back(col);
  }

  schema = new Schema(cols, true);
  return offset;
}