#include "record/column.h"

#include "glog/logging.h"

Column::Column(std::string column_name, TypeId type, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)), type_(type), table_ind_(index), nullable_(nullable), unique_(unique) {
  ASSERT(type != TypeId::kTypeChar, "Wrong constructor for CHAR type.");
  switch (type) {
    case TypeId::kTypeInt:
      len_ = sizeof(int32_t);
      break;
    case TypeId::kTypeFloat:
      len_ = sizeof(float_t);
      break;
    default:
      ASSERT(false, "Unsupported column type.");
  }
}

Column::Column(std::string column_name, TypeId type, uint32_t length, uint32_t index, bool nullable, bool unique)
    : name_(std::move(column_name)),
      type_(type),
      len_(length),
      table_ind_(index),
      nullable_(nullable),
      unique_(unique) {
  ASSERT(type == TypeId::kTypeChar, "Wrong constructor for non-VARCHAR type.");
}

Column::Column(const Column *other)
    : name_(other->name_),
      type_(other->type_),
      len_(other->len_),
      table_ind_(other->table_ind_),
      nullable_(other->nullable_),
      unique_(other->unique_) {}

/**
* TODO: Student Implement
*/
uint32_t Column::SerializeTo(char *buf) const {
  uint32_t offset = 0;

  // 写入魔数
  *(uint32_t *)(buf + offset) = COLUMN_MAGIC_NUM;
  offset += sizeof(uint32_t);

  // 写入列名长度
  uint32_t name_len = static_cast<uint32_t>(name_.length());
  *(uint32_t *)(buf + offset) = name_len;
  offset += sizeof(uint32_t);

  // 写入列名
  memcpy(buf + offset, name_.c_str(), name_len);
  offset += name_len;

  // 写入其他成员变量
  *(uint32_t *)(buf + offset) = static_cast<uint32_t>(type_);
  offset += sizeof(uint32_t);
  *(uint32_t *)(buf + offset) = len_;
  offset += sizeof(uint32_t);
  *(uint32_t *)(buf + offset) = table_ind_;
  offset += sizeof(uint32_t);
  *(uint32_t *)(buf + offset) = static_cast<uint32_t>(nullable_);
  offset += sizeof(uint32_t);
  *(uint32_t *)(buf + offset) = static_cast<uint32_t>(unique_);
  offset += sizeof(uint32_t);

  return offset;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::GetSerializedSize() const {
  uint32_t size = sizeof(uint32_t); // 魔数
  size += sizeof(uint32_t); // 列名长度
  size += name_.length(); // 列名
  size += sizeof(uint32_t) * 5; // 其他成员变量

  return size;
}

/**
 * TODO: Student Implement
 */
uint32_t Column::DeserializeFrom(char *buf, Column *&column) {
  uint32_t offset = 0;

  // 检查魔数
  uint32_t magic_num = *(uint32_t *)(buf + offset);
  offset += sizeof(uint32_t);
  if (magic_num != COLUMN_MAGIC_NUM) {
    return 0; // 魔数错误
  }

  // 读取列名长度
  uint32_t name_len = *(uint32_t *)(buf + offset);
  offset += sizeof(uint32_t);

  // 读取列名
  std::string name(buf + offset, name_len);
  offset += name_len;

  // 读取其他成员变量
  uint32_t type = *(uint32_t *)(buf + offset);
  offset += sizeof(uint32_t);
  uint32_t len = *(uint32_t *)(buf + offset);
  offset += sizeof(uint32_t);
  uint32_t table_ind = *(uint32_t *)(buf + offset);
  offset += sizeof(uint32_t);
  uint32_t nullable = *(uint32_t *)(buf + offset);
  offset += sizeof(uint32_t);
  uint32_t unique = *(uint32_t *)(buf + offset);
  offset += sizeof(uint32_t);

  column = new Column(name, static_cast<TypeId>(type), len, table_ind, nullable != 0, unique != 0);

  return offset;
}
