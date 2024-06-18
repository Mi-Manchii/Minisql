#include "record/row.h"

/**
 * TODO: Student Implement
 */
uint32_t Row::SerializeTo(char *buf, Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  uint32_t cur = 0;
  uint32_t field_num = fields_.size();
  memcpy(buf+cur,&field_num,sizeof(uint32_t));cur+=sizeof(uint32_t);

  uint32_t bitmap_len = (field_num +7)/8;//取上整个字节
  char* bitmap = buf+cur;
  memset(bitmap,0,bitmap_len);//初始化为0
  cur+=bitmap_len;
  for(uint32_t i=0;i<field_num;i++){
    Field *field = fields_[i];
    if(field->IsNull()){
      bitmap[i/8] |= (1<<(i%8));
    }else{
      cur+=field->SerializeTo(buf+cur);
    }
  }

  return cur;
}

uint32_t Row::DeserializeFrom(char *buf, Schema *schema) {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(fields_.empty(), "Non empty field in row.");
  // 从buf中读取字段数量和空值位图
  uint32_t field_count = *reinterpret_cast<uint32_t *>(buf);
  buf += sizeof(uint32_t);
  uint32_t null_bitmap = *reinterpret_cast<uint32_t *>(buf);
  buf += sizeof(uint32_t);

  // 检查字段数量是否与schema一致
  if (field_count != schema->GetColumnCount()) {
    return 0;
  }

  // 反序列化每个字段
  for (uint32_t i = 0; i < field_count; i++) {
    bool is_null = (null_bitmap & (1 << i)) != 0;
    TypeId type = schema->GetColumn(i).GetType();
    uint32_t len = schema->GetColumn(i).GetLength();

    Field *field;
    if (is_null) {
      field = new Field(type, len, true, nullptr);
    } else {
      field = new Field(type, len, false, buf);
      buf += field->GetSerializedSize();
    }

    fields_.push_back(field);
  }

  // 返回反序列化所需的总字节数
  return sizeof(uint32_t) * 2 + GetSerializedSize(schema);
}

uint32_t Row::GetSerializedSize(Schema *schema) const {
  ASSERT(schema != nullptr, "Invalid schema before serialize.");
  ASSERT(schema->GetColumnCount() == fields_.size(), "Fields size do not match schema's column size.");
  // 初始化序列化大小为字段数量和空值位图的大小
  uint32_t serialized_size = sizeof(uint32_t) * 2;

  // 计算每个字段的序列化大小
  for (uint32_t i = 0; i < fields_.size(); i++) {
    Field *field = fields_[i];
    TypeId type = schema->GetColumn(i).GetType();
    uint32_t len = schema->GetColumn(i).GetLength();

    // 如果字段为空,序列化大小为0
    if (field->IsNull()) {
      continue;
    }

    // 根据字段类型计算序列化大小
    switch (type) {
      case TypeId::BOOLEAN:
      case TypeId::TINYINT:
        serialized_size += sizeof(int8_t);
        break;
      case TypeId::SMALLINT:
        serialized_size += sizeof(int16_t);
        break;
      case TypeId::INTEGER:
      case TypeId::BIGINT:
      case TypeId::FLOAT:
        serialized_size += sizeof(int32_t);
        break;
      case TypeId::DOUBLE:
        serialized_size += sizeof(int64_t);
        break;
      case TypeId::CHAR:
      case TypeId::VARCHAR:
        serialized_size += len + sizeof(uint32_t); // 字符串类型需额外存储长度
        break;
      default:
        ASSERT(false, "Invalid type id");
    }
  }

  return serialized_size;
}

void Row::GetKeyFromRow(const Schema *schema, const Schema *key_schema, Row &key_row) {
  auto columns = key_schema->GetColumns();
  std::vector<Field> fields;
  uint32_t idx;
  for (auto column : columns) {
    schema->GetColumnIndex(column->GetName(), idx);
    fields.emplace_back(*this->GetField(idx));
  }
  key_row = Row(fields);
}
