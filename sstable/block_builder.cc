#include <sstable/block_builder.h>

namespace {

// Convert from string to uint8
void ConvertSizeTToBytes(size_t size) {
  const Byte* byte_buf = reinterpret_cast<Byte*>(sv.data());


}

} // namespace

namespace kvs {

BlockBuilder::BlockBuilder() : is_finished_(false) {}

void BlockBuilder::Add(
    std::string_view key, std::string_view value, TxnId txn_id) {
  if (!is_finished_) {
    return;
  }

  assert(key.size() <= kMaxKeySize);
  const Byte* key_buf = reinterpret_cast<Byte*>(key.data());
  // const size_t key_len = key.size();
  const DoubleByte key_len = static_cast<DoubleByte>(key.size());


  buffer_.insert();
  buffer_.insert(buffer_.end(), key_len);


}

} // namespace kvs