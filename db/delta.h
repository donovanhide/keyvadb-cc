#pragma once

#include <cstdint>
#include <cstddef>
#include <set>
#include <algorithm>

namespace keyvadb {

template <std::uint32_t BITS>
class Delta {
  using util = detail::KeyUtil<BITS>;
  using key_type = typename util::key_type;
  using node_type = Node<BITS>;
  using node_ptr = std::shared_ptr<node_type>;
  using snapshot_ptr = std::unique_ptr<Snapshot<BITS>>;

 private:
  std::uint64_t existing_;
  std::uint64_t insertions_;
  std::uint64_t evictions_;
  std::uint64_t synthetics_;
  std::uint64_t children_;
  node_ptr current_;
  node_ptr previous_;

 public:
  explicit Delta(node_ptr const& node)
      : existing_(0),
        insertions_(0),
        evictions_(0),
        synthetics_(0),
        children_(0),
        current_(node) {}

  void Flip() {
    // Copy on write
    if (!Dirty()) {
      previous_ = current_;
      current_ = std::make_shared<node_type>(*previous_);
    }
  }

  constexpr bool Dirty() const { return previous_ ? true : false; }
  constexpr node_ptr Current() const { return current_; }
  constexpr std::uint64_t Insertions() const {
    return insertions_ - evictions_;
  }

  void CheckSanity() {
    if (!current_->IsSane()) {
      std::cout << *current_ << std::endl;
      throw std::domain_error("Insane node");
    }
  }

  void SetChild(std::size_t const i, std::uint64_t const cid) {
    Flip();
    children_++;
    current_->SetChild(i, cid);
  }

  void AddKeys(snapshot_ptr const& snapshot) {
    // Full node, nothing to do
    if (current_->EmptyKeyCount() == 0) return;
    auto N = current_->MaxKeys();
    std::set<KeyValue<BITS>> candidates(snapshot->Lower(current_->First()),
                                        snapshot->Upper(current_->Last()));
    std::set<KeyValue<BITS>> existing(current_->NonZeroBegin(),
                                      current_->keys.cend());
    existing_ = existing.size();
    std::set<KeyValue<BITS>> combined(candidates);
    std::copy(existing.cbegin(), existing.cend(),
              std::inserter(combined, combined.end()));
    if (existing_ == combined.size()) {
      // All dupes nothing to do
      return;
    }
    Flip();
    if (combined.size() <= N) {
      // Won't overflow copy right
      std::copy_backward(combined.cbegin(), combined.cend(),
                         current_->keys.end());
      insertions_ = combined.size() - existing_;
      return;
    }
    // Handle overflowing node
    current_->Clear();
    auto stride = current_->Stride();
    std::size_t index = 0;
    auto best = util::Max();
    for (auto const& kv : combined) {
      std::uint32_t nearest;
      key_type distance;
      util::NearestStride(current_->First(), stride, kv.key, distance, nearest);
      if ((nearest == index && distance < best) || (nearest != index)) {
        current_->SetKeyValue(nearest, kv);
        best = distance;
      }
      index = nearest;
    }
    synthetics_ = current_->AddSyntheticKeyValues();
    for (auto const& kv : current_->keys) {
      if (kv.IsSynthetic()) continue;
      if (candidates.count(kv) > 0) {
        insertions_++;
      }
      existing.erase(kv);
    }
    for (auto const& kv : existing) {
      evictions_++;
      snapshot->Add(kv.key, kv.value);
    }
    return;
  }

  friend std::ostream& operator<<(std::ostream& stream, const Delta& delta) {
    stream << "Id: " << std::setw(12) << delta.current_->Id()
           << " Existing: " << std::setw(3) << delta.existing_
           << " Insertions: " << std::setw(3) << delta.insertions_
           << " Evictions: " << std::setw(3) << delta.evictions_
           << " Synthetics: " << std::setw(3) << delta.synthetics_
           << " Children: " << std::setw(3) << delta.children_;
    return stream;
  }
};
}  // namespace keyvadb
