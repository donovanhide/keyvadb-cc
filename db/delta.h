#pragma once

#include <cstdint>
#include <cstddef>
#include <set>

namespace keyvadb {

template <std::uint32_t BITS>
class Delta {
  using node_type = Node<BITS>;
  using node_ptr = std::shared_ptr<node_type>;
  using snapshot_ptr = std::unique_ptr<Snapshot<BITS>>;
  using key_type = Key<BITS>;

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
  constexpr std::uint64_t Insertions() const { return insertions_; }

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
    std::set<KeyValue<BITS>> C(current_->NonZeroBegin(), current_->cend());
    existing_ = C.size();
    C.insert(snapshot->Lower(current_->First()),
             snapshot->Upper(current_->Last()));
    auto combined = C.size();
    if (existing_ == combined) {
      // All dupes nothing to do
      return;
    }
    Flip();
    if (combined <= N) {
      // Won't overflow copy right
      std::copy_backward(std::cbegin(C), std::cend(C), current_->end());
      insertions_ = C.size() - existing_;
      return;
    }
    // Handle overflowing nodec
    current_->Clear();
    auto stride = current_->Stride();
    std::size_t index = 0;
    auto best = Max<BITS>();
    for (auto const& kv : C) {
      std::uint32_t nearest;
      key_type distance;
      NearestStride(current_->First(), stride, kv.key, distance, nearest);
      if ((nearest == index && distance < best) || (nearest != index)) {
        current_->SetKeyValue(nearest, kv);
        best = distance;
      }
      index = nearest;
    }
    synthetics_ = current_->AddSyntheticKeyValues();
    for (auto const& kv : *current_)
      if (!kv.IsSynthetic()) {
        C.erase(kv);
        // insertions_++;
      }
    for (auto const& kv : C)
      if (snapshot->Add(kv.key, kv.value)) evictions_++;
    insertions_ = existing_ - evictions_ - synthetics_;
    return;
  }

  friend std::ostream& operator<<(std::ostream& stream, const Delta& delta) {
    stream << "Id: " << std::setw(3) << delta.current_->Id()
           << " Existing: " << std::setw(3) << delta.existing_
           << " Insertions: " << std::setw(3) << delta.insertions_
           << " Evictions: " << std::setw(3) << delta.evictions_
           << " Synthetics: " << std::setw(3) << delta.synthetics_
           << " Children: " << std::setw(3) << delta.children_;
    return stream;
  }
};
}  // namespace keyvadb
