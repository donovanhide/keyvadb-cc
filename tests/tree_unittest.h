#include "tests/common.h"
#include "db/memory.h"
#include "db/tree.h"

using namespace keyvadb;

TYPED_TEST(StoreTest, TreeOperations)
{
    auto first = this->FromHex('0');
    auto last = this->FromHex('F');

    auto tree = this->GetTree();
    ASSERT_FALSE(tree->Init(false));
    // Check root has been created
    this->checkTree(tree);
    ASSERT_NE(0UL, this->keys_->Size());
    // Insert some random values
    // twice with same seed to insert duplicates
    const std::size_t n = 10;
    const std::size_t rounds = 4;
    for (std::size_t i = 0; i < 2; i++)
    {
        for (std::size_t j = 0; j < rounds; j++)
        {
            // Use j as seed
            auto input = this->RandomKeyValues(n, j);
            for (auto const& kv : input) this->buffer_.Add(kv.first, kv.second);
            ASSERT_EQ(n, this->buffer_.Size());
            auto journal = this->GetJournal();
            ASSERT_FALSE(journal->Process(*tree));
            this->checkTree(tree);
            std::cout << this->buffer_;
            ASSERT_FALSE(journal->Commit(*tree));
            this->checkTree(tree);
            std::cout << this->buffer_;
            // if (i == 0)
            // {
            //     ASSERT_GT(journal->Size(), 0UL);
            //     ASSERT_EQ(n, journal->TotalInsertions());
            //     this->checkCount(tree, n * (j + 1));
            // }
            // else
            // {
            //     ASSERT_EQ(journal->Size(), 0UL);
            //     ASSERT_EQ(0UL, journal->TotalInsertions());
            // }
            for (auto const& kv : this->buffer_.GetRange(first, last))
                this->checkValue(tree, kv);
            // this->checkTree(tree);
            // std::cout << *journal << "----" << std::endl;
        }
    }
    std::cout << this->cache_;
}
