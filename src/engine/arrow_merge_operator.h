#pragma once

#include <rocksdb/merge_operator.h>

namespace feature_store {

class SchemaRegistry;

class ArrowMergeOperator : public rocksdb::MergeOperator {
public:
    explicit ArrowMergeOperator(SchemaRegistry* registry) : registry_(registry) {}

    bool FullMergeV2(const MergeOperationInput& merge_in,
                     MergeOperationOutput* merge_out) const override;

    bool PartialMerge(const rocksdb::Slice& key,
                      const rocksdb::Slice& left,
                      const rocksdb::Slice& right,
                      std::string* new_value,
                      rocksdb::Logger* logger) const override;

    const char* Name() const override { return "ArrowMergeOperator"; }

private:
    SchemaRegistry* registry_;
};

}  // namespace feature_store
