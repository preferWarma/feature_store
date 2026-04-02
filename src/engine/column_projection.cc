#include "column_projection.h"

#include <cstddef>
#include <vector>

#include <arrow/status.h>
#include <arrow/type.h>

namespace feature_store {

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ProjectColumns(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::vector<std::string>& columns) {
    if (!batch) {
        return arrow::Status::Invalid("batch is null");
    }
    if (columns.empty()) {
        return batch;
    }

    std::vector<int> indices;
    indices.reserve(columns.size());
    for (const auto& col : columns) {
        const int idx = batch->schema()->GetFieldIndex(col);
        if (idx == -1) {
            return arrow::Status::KeyError("Column not found: " + col);
        }
        indices.push_back(idx);
    }
    return batch->SelectColumns(indices);
}

}  // namespace feature_store
