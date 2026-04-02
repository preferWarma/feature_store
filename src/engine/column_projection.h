#pragma once

#include <memory>
#include <string>
#include <vector>

#include <arrow/record_batch.h>
#include <arrow/result.h>

namespace feature_store {

arrow::Result<std::shared_ptr<arrow::RecordBatch>> ProjectColumns(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    const std::vector<std::string>& columns);

}  // namespace feature_store
