#include "../engine/column_projection.h"

#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>

namespace feature_store {
namespace {

std::shared_ptr<arrow::RecordBatch> MakeBatch() {
    arrow::Int64Builder a_builder;
    arrow::Int32Builder b_builder;
    EXPECT_TRUE(a_builder.Append(1).ok());
    EXPECT_TRUE(b_builder.Append(2).ok());

    std::shared_ptr<arrow::Int64Array> a;
    std::shared_ptr<arrow::Int32Array> b;
    EXPECT_TRUE(a_builder.Finish(&a).ok());
    EXPECT_TRUE(b_builder.Finish(&b).ok());

    auto schema = arrow::schema({
        arrow::field("a", arrow::int64()),
        arrow::field("b", arrow::int32()),
    });
    return arrow::RecordBatch::Make(schema, 1, {a, b});
}

TEST(ColumnProjectionTest, SelectSubsetColumns) {
    auto batch = MakeBatch();
    auto res = ProjectColumns(batch, {"b"});
    ASSERT_TRUE(res.ok()) << res.status().ToString();
    ASSERT_EQ((*res)->num_columns(), 1);
    EXPECT_EQ((*res)->schema()->field(0)->name(), "b");
}

TEST(ColumnProjectionTest, MissingColumnReturnsError) {
    auto batch = MakeBatch();
    auto res = ProjectColumns(batch, {"c"});
    ASSERT_FALSE(res.ok());
    EXPECT_EQ(res.status().code(), arrow::StatusCode::KeyError);
}

}  // namespace
}  // namespace feature_store
