#include "../engine/arrow_merge_operator.h"

#include <filesystem>
#include <memory>
#include <string>
#include <vector>

#include <arrow/api.h>
#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/options.h>

#include "../engine/frame_codec.h"
#include "../engine/key_encoder.h"
#include "../engine/schema_registry.h"

namespace feature_store {
namespace {

std::shared_ptr<arrow::RecordBatch> MakeBatch(int64_t v) {
    arrow::Int64Builder builder;
    EXPECT_TRUE(builder.Append(v).ok());
    std::shared_ptr<arrow::Int64Array> arr;
    EXPECT_TRUE(builder.Finish(&arr).ok());
    auto schema = arrow::schema({arrow::field("x", arrow::int64())});
    return arrow::RecordBatch::Make(schema, 1, {arr});
}

std::vector<std::string> SplitFrames(const std::string& value) {
    std::vector<std::string> frames;
    auto bytes = std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(value.data()), value.size());
    std::size_t offset = 0;
    while (offset < bytes.size()) {
        auto header_res = DecodeFrameHeader(bytes.subspan(offset));
        if (!header_res.ok()) {
            break;
        }
        const std::size_t frame_size = header_res->total_size();
        if (frame_size > bytes.size() - offset) {
            break;
        }
        frames.emplace_back(value.substr(offset, frame_size));
        offset += frame_size;
    }
    return frames;
}

TEST(ArrowMergeOperatorTest, FullMergeSingleOperand) {
    SchemaRegistry registry;
    ArrowMergeOperator merge_op(&registry);

    auto batch = MakeBatch(1);
    auto frame_res = EncodeFrame(1, 100, *batch);
    ASSERT_TRUE(frame_res.ok()) << frame_res.status().ToString();
    std::string operand = frame_res.ValueOrDie();

    const rocksdb::Slice key("k");
    const std::vector<rocksdb::Slice> operands{rocksdb::Slice(operand)};
    rocksdb::MergeOperator::MergeOperationInput in(key, nullptr, operands, nullptr);
    std::string out;
    rocksdb::Slice existing_operand;
    rocksdb::MergeOperator::MergeOperationOutput out_wrap(out, existing_operand);

    ASSERT_TRUE(merge_op.FullMergeV2(in, &out_wrap));
    EXPECT_EQ(out, operand);
}

TEST(ArrowMergeOperatorTest, FullMergeMultipleOperands) {
    SchemaRegistry registry;
    ArrowMergeOperator merge_op(&registry);

    auto f1 = EncodeFrame(1, 100, *MakeBatch(1));
    auto f2 = EncodeFrame(1, 200, *MakeBatch(2));
    ASSERT_TRUE(f1.ok());
    ASSERT_TRUE(f2.ok());

    std::string op1 = f1.ValueOrDie();
    std::string op2 = f2.ValueOrDie();

    const rocksdb::Slice key("k");
    const std::vector<rocksdb::Slice> operands{rocksdb::Slice(op1), rocksdb::Slice(op2)};
    rocksdb::MergeOperator::MergeOperationInput in(key, nullptr, operands, nullptr);
    std::string out;
    rocksdb::Slice existing_operand;
    rocksdb::MergeOperator::MergeOperationOutput out_wrap(out, existing_operand);

    ASSERT_TRUE(merge_op.FullMergeV2(in, &out_wrap));
    EXPECT_EQ(out, op1 + op2);
}

TEST(ArrowMergeOperatorTest, FullMergeSkipsBadCRCFrame) {
    SchemaRegistry registry;
    ArrowMergeOperator merge_op(&registry);

    auto f1 = EncodeFrame(1, 100, *MakeBatch(1));
    auto f2 = EncodeFrame(1, 200, *MakeBatch(2));
    ASSERT_TRUE(f1.ok());
    ASSERT_TRUE(f2.ok());

    std::string good = f1.ValueOrDie();
    std::string bad = f2.ValueOrDie();
    bad[bad.size() - 1] ^= 0x01;

    const rocksdb::Slice key("k");
    const std::vector<rocksdb::Slice> operands{rocksdb::Slice(good), rocksdb::Slice(bad)};
    rocksdb::MergeOperator::MergeOperationInput in(key, nullptr, operands, nullptr);
    std::string out;
    rocksdb::Slice existing_operand;
    rocksdb::MergeOperator::MergeOperationOutput out_wrap(out, existing_operand);

    ASSERT_TRUE(merge_op.FullMergeV2(in, &out_wrap));
    EXPECT_EQ(out, good);
    EXPECT_TRUE(ValidateFrameCRC(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(out.data()), out.size())));
}

TEST(ArrowMergeOperatorTest, PartialMergeFastConcat) {
    SchemaRegistry registry;
    ArrowMergeOperator merge_op(&registry);

    auto f1 = EncodeFrame(2, 100, *MakeBatch(1));
    auto f2 = EncodeFrame(2, 200, *MakeBatch(2));
    auto f3 = EncodeFrame(2, 300, *MakeBatch(3));
    ASSERT_TRUE(f1.ok());
    ASSERT_TRUE(f2.ok());
    ASSERT_TRUE(f3.ok());

    std::string left = f1.ValueOrDie() + f2.ValueOrDie();
    std::string right = f3.ValueOrDie();
    std::string out;

    ASSERT_TRUE(merge_op.PartialMerge(rocksdb::Slice("k"),
                                      rocksdb::Slice(left),
                                      rocksdb::Slice(right),
                                      &out,
                                      nullptr));
    EXPECT_EQ(out, left + right);
}

TEST(ArrowMergeOperatorTest, PartialMergeSkipsBadFrameAndFallsBack) {
    SchemaRegistry registry;
    ArrowMergeOperator merge_op(&registry);

    auto f1 = EncodeFrame(2, 100, *MakeBatch(1));
    auto f2 = EncodeFrame(2, 200, *MakeBatch(2));
    auto f3 = EncodeFrame(2, 300, *MakeBatch(3));
    ASSERT_TRUE(f1.ok());
    ASSERT_TRUE(f2.ok());
    ASSERT_TRUE(f3.ok());

    std::string left = f1.ValueOrDie() + f2.ValueOrDie();
    std::string right = f3.ValueOrDie();
    right[10] ^= 0x01;

    std::string out;
    ASSERT_TRUE(merge_op.PartialMerge(rocksdb::Slice("k"),
                                      rocksdb::Slice(left),
                                      rocksdb::Slice(right),
                                      &out,
                                      nullptr));
    EXPECT_EQ(out, left);
}

TEST(ArrowMergeOperatorTest, RocksDBEndToEndMergeAndGet) {
    SchemaRegistry registry;
    auto merge_op = std::make_shared<ArrowMergeOperator>(&registry);

    auto path = (std::filesystem::temp_directory_path() /
                 std::filesystem::path("feature_store_merge_operator_testdb"))
                    .string();
    std::error_code ec;
    std::filesystem::remove_all(path, ec);

    rocksdb::Options options;
    options.create_if_missing = true;
    options.merge_operator = merge_op;

    rocksdb::DB* raw_db = nullptr;
    auto s = rocksdb::DB::Open(options, path, &raw_db);
    ASSERT_TRUE(s.ok()) << s.ToString();
    std::unique_ptr<rocksdb::DB> db(raw_db);

    auto f1 = EncodeFrame(1, 100, *MakeBatch(1));
    auto f2 = EncodeFrame(1, 200, *MakeBatch(2));
    ASSERT_TRUE(f1.ok());
    ASSERT_TRUE(f2.ok());

    const std::string key = EncodeKey(123);
    s = db->Merge(rocksdb::WriteOptions(), key, f1.ValueOrDie());
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = db->Merge(rocksdb::WriteOptions(), key, f2.ValueOrDie());
    ASSERT_TRUE(s.ok()) << s.ToString();

    std::string value;
    s = db->Get(rocksdb::ReadOptions(), key, &value);
    ASSERT_TRUE(s.ok()) << s.ToString();

    auto frames = SplitFrames(value);
    ASSERT_EQ(frames.size(), 2U);
    EXPECT_TRUE(ValidateFrameCRC(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(frames[0].data()), frames[0].size())));
    EXPECT_TRUE(ValidateFrameCRC(std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(frames[1].data()), frames[1].size())));

    {
        auto decoded = DecodeFrame(std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(frames[0].data()), frames[0].size()));
        ASSERT_TRUE(decoded.ok());
        EXPECT_EQ(decoded->timestamp, 100);
        EXPECT_EQ(decoded->schema_version, 1);
        EXPECT_EQ(decoded->record_batch->num_rows(), 1);
    }
    {
        auto decoded = DecodeFrame(std::span<const uint8_t>(
            reinterpret_cast<const uint8_t*>(frames[1].data()), frames[1].size()));
        ASSERT_TRUE(decoded.ok());
        EXPECT_EQ(decoded->timestamp, 200);
        EXPECT_EQ(decoded->schema_version, 1);
        EXPECT_EQ(decoded->record_batch->num_rows(), 1);
    }

    db.reset();
    std::filesystem::remove_all(path, ec);
}

}  // namespace
}  // namespace feature_store
