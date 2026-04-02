#include "../engine/frame_codec.h"

#include <cstdint>
#include <limits>
#include <span>
#include <string>

#include <arrow/api.h>
#include <arrow/status.h>
#include <gtest/gtest.h>

namespace feature_store {
namespace {

std::span<const uint8_t> AsBytes(const std::string& value) {
    return std::span<const uint8_t>(
        reinterpret_cast<const uint8_t*>(value.data()), value.size());
}

void WriteUInt32LittleEndian(uint32_t value, std::string* target, std::size_t offset) {
    for (std::size_t i = 0; i < sizeof(uint32_t); ++i) {
        (*target)[offset + i] = static_cast<char>((value >> (i * 8U)) & 0xFFU);
    }
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeSampleBatch() {
    arrow::Int64Builder ids_builder;
    arrow::Int64Builder scores_builder;

    auto status = ids_builder.Append(1);
    if (!status.ok()) {
        return status;
    }
    status = ids_builder.Append(2);
    if (!status.ok()) {
        return status;
    }
    status = ids_builder.Append(3);
    if (!status.ok()) {
        return status;
    }
    status = scores_builder.Append(10);
    if (!status.ok()) {
        return status;
    }
    status = scores_builder.Append(20);
    if (!status.ok()) {
        return status;
    }
    status = scores_builder.Append(30);
    if (!status.ok()) {
        return status;
    }

    std::shared_ptr<arrow::Int64Array> ids;
    std::shared_ptr<arrow::Int64Array> scores;
    status = ids_builder.Finish(&ids);
    if (!status.ok()) {
        return status;
    }
    status = scores_builder.Finish(&scores);
    if (!status.ok()) {
        return status;
    }

    auto schema = arrow::schema({
        arrow::field("id", arrow::int64()),
        arrow::field("score", arrow::int64()),
    });
    return arrow::RecordBatch::Make(schema, 3, {ids, scores});
}

arrow::Result<std::shared_ptr<arrow::RecordBatch>> MakeEmptyBatch() {
    arrow::Int64Builder builder;
    std::shared_ptr<arrow::Int64Array> ids;
    auto status = builder.Finish(&ids);
    if (!status.ok()) {
        return status;
    }

    auto schema = arrow::schema({arrow::field("id", arrow::int64())});
    return arrow::RecordBatch::Make(schema, 0, {ids});
}

TEST(FrameCodecTest, RoundTripEncodeDecode) {
    auto batch_result = MakeSampleBatch();
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    auto record_batch = batch_result.ValueOrDie();
    auto encoded_result = EncodeFrame(7, 1712212123123LL, *record_batch);
    ASSERT_TRUE(encoded_result.ok()) << encoded_result.status().ToString();

    const std::string& encoded = encoded_result.ValueOrDie();
    EXPECT_TRUE(ValidateFrameCRC(AsBytes(encoded)));

    auto header_result = DecodeFrameHeader(AsBytes(encoded));
    ASSERT_TRUE(header_result.ok()) << header_result.status().ToString();
    EXPECT_EQ(header_result->magic, kFrameMagic);
    EXPECT_EQ(header_result->schema_version, 7);
    EXPECT_EQ(header_result->reserved, 0);
    EXPECT_EQ(header_result->timestamp, 1712212123123LL);
    EXPECT_EQ(header_result->total_size(), encoded.size());

    auto decoded_result = DecodeFrame(AsBytes(encoded));
    ASSERT_TRUE(decoded_result.ok()) << decoded_result.status().ToString();
    EXPECT_EQ(decoded_result->schema_version, 7);
    EXPECT_EQ(decoded_result->timestamp, 1712212123123LL);
    ASSERT_NE(decoded_result->record_batch, nullptr);
    EXPECT_TRUE(decoded_result->record_batch->Equals(*record_batch));
}

TEST(FrameCodecTest, DecodeRejectsInvalidMagic) {
    auto batch_result = MakeSampleBatch();
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    auto record_batch = batch_result.ValueOrDie();
    auto encoded_result = EncodeFrame(3, 1000, *record_batch);
    ASSERT_TRUE(encoded_result.ok()) << encoded_result.status().ToString();

    std::string corrupted = encoded_result.ValueOrDie();
    corrupted[0] = static_cast<char>(0x00);

    auto header_result = DecodeFrameHeader(AsBytes(corrupted));
    ASSERT_FALSE(header_result.ok());
    EXPECT_EQ(header_result.status().code(), arrow::StatusCode::Invalid);

    auto frame_result = DecodeFrame(AsBytes(corrupted));
    ASSERT_FALSE(frame_result.ok());
    EXPECT_EQ(frame_result.status().code(), arrow::StatusCode::Invalid);
}

TEST(FrameCodecTest, DecodeRejectsTamperedCRC) {
    auto batch_result = MakeSampleBatch();
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    auto record_batch = batch_result.ValueOrDie();
    auto encoded_result = EncodeFrame(5, 2000, *record_batch);
    ASSERT_TRUE(encoded_result.ok()) << encoded_result.status().ToString();

    std::string corrupted = encoded_result.ValueOrDie();
    corrupted[25] ^= 0x01;

    EXPECT_FALSE(ValidateFrameCRC(AsBytes(corrupted)));

    auto frame_result = DecodeFrame(AsBytes(corrupted));
    ASSERT_FALSE(frame_result.ok());
    EXPECT_EQ(frame_result.status().code(), arrow::StatusCode::IOError);
}

TEST(FrameCodecTest, DecodeRejectsIpcLengthOverflow) {
    auto batch_result = MakeSampleBatch();
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    auto record_batch = batch_result.ValueOrDie();
    auto encoded_result = EncodeFrame(9, 3000, *record_batch);
    ASSERT_TRUE(encoded_result.ok()) << encoded_result.status().ToString();

    std::string corrupted = encoded_result.ValueOrDie();
    WriteUInt32LittleEndian(std::numeric_limits<uint32_t>::max(), &corrupted, 16);

    auto header_result = DecodeFrameHeader(AsBytes(corrupted));
    ASSERT_FALSE(header_result.ok());
    EXPECT_EQ(header_result.status().code(), arrow::StatusCode::Invalid);

    auto frame_result = DecodeFrame(AsBytes(corrupted));
    ASSERT_FALSE(frame_result.ok());
    EXPECT_EQ(frame_result.status().code(), arrow::StatusCode::Invalid);
}

TEST(FrameCodecTest, RoundTripEmptyRecordBatch) {
    auto batch_result = MakeEmptyBatch();
    ASSERT_TRUE(batch_result.ok()) << batch_result.status().ToString();
    auto record_batch = batch_result.ValueOrDie();
    auto encoded_result = EncodeFrame(11, 4000, *record_batch);
    ASSERT_TRUE(encoded_result.ok()) << encoded_result.status().ToString();

    const std::string& encoded = encoded_result.ValueOrDie();
    EXPECT_TRUE(ValidateFrameCRC(AsBytes(encoded)));

    auto decoded_result = DecodeFrame(AsBytes(encoded));
    ASSERT_TRUE(decoded_result.ok()) << decoded_result.status().ToString();
    EXPECT_EQ(decoded_result->schema_version, 11);
    EXPECT_EQ(decoded_result->timestamp, 4000);
    ASSERT_NE(decoded_result->record_batch, nullptr);
    EXPECT_EQ(decoded_result->record_batch->num_rows(), 0);
    EXPECT_TRUE(decoded_result->record_batch->Equals(*record_batch));
}

}  // namespace
}  // namespace feature_store
