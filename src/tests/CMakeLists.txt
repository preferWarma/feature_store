if(NOT BUILD_TESTING)
    return()
endif()

find_package(GTest REQUIRED)

add_executable(frame_codec_test
    ${PROJECT_SOURCE_DIR}/src/tests/frame_codec_test.cc
)

target_link_libraries(frame_codec_test
    PRIVATE
    feature_store_engine
    GTest::gtest_main
)

add_test(NAME frame_codec_test COMMAND frame_codec_test)

add_executable(key_encoder_test
    ${PROJECT_SOURCE_DIR}/src/tests/key_encoder_test.cc
)

target_link_libraries(key_encoder_test
    PRIVATE
    feature_store_engine
    GTest::gtest_main
)

add_test(NAME key_encoder_test COMMAND key_encoder_test)

add_executable(engine_config_test
    ${PROJECT_SOURCE_DIR}/src/tests/engine_config_test.cc
)

target_link_libraries(engine_config_test
    PRIVATE
    feature_store_engine
    GTest::gtest_main
)

add_test(NAME engine_config_test COMMAND engine_config_test)

add_executable(schema_registry_test
    ${PROJECT_SOURCE_DIR}/src/tests/schema_registry_test.cc
)

target_link_libraries(schema_registry_test
    PRIVATE
    feature_store_engine
    GTest::gtest_main
)

add_test(NAME schema_registry_test COMMAND schema_registry_test)

add_executable(arrow_merge_operator_test
    ${PROJECT_SOURCE_DIR}/src/tests/arrow_merge_operator_test.cc
)

target_link_libraries(arrow_merge_operator_test
    PRIVATE
    feature_store_engine
    GTest::gtest_main
)

add_test(NAME arrow_merge_operator_test COMMAND arrow_merge_operator_test)

add_executable(arrow_ttl_filter_test
    ${PROJECT_SOURCE_DIR}/src/tests/arrow_ttl_filter_test.cc
)

target_link_libraries(arrow_ttl_filter_test
    PRIVATE
    feature_store_engine
    GTest::gtest_main
)

add_test(NAME arrow_ttl_filter_test COMMAND arrow_ttl_filter_test)

add_executable(column_projection_test
    ${PROJECT_SOURCE_DIR}/src/tests/column_projection_test.cc
)

target_link_libraries(column_projection_test
    PRIVATE
    feature_store_engine
    GTest::gtest_main
)

add_test(NAME column_projection_test COMMAND column_projection_test)

add_executable(arrow_rocks_engine_read_test
    ${PROJECT_SOURCE_DIR}/src/tests/arrow_rocks_engine_read_test.cc
)

target_link_libraries(arrow_rocks_engine_read_test
    PRIVATE
    feature_store_engine
    GTest::gtest_main
)

add_test(NAME arrow_rocks_engine_read_test COMMAND arrow_rocks_engine_read_test)

add_executable(arrow_rocks_engine_lifecycle_test
    ${PROJECT_SOURCE_DIR}/src/tests/arrow_rocks_engine_lifecycle_test.cc
)

target_link_libraries(arrow_rocks_engine_lifecycle_test
    PRIVATE
    feature_store_engine
    GTest::gtest_main
)

add_test(NAME arrow_rocks_engine_lifecycle_test COMMAND arrow_rocks_engine_lifecycle_test)

add_executable(metrics_test
    ${PROJECT_SOURCE_DIR}/src/tests/metrics_test.cc
)

target_link_libraries(metrics_test
    PRIVATE
    feature_store_engine
    GTest::gtest_main
)

add_test(NAME metrics_test COMMAND metrics_test)
