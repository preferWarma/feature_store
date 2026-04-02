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
