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
