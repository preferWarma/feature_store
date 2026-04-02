find_package(benchmark CONFIG REQUIRED)

add_executable(engine_bench
    ${PROJECT_SOURCE_DIR}/src/tests/benchmark/engine_bench.cc
)

target_link_libraries(engine_bench
    PRIVATE
    feature_store_engine
    benchmark::benchmark
)

target_compile_options(engine_bench PRIVATE)
