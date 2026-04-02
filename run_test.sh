#!/bin/bash

if [[ "${OSTYPE}" == "darwin"* ]]; then
  export ASAN_OPTIONS="detect_container_overflow=0:detect_leaks=0"
fi

ctest --test-dir build --output-on-failure
