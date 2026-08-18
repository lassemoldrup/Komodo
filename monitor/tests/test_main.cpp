#include "test_support.h"

#include <iostream>

int runCanonicalYcsbCodecTests();
int runClientTxnTests();
int runDebeziumAssemblerTests();
int runRedisReaderTests();

int main() {
  runCanonicalYcsbCodecTests();
  runClientTxnTests();
  runDebeziumAssemblerTests();
  runRedisReaderTests();
  if (blackbox::test::failures != 0) {
    std::cerr << blackbox::test::failures
              << " black-box monitor test(s) failed\n";
    return 1;
  }
  std::cout << "all black-box monitor tests passed\n";
  return 0;
}
