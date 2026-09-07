#include "test_support.h"

#include <iostream>

int runBackendAdapterTests();
int runBenchmarkAdapterTests();
int runCanonicalTpccCodecTests();
int runCanonicalSmallBankCodecTests();
int runCanonicalYcsbCodecTests();
int runCheckerAdapterTests();
int runClientTxnTests();
int runDebeziumAssemblerTests();
int runProtocolRecordTests();
int runRedisReaderTests();
int runRunControllerTests();
int runStrictJsonTests();
int runTransactionJoinerTests();
int runWorkloadRssTests();

int main() {
  runBackendAdapterTests();
  runBenchmarkAdapterTests();
  runCanonicalTpccCodecTests();
  runCanonicalSmallBankCodecTests();
  runCanonicalYcsbCodecTests();
  runCheckerAdapterTests();
  runClientTxnTests();
  runDebeziumAssemblerTests();
  runProtocolRecordTests();
  runRedisReaderTests();
  runRunControllerTests();
  runStrictJsonTests();
  runTransactionJoinerTests();
  runWorkloadRssTests();
  if (blackbox::test::failures != 0) {
    std::cerr << blackbox::test::failures
              << " black-box monitor test(s) failed\n";
    return 1;
  }
  std::cout << "all black-box monitor tests passed\n";
  return 0;
}
