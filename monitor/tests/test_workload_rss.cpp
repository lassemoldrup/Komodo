#include "../tools/workload_rss.h"
#include "test_support.h"

#include <vector>

static void testRssParser() {
  std::istringstream input("0000-ffff ---p [rollup]\nRss: 12345 kB\nPss: 42 kB\n");
  BLACKBOX_CHECK(WorkloadRssSampler::parseRss(input) == 12345U);
  for (const auto *bad : {"Pss: 42 kB\n", "Rss: -1 kB\n", "Rss: 42 MB\n"}) {
    bool rejected = false;
    try {
      std::istringstream invalid(bad);
      (void)WorkloadRssSampler::parseRss(invalid);
    } catch (const std::exception &) { rejected = true; }
    BLACKBOX_CHECK(rejected);
  }
}

static void testSamplingLifecycle() {
  WorkloadRssSampler sampler;
  sampler.stop();
  BLACKBOX_CHECK(!sampler.started());
  BLACKBOX_CHECK(!sampler.valid());
  BLACKBOX_CHECK(sampler.samples() == 0U);
  sampler.start();
  sampler.start(); // No second worker/window.
  std::vector<unsigned char> data(32U * 1024U * 1024U);
  volatile unsigned char *pages = data.data();
  for (std::size_t i = 0; i < data.size(); i += 4096U) pages[i] = 1;
  std::this_thread::sleep_for(std::chrono::milliseconds(350));
  sampler.stop();
  BLACKBOX_CHECK(sampler.valid());
  BLACKBOX_CHECK(sampler.samples() >= 3U);
  BLACKBOX_CHECK(sampler.peak() >= 32U * 1024U);
  const auto count = sampler.samples();
  sampler.stop();
  BLACKBOX_CHECK(sampler.samples() == count);
}

int runWorkloadRssTests() {
  testRssParser();
  testSamplingLifecycle();
  return 0;
}
