#!/usr/bin/env python3
"""Bootstrap adapter for the purpose-built YCSB fractured-read probe."""

from bootstrap_adapters.ycsb import YcsbBootstrapAdapter


class YcsbFracturedReadProbeBootstrapAdapter(YcsbBootstrapAdapter):
    benchmark = "ycsb_fractured_read_probe"


ADAPTER = YcsbFracturedReadProbeBootstrapAdapter()
