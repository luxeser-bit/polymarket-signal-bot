from __future__ import annotations

import unittest


class SrcCompatTests(unittest.TestCase):
    def test_src_live_paper_runner_import_alias(self) -> None:
        from src.live_paper_runner import LivePaperRunner
        from src.monitor import Monitor

        self.assertEqual(LivePaperRunner.__name__, "LivePaperRunner")
        self.assertEqual(Monitor.__name__, "Monitor")


if __name__ == "__main__":
    unittest.main()
