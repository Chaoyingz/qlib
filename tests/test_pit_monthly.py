import pandas as pd
import unittest
import sys
import shutil
import qlib
from qlib.data import D
from pathlib import Path

from qlib.tests import GetData

sys.path.append(str(Path(__file__).resolve().parent.parent.joinpath("qlib_scripts")))
from dump_pit import DumpPitData

pd.set_option("display.width", 1000)
pd.set_option("display.max_columns", None)

DATA_DIR = Path(__file__).parent.joinpath("test_pit_monthly_data")
SOURCE_DIR = DATA_DIR / "stock_data/source"
SOURCE_DIR.mkdir(exist_ok=True, parents=True)
QLIB_DIR = DATA_DIR / "qlib_data"
QLIB_DIR.mkdir(exist_ok=True, parents=True)


class TestPITMonthly(unittest.TestCase):
    data = [
        {
            "date": "2021-01-01",
            "period": 202001,
            "value": 1,
            "field": "open",
            "symbol": "sh600519",
        },
        {
            "date": "2021-01-01",
            "period": 202002,
            "value": 2,
            "field": "open",
            "symbol": "sh600519",
        },
        {
            "date": "2021-01-01",
            "period": 202004,
            "value": 3,
            "field": "open",
            "symbol": "sh600519",
        },
        {
            "date": "2021-01-01",
            "period": 202007,
            "value": 7,
            "field": "open",
            "symbol": "sh600519",
        },
        {
            "date": "2021-01-01",
            "period": 202004,
            "value": 4,
            "field": "close",
            "symbol": "sh600519",
        },
    ]

    @classmethod
    def tearDownClass(cls) -> None:
        shutil.rmtree(str(DATA_DIR.resolve()))

    def to_str(self, obj):
        return "".join(str(obj).split())

    def check_same(self, a, b):
        self.assertEqual(self.to_str(a), self.to_str(b))

    @classmethod
    def setUpClass(cls) -> None:
        df = pd.DataFrame(cls.data)
        df.to_csv(SOURCE_DIR.joinpath("sh600519.csv"), index=False)
        GetData().qlib_data(
            name="qlib_data_simple", target_dir=QLIB_DIR, region="cn", delete_old=False, exists_skip=True
        )
        GetData().qlib_data(name="qlib_data", target_dir=QLIB_DIR, region="pit", delete_old=False, exists_skip=True)
        DumpPitData(csv_path=SOURCE_DIR, qlib_dir=QLIB_DIR, interval="m").dump()

    def setUp(self):
        # qlib.init(kernels=1)  # NOTE: set kernel to 1 to make it debug easier
        qlib.init(provider_uri=QLIB_DIR)

    def test_query_feature(self):
        data = D.features(
            ["sh600519"], ["$$open_m", "$$close_m"], start_time="2020-01-01", end_time="2022-07-19", freq="day"
        )
        res = """
                                              $$open_m  $$close_m
        instrument period datetime
        sh600519   202001 2021-01-01       1.0        NaN
                   202002 2021-01-01       2.0        NaN
                   202004 2021-01-01       3.0        4.0
                   202007 2021-01-01       7.0        NaN
        """
        self.check_same(data, res)


if __name__ == "__main__":
    unittest.main()
