# Copyright (c) Microsoft Corporation.
# Licensed under the MIT License.
import pathlib
import shutil
from pathlib import Path
from typing import Iterable, Any
from concurrent.futures import ProcessPoolExecutor

import fire
import pandas as pd
from tqdm import tqdm
from loguru import logger

from qlib.data.storage.file_storage import FileFinancialStorage
from qlib.utils import fname_to_code


class DumpPitData:
    DATA_FILE_SUFFIX = ".data"
    INDEX_FILE_SUFFIX = ".index"

    def __init__(
        self,
        csv_path: str,
        qlib_dir: str,
        backup_dir: str = None,
        max_workers: int = 16,
        file_suffix: str = ".csv",
        exclude_fields: str = "",
        include_fields: str = "",
        limit_nums: int = None,
        interval: str = "q",
    ) -> None:
        """

        Parameters
        ----------
        csv_path: str
            stock data path or directory
        qlib_dir: str
            qlib(dump) data director
        backup_dir: str, default None
            if backup_dir is not None, backup qlib_dir to backup_dir
        max_workers: int, default None
            number of threads
        file_suffix: str, default ".csv"
            file suffix
        include_fields: tuple
            dump fields
        exclude_fields: tuple
            fields not dumped
        limit_nums: int
            Use when debugging, default None
        """
        csv_path = Path(csv_path).expanduser()
        if isinstance(exclude_fields, str):
            exclude_fields = exclude_fields.split(",")
        if isinstance(include_fields, str):
            include_fields = include_fields.split(",")
        self._exclude_fields = tuple(filter(lambda x: len(x) > 0, map(str.strip, exclude_fields)))
        self._include_fields = tuple(filter(lambda x: len(x) > 0, map(str.strip, include_fields)))
        self.file_suffix = file_suffix
        self.csv_files = sorted(csv_path.glob(f"*{self.file_suffix}") if csv_path.is_dir() else [csv_path])
        if limit_nums is not None:
            self.csv_files = self.csv_files[: int(limit_nums)]
        self.qlib_dir = Path(qlib_dir).expanduser()
        self.backup_dir = backup_dir if backup_dir is None else Path(backup_dir).expanduser()
        if backup_dir is not None:
            self._backup_qlib_dir(Path(backup_dir).expanduser())
        self.works = max_workers
        self.interval = interval
        self.calendar = self.get_calendar_df()

    def get_calendar_df(self) -> pd.DataFrame:
        cal_uri = self.qlib_dir / "calendars" / "day.txt"
        if not cal_uri.exists():
            raise ValueError(f"Calendar file not found: {cal_uri}")
        df = pd.read_csv(cal_uri, header=None)
        df.rename(columns={0: "date"}, inplace=True)
        df.index.name = "cal_index"
        df.reset_index(inplace=True)
        return df

    def _backup_qlib_dir(self, target_dir: Path) -> None:
        shutil.copytree(str(self.qlib_dir.resolve()), str(target_dir.resolve()))

    def get_source_data(self, file_path: Path) -> pd.DataFrame:
        df = pd.read_csv(str(file_path.resolve()), low_memory=False)
        return df

    def get_symbol_from_file(self, file_path: Path) -> str:
        return fname_to_code(file_path.name[: -len(self.file_suffix)].strip().lower())

    def get_dump_fields(self, df: Iterable[str]) -> Iterable[str]:
        return (
            set(self._include_fields)
            if self._include_fields
            else set(df[FileFinancialStorage.FIELD_COLUMN_NAME]) - set(self._exclude_fields)
            if self._exclude_fields
            else set(df[FileFinancialStorage.FIELD_COLUMN_NAME])
        )

    def _dump_pit(
        self,
        file_path: pathlib.Path,
    ) -> None:
        """
        dump data as the following format:
            `/path/to/<field>.data`
                [date, period, value, _next]
                [date, period, value, _next]
                [...]
            `/path/to/<field>.index`
                [first_year, index, index, ...]

        `<field.data>` contains the data as the point-in-time (PIT) order: `value` of `period`
        is published at `date`, and its successive revised value can be found at `_next` (linked list).

        `<field>.index` contains the index of value for each period (quarter or year). To save
        disk space, we only store the `first_year` as its followings periods can be easily infered.
        """
        symbol = self.get_symbol_from_file(file_path)
        df = self.get_source_data(file_path)
        if df.empty:
            logger.warning(f"{symbol} file is empty")
            return
        df = df.merge(self.calendar, on="date", how="left")
        df[FileFinancialStorage.DATE_COLUMN_NAME] = df["cal_index"]
        for field in self.get_dump_fields(df):
            field_df = df.query(f"{FileFinancialStorage.FIELD_COLUMN_NAME}=='{field}'").sort_values(
                FileFinancialStorage.DATE_COLUMN_NAME
            )
            if field_df.empty:
                logger.warning(f"Field {field} of {symbol} is empty.")
                continue
            field = f"{field}_{self.interval}"
            FileFinancialStorage(symbol, field, "day").write(field_df)

    def dump(self) -> None:
        logger.info("start dump pit data......")
        with tqdm(total=len(self.csv_files)) as p_bar:
            with ProcessPoolExecutor(max_workers=self.works) as executor:
                for _ in executor.map(self._dump_pit, self.csv_files):
                    p_bar.update()

    def __call__(self, *args: Any, **kwargs: Any) -> None:
        self.dump()


if __name__ == "__main__":
    fire.Fire(DumpPitData)
