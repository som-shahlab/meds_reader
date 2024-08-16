from __future__ import annotations

import argparse
import collections
import glob
import importlib.resources
import multiprocessing
import multiprocessing.spawn
import os
import pickle
import random
import sys
import warnings
from multiprocessing.context import SpawnProcess
from typing import Any, Callable, Iterator, List, Optional, Sequence, Tuple, TypeVar, Union, cast

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

import meds_reader._meds_reader as _meds_reader

Subject = _meds_reader.Subject
Event = _meds_reader.Event

__doc__ = _meds_reader.__doc__

A = TypeVar("A")

WorkEntry = Tuple[bytes, Union[np.ndarray, pd.DataFrame]]

mp = multiprocessing.get_context("spawn")


def meds_reader_verify():
    parser = argparse.ArgumentParser(description="Verify that a meds_reader dataset matches a source dataset")
    parser.add_argument("meds_dataset", type=str, help="A MEDS dataset to compare against")
    parser.add_argument("meds_reader_database", type=str, help="A meds_reader database to verify")

    args = parser.parse_args()

    database = SubjectDatabase(args.meds_reader_database)

    random.seed(3452342)

    files = sorted(glob.glob(os.path.join(args.meds_dataset, "data", "**", "*.parquet"), recursive=True))

    file = random.choice(files)
    reference = pq.ParquetFile(file)

    row_group = reference.read()

    custom_fields = sorted(set(row_group.schema.names) - {"subject_id"})
    all_properties = {k: row_group.schema.field(k).type for k in custom_fields}

    missing = set(all_properties) - set(database.properties)
    extra = set(database.properties) - set(all_properties)

    assert len(missing) == 0, f"Had missing properties {missing}"
    assert len(extra) == 0, f"Had extra properties {extra}"

    assert all_properties == database.properties

    python_objects = row_group.to_pylist()

    subject_objects = collections.defaultdict(list)

    for obj in python_objects:
        subject_id = obj["subject_id"]
        del obj["subject_id"]
        subject_objects[subject_id].append(obj)

    def assert_same(pyarrow_subject, reader_subject):

        assert len(pyarrow_subject) == len(
            reader_subject.events
        ), f"{len(pyarrow_subject)} {len(reader_subject.events)}"

        for pyarrow_event, reader_event in zip(pyarrow_subject, reader_subject.events):
            for property in database.properties:
                actual = getattr(reader_event, property)
                if property in pyarrow_event:
                    expected = pyarrow_event[property]
                else:
                    expected = pyarrow_event["properties"][property]

                assert actual == expected, (
                    f"Got {actual} expected {expected} for {reader_subject} {property}"
                    f" {pyarrow_event['time']} {reader_event.time}"
                )

    for subject_id, pyarrow_subject in subject_objects.items():
        reader_subject = database[subject_id]

        assert_same(pyarrow_subject, reader_subject)

    print("Test passed!")


def meds_reader_convert():
    submodules = importlib.resources.files("meds_reader")
    for module in submodules.iterdir():
        if module.name.startswith("meds_reader_convert"):
            with importlib.resources.as_file(module) as executible:
                os.execv(executible, sys.argv)


def _row_generator(database: _meds_reader.SubjectDatabase, data: pd.DataFrame):
    current_index = None
    current_rows: List[Any] = []
    for row in data.itertuples(index=False):
        if current_index is None:
            current_index = int(row.subject_id)
        if current_index is not None and current_index != int(row.subject_id):
            yield (database[current_index], current_rows)
            current_rows, current_index = [row], int(row.subject_id)
        else:
            current_rows.append(row)
    if current_index is not None:
        yield (database[current_index], current_rows)


def _runner(
    path_to_database: str,
    input_queue: multiprocessing.SimpleQueue[Optional[WorkEntry]],
    result_queue: multiprocessing.SimpleQueue[Any],
) -> None:
    database = _meds_reader.SubjectDatabase(path_to_database)
    while True:
        next_work = input_queue.get()
        if next_work is None:
            break

        map_func_str, subject_ids = next_work

        map_func = pickle.loads(map_func_str)
        del map_func_str

        if isinstance(subject_ids, pd.DataFrame):
            result = map_func(_row_generator(database, subject_ids))
        elif isinstance(subject_ids, np.ndarray):
            result = map_func(database[int(subject_id)] for subject_id in subject_ids)
        else:
            raise RuntimeError("Should only be given numpy arrays or data frames")

        result_queue.put(result)


class _SubjectDatabaseWrapper:
    def __init__(self, db: SubjectDatabase, subjects_ids: np.ndarray):
        self._db = db
        self._selected_subjects = subjects_ids
        self.path_to_database = db.path_to_database

    @property
    def properties(self):
        return self._db.properties

    def __len__(self) -> int:
        """The number of subjects in the database"""
        return len(self._selected_subjects)

    def __getitem__(self, subject_id: int) -> Any:
        """Retrieve a single subject from the database"""
        return self._db[subject_id]

    def __iter__(self) -> Iterator[int]:
        return iter(self._selected_subjects)

    def filter(self, subject_ids: Sequence[int]):
        return cast(SubjectDatabase, _SubjectDatabaseWrapper(self._db, np.sort(subject_ids)))

    def map_with_data(
        self,
        map_func: Callable[[Iterator[Any]], A],
        data: pd.DataFrame,
        assume_sorted: bool = False,
    ) -> Iterator[A]:
        return self._db.map_with_data(map_func, data, assume_sorted)

    def map(self, map_func: Callable[[Iterator[Any]], A]) -> Iterator[A]:
        return self._db._map_fast(map_func, self._selected_subjects)


class SubjectDatabase:
    def __init__(self, path_to_database: str, num_threads: int = 1) -> None:
        self.path_to_database = path_to_database
        self._num_threads = num_threads
        self._database = _meds_reader.SubjectDatabase(path_to_database)
        self._all_subject_ids: np.ndarray = np.array(list(self._database))

        if num_threads != 1:
            self._processes: Optional[List[SpawnProcess]] = []

            self._input_queue: multiprocessing.SimpleQueue[Optional[WorkEntry]] = mp.SimpleQueue()
            self._result_queue: multiprocessing.SimpleQueue[Any] = mp.SimpleQueue()

            for _ in range(num_threads):
                process = mp.Process(
                    target=_runner,
                    kwargs={
                        "path_to_database": path_to_database,
                        "input_queue": self._input_queue,
                        "result_queue": self._result_queue,
                    },
                )
                process.start()
                self._processes.append(process)

        self.path_to_database = path_to_database

    @property
    def properties(self):
        return self._database.properties

    def __len__(self) -> int:
        """The number of subjects in the database"""
        return len(self._database)

    def __getitem__(self, subject_id: int) -> Any:
        """Retrieve a single subject from the database"""
        return self._database[int(subject_id)]

    def __iter__(self) -> Iterator[int]:
        """Get all subject ids in the database"""
        return iter(self._all_subject_ids)

    def filter(self, subject_ids: Sequence[int]) -> SubjectDatabase:
        """Filter to a provided set of subject ids"""
        return cast(
            SubjectDatabase,
            _SubjectDatabaseWrapper(self, np.sort(subject_ids)),
        )

    def map(
        self,
        map_func: Callable[[Iterator[Any]], A],
    ) -> Iterator[A]:
        """Apply the provided map function to the database"""
        return self._map_fast(map_func, self._all_subject_ids)

    def map_with_data(
        self,
        map_func: Callable[[Iterator[Any]], A],
        data: pd.DataFrame,
        assume_sorted: bool = False,
    ) -> Iterator[A]:
        """Apply the provided map function to the database"""

        assert "subject_id" in data.columns

        if not assume_sorted:
            data = data.sort_values(by=["subject_id"])

        if self._num_threads != 1:
            num_rows = data.shape[0]
            num_rows_per_shard = (num_rows + self._num_threads - 1) // self._num_threads

            subject_ids = data["subject_id"]

            map_func_p = pickle.dumps(map_func)

            num_parts = 0
            last_index = 0
            for _ in range(self._num_threads):
                next_index = min(num_rows, last_index + num_rows_per_shard)
                while (next_index < num_rows) and (subject_ids[next_index - 1] == subject_ids[next_index]):
                    next_index += 1

                part = data.iloc[last_index:next_index]
                self._input_queue.put((map_func_p, part))

                last_index = next_index
                num_parts += 1

                if last_index == num_rows:
                    break

            return (self._result_queue.get() for _ in range(num_parts))
        else:
            return iter((map_func(_row_generator(self._database, data)),))

    def _map_fast(self, map_func: Callable[[Iterator[Any]], A], subject_ids: np.ndarray) -> Iterator[A]:
        """Apply the provided map function to the database"""
        if self._num_threads != 1:
            subjects_per_part = np.array_split(subject_ids, self._num_threads)

            map_func_p = pickle.dumps(map_func)

            for part in subjects_per_part:
                self._input_queue.put((map_func_p, part))

            return (self._result_queue.get() for _ in subjects_per_part)
        else:
            return iter((map_func(self._database[int(subject_id)] for subject_id in subject_ids),))

    def terminate(self) -> None:
        """Close the pool"""
        if self._num_threads != 1 and getattr(self, "_processes", None) is not None:
            assert self._processes is not None
            for _ in self._processes:
                self._input_queue.put(None)
            for process in self._processes:
                process.join()
            self._input_queue.close()
            self._result_queue.close()
            self._processes = None

    def __del__(self):
        if self._num_threads != 1 and getattr(self, "_processes", None) is not None:
            warnings.warn("SubjectDatabase had a thread pool attached, but was never shut down")

    def __enter__(self) -> SubjectDatabase:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.terminate()


__all__ = ["SubjectDatabase", "Subject", "Event"]
