from __future__ import annotations

import importlib.resources
from multiprocessing.context import SpawnProcess
import multiprocessing.spawn
import os
import sys
import multiprocessing
import numpy as np
import pickle
import warnings
import pyarrow.parquet as pq
import pyarrow as pa

from typing import (
    List,
    Mapping,
    Optional,
    Tuple,
    Any,
    Sequence,
    TypeVar,
    Callable,
    Iterator,
    cast,
)

from . import _meds_reader

__doc__ = _meds_reader.__doc__

A = TypeVar("A")

WorkEntry = Tuple[bytes, np.ndarray]

mp = multiprocessing.get_context("spawn")


def meds_reader_convert():
    submodules = importlib.resources.files("meds_reader")
    for module in submodules.iterdir():
        if module.name.startswith("meds_reader_convert"):
            with importlib.resources.as_file(module) as executible:
                os.execv(executible, sys.argv)


def _runner(
    path_to_database: str,
    input_queue: multiprocessing.SimpleQueue[Optional[WorkEntry]],
    result_queue: multiprocessing.SimpleQueue[Any],
) -> None:
    database = _meds_reader.PatientDatabase(path_to_database)
    while True:
        next_work = input_queue.get()
        if next_work is None:
            break

        map_func_str, patient_ids = next_work

        map_func = pickle.loads(map_func_str)
        del map_func_str

        result = map_func(database[int(patient_id)] for patient_id in patient_ids)

        result_queue.put(result)


def _filter_patients(
    all_patient_ids: np.ndarray, filter_list: Sequence[int]
) -> np.ndarray:
    found_patients = all_patient_ids[np.isin(all_patient_ids, filter_list)]
    if len(found_patients) != len(filter_list):
        if len(set(filter_list)) != len(filter_list):
            raise ValueError(
                f"Called filter with a set of patient ids with duplicates {len(set(filter_list))} {len(filter_list)}"
            )

        missing_patients = [a for a in filter_list if a not in all_patient_ids]
        raise ValueError(
            f"Called filter, but couldn't find patients {repr(missing_patients)} {len(filter_list)} {len(found_patients)}"
        )

    return found_patients


class _PatientDatabaseWrapper:
    def __init__(self, db: PatientDatabase, patients_ids: np.ndarray):
        self._db = db
        self._selected_patients = patients_ids
        self.path_to_database = db.path_to_database

    @property
    def metadata(self):
        return self._db.metadata

    @property
    def properties(self):
        return self._db.properties

    def __len__(self) -> int:
        """The number of patients in the database"""
        return len(self._selected_patients)

    def __getitem__(self, patient_id: int) -> Patient:
        """Retrieve a single patient from the database"""
        return self._db[patient_id]

    def __iter__(self) -> Iterator[int]:
        return iter(self._selected_patients)

    def filter(self, patient_ids: Sequence[int]):
        return cast(
            PatientDatabase,
            _PatientDatabaseWrapper(
                self._db, _filter_patients(self._selected_patients, patient_ids)
            ),
        )

    def map(self, map_func: Callable[[Iterator[Patient]], A]) -> Iterator[A]:
        return self._db._map_fast(map_func, self._selected_patients)


class PatientDatabase:
    def __init__(self, path_to_database: str, num_threads: int = 1) -> None:
        self.path_to_database = path_to_database
        self._database = _meds_reader.PatientDatabase(path_to_database)
        self._all_patient_ids: np.ndarray = np.array(list(self._database))
        self._num_threads = num_threads

        if num_threads != 1:
            self._processes: Optional[List[SpawnProcess]] = []

            self._input_queue: multiprocessing.SimpleQueue[Optional[WorkEntry]] = (
                mp.SimpleQueue()
            )
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
    def metadata(self):
        return self._database.metadata

    @property
    def properties(self):
        return self._database.properties

    def __len__(self) -> int:
        """The number of patients in the database"""
        return len(self._database)

    def __getitem__(self, patient_id: int) -> Patient:
        """Retrieve a single patient from the database"""
        return self._database[int(patient_id)]

    def __iter__(self) -> Iterator[int]:
        """Get all patient ids in the database"""
        return iter(self._all_patient_ids)

    def filter(self, patient_ids: Sequence[int]) -> PatientDatabase:
        """Filter to a provided set of patient ids"""
        return cast(
            PatientDatabase,
            _PatientDatabaseWrapper(
                self, _filter_patients(self._all_patient_ids, patient_ids)
            ),
        )

    def map(
        self,
        map_func: Callable[[Iterator[Patient]], A],
    ) -> Iterator[A]:
        """Apply the provided map function to the database"""
        return self._map_fast(map_func, self._all_patient_ids)

    def _map_fast(
        self, map_func: Callable[[Iterator[Patient]], A], patient_ids: np.ndarray
    ) -> Iterator[A]:
        """Apply the provided map function to the database"""
        if self._num_threads != 1:
            patients_per_part = np.array_split(patient_ids, self._num_threads)

            map_func_p = pickle.dumps(map_func)

            for part in patients_per_part:
                self._input_queue.put((map_func_p, part))

            return (self._result_queue.get() for _ in patients_per_part)
        else:
            return iter(
                (
                    map_func(
                        self._database[int(patient_id)] for patient_id in patient_ids
                    ),
                )
            )

    def terminate(self) -> None:
        """Close the pool"""
        if self._num_threads != 1:
            assert self._processes is not None
            for _ in self._processes:
                self._input_queue.put(None)
            for process in self._processes:
                process.join()
            self._input_queue.close()
            self._result_queue.close()
            self._processes = None

    def __del__(self):
        if self._num_threads != 1 and self._processes is not None:
            warnings.warn(
                "PatientDatabase had a thread pool attached, but was never shut down"
            )

    def __enter__(self) -> PatientDatabase:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.terminate()


Patient = _meds_reader.Patient
Event = _meds_reader.Event


__all__ = ["PatientDatabase", "Patient", "Event"]
