from __future__ import annotations

import datetime
import multiprocessing
from multiprocessing.context import SpawnProcess
import pickle
import shutil
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Tuple
import os
import glob

import pyarrow as pa
import pyarrow.parquet as pq


mp = multiprocessing.get_context("spawn")


class MutablePatient:
    """A patient consists of a patient_id and a sequence of Events"""

    def __init__(self, patient_id, events: List[MutableEvent] = []):
        # Create a new list to avoid bugs related to shared default parameters
        if events == []:
            events = []

        self.patient_id = patient_id
        self.events = events

    patient_id: int
    "The unique identifier for this patient"

    events: List[MutableEvent]
    "Items that have happened to a patient"

    def __eq__(self, other: MutablePatient) -> bool:
        return (self.patient_id, self.events) == (other.patient_id, other.events)


class MutableEvent:
    """An event represents a single unit of information about a patient. It contains a time and code, and potentially more properties."""

    def __init__(
        self, time: datetime.datetime, code: str, properties: Dict[str, Any] = {}
    ):
        if properties == {}:
            properties = {}

        super().__setattr__("properties", properties)

        properties["code"] = code
        properties["time"] = time

    time: datetime.datetime
    "The time the event occurred"

    code: str
    "An identifier for the type of event that occured"

    def __getattr__(self, name: str) -> Any:
        """Events can contain arbitrary additional properties. This retrieves the specified property, or returns None"""
        return self.properties.get(name)

    def __setattr__(self, name: str, value: Any) -> None:
        self.properties[name] = value

    def __iter__(self) -> Iterator[Tuple[str, Any]]:
        """Iterate over all non-None properties within this event."""
        yield from self.properties.items()

    def __eq__(self, other: MutableEvent) -> bool:
        return self.properties == other.properties


def _convert_dict_to_patient(
    patient_id, events: List[Mapping[str, Any]]
) -> MutablePatient:
    def create_event(event_dict: Mapping[str, Any]) -> MutableEvent:
        time = event_dict["time"]
        code = event_dict["code"]
        properties = {k: v for k, v in event_dict.items() if k not in ("time", "code")}
        return MutableEvent(time, code, properties)

    return MutablePatient(
        patient_id=patient_id,
        events=[create_event(event_dict) for event_dict in events],
    )


def _convert_patient_to_dict(patient: MutablePatient) -> List[Mapping[str, Any]]:
    def create_event(event: MutableEvent) -> Mapping[str, Any]:
        result = {k: v for k, v in event}
        result["patient_id"] = patient.patient_id
        return result

    return [create_event(event) for event in patient.events]


def _transform_meds_dataset_worker(
    work_queue: multiprocessing.SimpleQueue[Optional[str]],
    transform_func_pkl: bytes,
    out_path: str,
    schema: pa.Schema,
):
    writer = pq.ParquetWriter(out_path, schema, compression="zstd")

    transform_func = pickle.loads(transform_func_pkl)

    while True:
        item = work_queue.get()
        if item is None:
            return
        source_path = item
        reader = pq.ParquetFile(source_path)

        current_patient_id = None
        current_events = None

        transformed_events = []

        def flush_patient():
            assert current_patient_id is not None
            assert current_events is not None
            patient = _convert_dict_to_patient(current_patient_id, current_events)

            updated_patient = transform_func(patient)
            if updated_patient is not None:
                transformed_events.extend(_convert_patient_to_dict(updated_patient))

        for row_group in range(0, reader.num_row_groups):
            original_table = reader.read_row_group(row_group)
            for event_dict in original_table.to_pylist():
                if (
                    current_patient_id is None
                    or event_dict["patient_id"] != current_patient_id
                ):
                    if current_patient_id is not None:
                        flush_patient()

                    current_patient_id = event_dict["patient_id"]
                    current_events = []

                assert current_events is not None
                current_events.append(event_dict)

            if row_group == reader.num_row_groups - 1:
                if current_patient_id is not None:
                    flush_patient()

            new_table = pa.Table.from_pylist(transformed_events, schema=schema)
            writer.write_table(new_table)
            transformed_events = []


def transform_meds_dataset(
    source_dataset_path: str,
    target_dataset_path: str,
    transform_func: Callable[[MutablePatient], MutablePatient],
    num_threads: int = 1,
):
    """Transform a MEDS dataset using the provided transform_func"""
    os.mkdir(target_dataset_path)
    shutil.copytree(
        os.path.join(source_dataset_path, "metadata"),
        os.path.join(target_dataset_path, "metadata"),
    )

    source_parquet_files = list(
        glob.glob(
            os.path.join(source_dataset_path, "data", "**", "*.parquet"), recursive=True
        )
    )

    assert len(source_parquet_files) > 0

    schema: Optional[pa.Schema] = None

    for file in source_parquet_files:
        reader = pq.ParquetFile(file)

        if schema is None:
            schema = reader.schema_arrow
        else:
            assert schema == reader.schema_arrow

    work_queue: multiprocessing.SimpleQueue[Optional[str]] = mp.SimpleQueue()
    for file in source_parquet_files:
        work_queue.put(file)

    for _ in range(num_threads):
        work_queue.put(None)

    transform_func_pkl = pickle.dumps(transform_func)

    os.mkdir(os.path.join(target_dataset_path, "data"))

    if num_threads == 1:
        _transform_meds_dataset_worker(
            work_queue,
            transform_func_pkl,
            os.path.join(target_dataset_path, "data", "0.parquet"),
            schema,
        )
    else:
        processes: List[SpawnProcess] = []

        for i in range(num_threads):
            target_path = os.path.join(target_dataset_path, "data", f"{i}.parquet")
            process = mp.Process(
                target=_transform_meds_dataset_worker,
                args=(work_queue, transform_func_pkl, target_path, schema),
            )
            process.start()
            processes.append(process)

        for process in processes:
            process.join()

    work_queue.close()
