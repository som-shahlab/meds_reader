from __future__ import annotations

import datetime
import multiprocessing
from multiprocessing.context import SpawnProcess
import pickle
import shutil
from typing import Any, Callable, Dict, Iterator, List, Mapping, Optional, Tuple
import os

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


def _convert_dict_to_patient(patient_dict: Mapping[str, Any]) -> MutablePatient:
    def create_event(event_dict: Mapping[str, Any]) -> MutableEvent:
        time = event_dict["time"]
        code = event_dict["code"]
        properties = {
            k: v
            for k, v in event_dict.items()
            if k not in ("time", "code", "properties")
        }
        for k, v in (event_dict.get("properties") or {}).items():
            properties[k] = v
        return MutableEvent(time, code, properties)

    return MutablePatient(
        patient_id=patient_dict["patient_id"],
        events=[create_event(event_dict) for event_dict in patient_dict["events"]],
    )


def _convert_patient_to_dict(patient: MutablePatient) -> Mapping[str, Any]:
    known_properties = ["datetime_value", "numeric_value", "text_value", "time", "code"]

    def create_event(event: MutableEvent) -> Mapping[str, Any]:
        result = {}
        for k in known_properties:
            if getattr(event, k, None) is not None:
                result[k] = getattr(event, k)

        result["properties"] = {k: v for k, v in event if k not in known_properties}
        return result

    return {
        "patient_id": patient.patient_id,
        "events": [create_event(event) for event in patient.events],
    }


def _transform_meds_dataset_worker(
    work_queue: multiprocessing.SimpleQueue[Optional[Tuple[str, int, int]]],
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
        source_path, start_index, end_index = item
        reader = pq.ParquetFile(source_path)
        for row_group in range(start_index, end_index):
            new_patients = []
            original_table = reader.read_row_group(row_group)
            for patient_dict in original_table.to_pylist():
                patient = _convert_dict_to_patient(patient_dict)
                updated_patient = transform_func(patient)
                if updated_patient is not None:
                    new_patients.append(_convert_patient_to_dict(updated_patient))

            new_table = pa.Table.from_pylist(new_patients, schema=schema)
            writer.write_table(new_table)


def transform_meds_dataset(
    source_dataset_path: str,
    target_dataset_path: str,
    transform_func: Callable[[MutablePatient], MutablePatient],
    num_threads: int = 1,
):
    """Transform a MEDS dataset using the provided transform_func"""
    os.mkdir(target_dataset_path)
    shutil.copyfile(
        os.path.join(source_dataset_path, "metadata.json"),
        os.path.join(target_dataset_path, "metadata.json"),
    )

    source_parquet_files = [
        os.path.join(source_dataset_path, "data", a)
        for a in os.listdir(os.path.join(source_dataset_path, "data"))
    ]
    row_groups_per_file = {}
    total_row_groups = 0

    schema: Optional[pa.Schema] = None

    for file in source_parquet_files:
        reader = pq.ParquetFile(file)
        row_groups_per_file[file] = reader.num_row_groups
        total_row_groups += reader.num_row_groups

        if schema is None:
            schema = reader.schema_arrow
        else:
            assert schema == reader.schema_arrow

    row_groups_per_thread = (total_row_groups + num_threads - 1) // num_threads

    work_queue: multiprocessing.SimpleQueue[Optional[Tuple[str, int, int]]] = (
        mp.SimpleQueue()
    )
    for file in source_parquet_files:
        current_row_group = 0
        while current_row_group < row_groups_per_file[file]:
            end = min(
                current_row_group + row_groups_per_thread, row_groups_per_file[file]
            )
            work_queue.put((file, current_row_group, end))
            current_row_group = end

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
