from __future__ import annotations

import datetime
from typing import Any, Callable, Collection, Iterator, List, Mapping, Optional, Sequence, Tuple, TypeVar

import meds
import pyarrow as pa

A = TypeVar("A")

class PatientDatabase:
    """A PatientDatabase is a read-only mapping from patient_id to Patient objects.

    It also stores metadata such as meds.DatasetMetadat and the custom per-event properties.
    """

    def __init__(self, path_to_database: str, num_threads: int = 1) -> None:
        """Open a PatientDatabase. The path must be from convert_to_meds_reader."""
        ...
    path_to_database: str
    "The path to the database object"

    properties: Mapping[str, pa.DataType]
    "The per-event properties for this dataset"

    def __len__(self) -> int:
        """The number of patients in the database"""
        ...

    def __getitem__(self, patient_id: int) -> Patient:
        """Retrieve a single patient from the database"""
        ...

    def __iter__(self) -> Iterator[int]:
        """Get all patient ids in the database"""
        ...

    def filter(self, patient_ids: List[int]) -> PatientDatabase:
        """Filter the database to a list of patients"""
        ...

    def map(self, map_func: Callable[[Iterator[Patient]], A]) -> Iterator[A]:
        """Apply a function to every patient in the database, in a multi-threaded manner.

        map_func is a callable that takes an iterable of patients.
        """
        ...

    def __enter__(self) -> PatientDatabase: ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...

class Patient:
    """A patient consists of a patient_id and a sequence of Events"""

    patient_id: int
    "The unique identifier for this patient"

    events: Sequence[Event]
    "Items that have happened to a patient"

class Event:
    """An event represents a single unit of information about a patient. It contains a time and code, and potentially more properties."""

    time: datetime.datetime
    "The time the event occurred"

    code: str
    "An identifier for the type of event that occured"

    def __getattr__(self, name: str) -> Any:
        """Events can contain arbitrary additional properties. This retrieves the specified property, or returns None"""
        ...

    def __iter__(self) -> Iterator[Tuple[str, Any]]:
        """Iterate through the non-None properties for this type."""
        ...

__all__ = ["PatientDatabase", "Patient", "Event"]
