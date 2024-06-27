from __future__ import annotations

from typing import Mapping, Iterator, Sequence, Collection
from typing import Any, Optional
import meds
import datetime
import pyarrow as pa

class PatientDatabase:
    """A PatientDatabase is a read-only mapping from patient_id to Patient objects.

    It also stores metadata such as meds.DatasetMetadat and the custom per-event properties.
    """

    def __init__(self, path_to_database: str) -> None:
        """Open a PatientDatabase. The path must be from convert_to_meds_reader."""
        ...
    metadata: meds.DatasetMetadata
    "The MEDS dataset metadata"

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

__all__ = ["PatientDatabase", "Patient", "Event"]
