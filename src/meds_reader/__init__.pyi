from __future__ import annotations

import datetime
from typing import Any, Callable, Iterator, List, Mapping, Sequence, Tuple, TypeVar

import pandas as pd
import pyarrow as pa

A = TypeVar("A")

class SubjectDatabase:
    """A SubjectDatabase is a read-only mapping from subject_id to Subject objects.

    It also stores metadata such as meds.DatasetMetadat and the custom per-event properties.
    """

    def __init__(self, path_to_database: str, num_threads: int = 1) -> None:
        """Open a SubjectDatabase. The path must be from convert_to_meds_reader."""
        ...
    path_to_database: str
    "The path to the database object"

    properties: Mapping[str, pa.DataType]
    "The per-event properties for this dataset"

    def __len__(self) -> int:
        """The number of subjects in the database"""
        ...

    def __getitem__(self, subject_id: int) -> Subject:
        """Retrieve a single subject from the database"""
        ...

    def __iter__(self) -> Iterator[int]:
        """Get all subject ids in the database"""
        ...

    def filter(self, subject_ids: List[int]) -> SubjectDatabase:
        """Filter the database to a list of subjects"""
        ...

    def map(self, map_func: Callable[[Iterator[Subject]], A]) -> Iterator[A]:
        """Apply a function to every subject in the database, in a multi-threaded manner.

        map_func is a callable that takes an iterable of subjects.
        """
        ...

    def map_with_data(
        self,
        map_func: Callable[[Iterator[Tuple[Subject, Sequence[Any]]]], A],
        data: pd.DataFrame,
        assume_sorted: bool = False,
    ) -> Iterator[A]:
        """Apply a function with associated data to every subject in the database, in a multi-threaded manner.

        map_func is a callable that takes an iterable of subjects paired with rows from the provided table for that subject_id.

        The provided table must have 'subject_id' as an integer index that will be used for mapping rows.

        Note:
            This code requires the input to be sorted by subject_id. It will automatically do that sorting
            for you, but we also provide assume_sorted to allow people to skip that step for already sorted data.
        """
        ...

    def __enter__(self) -> SubjectDatabase: ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...

class Subject:
    """A subject consists of a subject_id and a sequence of Events"""

    subject_id: int
    "The unique identifier for this subject"

    events: Sequence[Event]
    "Items that have happened to a subject"

class Event:
    """An event represents a single unit of information about a subject. It contains a time and code, and potentially more properties."""

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

__all__ = ["SubjectDatabase", "Subject", "Event"]
