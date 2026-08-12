from __future__ import annotations

import datetime
from typing import Any, Callable, Iterator, Mapping, Optional, Sequence, Tuple, TypeVar

import pandas as pd
import pyarrow as pa

A = TypeVar("A")

class SubjectDatabase:
    """A SubjectDatabase is a read-only mapping from subject_id to Subject objects.

    It also stores metadata such as meds.DatasetMetadata and the custom per-event properties.
    """

    # Opens a SubjectDatabase from a path on disk.
    def __init__(self, path_to_database: str, num_threads: int = 1) -> None:
        """Open a SubjectDatabase. The path must be from convert_to_meds_reader."""
        ...
    path_to_database: str
    "The path to the database object"

    properties: Mapping[str, pa.DataType]
    "The per-event properties for this dataset"

    # Returns the number of subjects in the database.
    def __len__(self) -> int:
        """The number of subjects in the database"""
        ...
    # Retrieves a single subject by id.
    def __getitem__(self, subject_id: int) -> Subject:
        """Retrieve a single subject from the database"""
        ...
    # Iterates over subject ids in the database.
    def __iter__(self) -> Iterator[int]:
        """Get all subject ids in the database"""
        ...
    # Filters the database to a list of subjects.
    def filter(self, subject_ids: Sequence[int]) -> SubjectDatabase:
        """Filter the database to a list of subjects"""
        ...
    # Applies a map function to subjects.
    def map(self, map_func: Callable[[Iterator[Subject]], A]) -> Iterator[A]:
        """Apply a function to every subject in the database, in a multi-threaded manner.

        map_func is a callable that takes an iterable of subjects.
        """
        ...
    # Applies a map function to subjects with associated data rows.
    def map_with_data(
        self,
        map_func: Callable[[Iterator[Tuple[Subject, Sequence[Any]]]], A],
        data: pd.DataFrame,
        assume_sorted: bool = False,
    ) -> Iterator[A]:
        """Apply a function with associated data to every subject in the database, in a multi-threaded manner.

        map_func is a callable that takes an iterable of subjects paired with rows from the provided table for that subject_id.

        The provided table must have an integer 'subject_id' column that will be used for mapping rows.

        Note:
            This code requires the input to be sorted by subject_id. It will automatically do that sorting
            for you, but we also provide assume_sorted to allow people to skip that step for already sorted data.
        """
        ...
    # Enters a context-managed database session.
    def __enter__(self) -> SubjectDatabase: ...
    # Exits a context-managed database session.
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...
    # Shuts down worker processes owned by the database.
    def terminate(self) -> None: ...

class Subject:
    """A subject consists of a subject_id and a sequence of Events"""

    subject_id: int
    "The unique identifier for this subject"

    events: Sequence[Event]
    "Items that have happened to a subject"

class Event:
    """An event represents a single unit of information about a subject. It contains a time and code, and potentially more properties."""

    time: Optional[datetime.datetime]
    "The time the event occurred"

    code: str
    "An identifier for the type of event that occurred"

    # Retrieves a dynamic event property by name.
    def __getattr__(self, name: str) -> Any:
        """Events can contain arbitrary additional properties. This retrieves the specified property, or returns None"""
        ...
    # Iterates through non-None event properties.
    def __iter__(self) -> Iterator[Tuple[str, Any]]:
        """Iterate through the non-None properties for this type."""
        ...

__all__ = ["SubjectDatabase", "Subject", "Event"]
