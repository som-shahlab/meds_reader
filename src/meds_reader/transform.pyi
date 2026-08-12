from __future__ import annotations

import datetime
from typing import Any, Callable, Dict, Iterator, List, Optional, Tuple

import pyarrow as pa

import meds_reader

class MutableSubject(meds_reader.Subject):
    """A subject consists of a subject_id and a sequence of Events"""

    # Initializes a mutable subject with an id and events.
    def __init__(self, subject_id: int, events: Optional[List[MutableEvent]] = None): ...

    subject_id: int
    "The unique identifier for this subject"

    events: List[MutableEvent]
    "Items that have happened to a subject"

class MutableEvent(meds_reader.Event):
    """An event represents a single unit of information about a subject. It contains a time and code, and potentially more properties."""

    # Initializes an event with core fields and extra properties.
    def __init__(
        self,
        time: Optional[datetime.datetime],
        code: str,
        properties: Optional[Dict[str, Any]] = None,
    ): ...

    time: Optional[datetime.datetime]
    "The time the event occurred"

    code: str
    "An identifier for the type of event that occurred"

    # Looks up a dynamic property by name.
    def __getattr__(self, name: str) -> Any:
        """Events can contain arbitrary additional properties. This retrieves the specified property, or returns None"""
        ...
    # Sets a dynamic property on the event.
    def __setattr__(self, name: str, value: Any) -> None:
        """Assign an arbitrary property to this event."""
        ...
    # Iterates over stored event properties.
    def __iter__(self) -> Iterator[Tuple[str, Any]]:
        """Iterate over all non-None properties within this event."""
        ...

# Transforms a MEDS dataset into a new dataset with a given function.
def transform_meds_dataset(
    source_dataset_path: str,
    target_dataset_path: str,
    transform_func: Callable[[MutableSubject], Optional[MutableSubject]],
    num_threads: int = 1,
    schema: Optional[pa.Schema] = None,
):
    """Transform a MEDS dataset using the provided transform function, potentially using multiple threads."""
    ...
