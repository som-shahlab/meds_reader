from __future__ import annotations

import datetime
import json
import os
import subprocess
import sys
import time

import meds
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import meds_reader
import meds_reader.transform

metadata = {"dataset_name": "Testing Dataset!"}


# Sleeps to avoid Windows file consistency issues.
def sleep_on_windows():
    if sys.platform == "win32":
        # Windows has file consistency bugs/ issues
        # Need a sleep here
        time.sleep(5)


@pytest.fixture
# Builds a temporary MEDS dataset fixture.
def meds_dataset(tmpdir: str):
    meds_dir = os.path.join(tmpdir, "meds")

    os.mkdir(meds_dir)

    os.mkdir(os.path.join(meds_dir, "metadata"))

    with open(os.path.join(meds_dir, "metadata", "dataset.json"), "w") as f:
        json.dump(metadata, f)

    data_dir = os.path.join(meds_dir, "data")
    os.mkdir(data_dir)

    entries = [
        {
            "subject_id": 32,
            "time": None,
            "code": "Whatever",
            "other": "need",
            "numeric": 38,
        },
        {
            "subject_id": 32,
            "time": datetime.datetime(2013, 10, 2),
            "datetime_value": datetime.datetime(1999, 4, 2, 2, 4, 29, 999999),
            "code": "Whatever2",
        },
        {
            "subject_id": 32,
            "time": datetime.datetime(2019, 12, 2),
            "datetime_value": datetime.datetime(1960, 4, 2, 2, 4, 29, 999999),
            "code": "Whatever2",
        },
        {
            "subject_id": 64,
            "time": datetime.datetime(1969, 12, 31, 23, 59, 58, 999999),
            "datetime_value": datetime.datetime(1969, 12, 31, 23, 59, 59, 999999),
            "code": "Whatever",
            "other": "need",
            "numeric": 38,
        },
        {
            "subject_id": 64,
            "time": datetime.datetime(1969, 12, 31, 23, 59, 59, 999999),
            "datetime_value": datetime.datetime(1969, 12, 31, 23, 59, 59, 1),
            "code": "Whatever2",
        },
        {
            "subject_id": 64,
            "time": datetime.datetime(1970, 1, 1),
            "datetime_value": datetime.datetime(1970, 1, 1),
            "code": "Whatever3",
        },
    ]

    custom_properties = [
        ("datetime_value", pa.timestamp("us")),
        ("other", pa.string()),
        ("numeric", pa.float32()),
    ]

    schema = pa.schema(
        [
            *meds.DataSchema.schema(),
            *(pa.field(name, dtype) for name, dtype in custom_properties),
        ]
    )
    table = pa.Table.from_pylist(entries, schema=schema)

    pq.write_table(table, os.path.join(data_dir, "entries.parquet"))
    return os.path.join(tmpdir, "meds")


@pytest.fixture
# Builds a SubjectDatabase fixture from a converted dataset.
def subject_database(tmpdir: str, meds_dataset: str):

    meds_reader_dir = os.path.join(tmpdir, "meds_reader")

    subprocess.run(
        ["meds_reader_convert", meds_dataset, meds_reader_dir, "--num_threads", "4"],
        check=True,
    )

    sleep_on_windows()

    return meds_reader.SubjectDatabase(str(meds_reader_dir))


@pytest.fixture
# Builds a multithreaded SubjectDatabase fixture.
def threaded_subject_database(tmpdir: str, meds_dataset: str):

    meds_reader_dir = os.path.join(tmpdir, "meds_reader")

    subprocess.run(
        ["meds_reader_convert", meds_dataset, meds_reader_dir, "--num_threads", "4"],
        check=True,
    )

    sleep_on_windows()

    database = meds_reader.SubjectDatabase(str(meds_reader_dir), num_threads=4)
    yield database
    database.terminate()


# Verifies metadata passthrough from the dataset.
def test_metadata(subject_database):
    with open(os.path.join(subject_database.path_to_database, "metadata", "dataset.json")) as f:
        loaded_metadata = json.load(f)
    assert loaded_metadata == metadata


# Verifies database length matches subject count.
def test_size(subject_database):
    assert len(subject_database) == 2


# Verifies missing subjects raise KeyError.
def test_missing(subject_database):
    with pytest.raises(KeyError):
        subject_database[34234]


# Verifies iteration over subject ids.
def test_iter(subject_database):
    assert list(subject_database) == [32, 64]


# Extracts subject ids from a subject iterator.
def h(subjects):
    result = []
    for p in subjects:
        result.append(p.subject_id)
    return result


# Extracts subject ids with associated row data.
def h2(subjects_and_data):
    result = []
    for subject, rows in subjects_and_data:
        assert len(rows) == 1
        row = rows[0]
        print(subject, row)
        result.append((subject.subject_id, row.other))
    return result


# Extracts all associated row markers for each subject shard.
def collect_rows(subjects_and_data):
    return [(subject.subject_id, tuple(row.marker for row in rows)) for subject, rows in subjects_and_data]


# Raises inside a multiprocessing map worker.
def failing_map(subjects):
    list(subjects)
    raise ValueError("intentional map failure")


# Exits a multiprocessing worker without returning a queue result.
def crashing_map(subjects):
    os._exit(7)


# Returns a value that multiprocessing queues cannot serialize.
def unpickleable_map_result(subjects):
    list(subjects)
    return lambda: None


# Exercises map and map_with_data helpers.
def map_helper(subject_database):

    results = list(subject_database.map(h))

    final_result = {a for b in results for a in b}

    print(final_result)

    assert final_result == {32, 64}

    table = pd.DataFrame({"subject_id": [64, 32], "other": [1, 1000]})

    results = list(subject_database.map_with_data(h2, table))

    final_result = {a for b in results for a in b}

    print(final_result)

    assert final_result == {(32, 1000), (64, 1)}


# Verifies map on a single-threaded database.
def test_map(subject_database):
    map_helper(subject_database)


# Verifies map on a threaded database.
def test_map_threaded(threaded_subject_database):
    map_helper(threaded_subject_database)
    threaded_subject_database.terminate()


# Verifies dataframe shards use positional indexes and keep subjects intact.
def test_map_with_data_nondefault_index(threaded_subject_database):
    table = pd.DataFrame(
        {
            "subject_id": [32, 64, 64, 32],
            "marker": ["32-a", "64-a", "64-b", "32-b"],
        },
        index=[10, 20, 30, 40],
    )

    results = list(threaded_subject_database.map_with_data(collect_rows, table))
    flattened = {entry for result in results for entry in result}

    assert flattened == {
        (32, ("32-a", "32-b")),
        (64, ("64-a", "64-b")),
    }


# Verifies worker exceptions are returned to the caller without poisoning the pool.
def test_map_threaded_propagates_exceptions(threaded_subject_database):
    with pytest.raises(RuntimeError, match="ValueError: intentional map failure"):
        list(threaded_subject_database.map(failing_map))

    results = list(threaded_subject_database.map(h))
    assert {subject_id for result in results for subject_id in result} == {32, 64}


# Verifies serialization failures are surfaced and do not poison the pool.
def test_map_threaded_rejects_unpickleable_results(threaded_subject_database):
    with pytest.raises(RuntimeError, match="(AttributeError|PicklingError)"):
        list(threaded_subject_database.map(unpickleable_map_result))

    results = list(threaded_subject_database.map(h))
    assert {subject_id for result in results for subject_id in result} == {32, 64}


# Verifies an unexpectedly terminated worker is detected instead of hanging.
def test_map_threaded_detects_crashed_worker(threaded_subject_database):
    with pytest.raises(RuntimeError, match="map worker failure"):
        list(threaded_subject_database.map(crashing_map))


# Verifies property schema exposed by the database.
def test_properties(subject_database):
    print(subject_database.properties)
    assert subject_database.properties == {
        "code": pa.string(),
        "datetime_value": pa.timestamp("us"),
        "numeric_value": pa.float32(),
        "other": pa.string(),
        "text_value": pa.large_string(),
        "time": pa.timestamp("us"),
        "numeric": pa.float32(),
    }


# Verifies struct columns are ignored instead of aborting conversion.
def test_complex_columns_are_ignored(tmpdir: str):
    meds_dir = os.path.join(tmpdir, "meds")
    data_dir = os.path.join(meds_dir, "data")
    os.makedirs(os.path.join(meds_dir, "metadata"))
    os.mkdir(data_dir)

    with open(os.path.join(meds_dir, "metadata", "dataset.json"), "w") as f:
        json.dump(metadata, f)

    table = pa.Table.from_pylist(
        [
            {
                "subject_id": 1,
                "time": datetime.datetime(2020, 1, 1),
                "code": "A",
                "numeric_value": 1.0,
                "code_components": {"gender": "F", "race_concept_id": 5},
            }
        ],
        schema=pa.schema(
            [
                ("subject_id", pa.int64()),
                ("time", pa.timestamp("us")),
                ("code", pa.string()),
                ("numeric_value", pa.float32()),
                (
                    "code_components",
                    pa.struct([("gender", pa.string()), ("race_concept_id", pa.int64())]),
                ),
            ]
        ),
    )
    pq.write_table(table, os.path.join(data_dir, "entries.parquet"))

    meds_reader_dir = os.path.join(tmpdir, "meds_reader")
    result = subprocess.run(
        ["meds_reader_convert", meds_dir, meds_reader_dir, "--num_threads", "1"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "meds_reader ignoring non-leaf property: code_components" in result.stderr
    database = meds_reader.SubjectDatabase(str(meds_reader_dir))
    assert "code_components" not in database.properties
    assert database[1].events[0].code == "A"


# Verifies missing event properties raise errors.
def test_missing_property(subject_database):
    p = subject_database[32]
    e = p.events[0]

    with pytest.raises(AttributeError):
        print(e.missing)


# Verifies subject and event lookup behavior.
def test_lookup(subject_database):
    p = subject_database[32]

    assert p.subject_id == 32

    assert len(p.events) == 3

    assert p.events[0].code == "Whatever"
    assert p.events[1].code == "Whatever2"

    assert p.events[0].time is None
    assert p.events[1].time == datetime.datetime(2013, 10, 2)

    assert p.events[0].other == "need"
    assert p.events[1].other is None

    assert p.events[0].numeric == 38
    assert p.events[1].numeric is None

    assert p.events[0].datetime_value is None
    assert p.events[1].datetime_value == datetime.datetime(1999, 4, 2, 2, 4, 29, 999999)
    assert p.events[2].datetime_value == datetime.datetime(1960, 4, 2, 2, 4, 29, 999999)

    assert set(p.events[0]) == {
        ("code", "Whatever"),
        ("numeric", 38),
        ("other", "need"),
    }
    assert set(p.events[1]) == {
        ("code", "Whatever2"),
        ("time", datetime.datetime(2013, 10, 2)),
        ("datetime_value", datetime.datetime(1999, 4, 2, 2, 4, 29, 999999)),
    }


# Verifies timestamp decoding on both sides of the Unix epoch boundary.
def test_timestamp_epoch_boundaries(subject_database):
    events = subject_database[64].events

    assert [event.time for event in events] == [
        datetime.datetime(1969, 12, 31, 23, 59, 58, 999999),
        datetime.datetime(1969, 12, 31, 23, 59, 59, 999999),
        datetime.datetime(1970, 1, 1),
    ]
    assert [event.datetime_value for event in events] == [
        datetime.datetime(1969, 12, 31, 23, 59, 59, 999999),
        datetime.datetime(1969, 12, 31, 23, 59, 59, 1),
        datetime.datetime(1970, 1, 1),
    ]


# Verifies subject filtering preserves data consistency.
def test_filter(subject_database):
    print(subject_database.path_to_database)
    sub_database = subject_database.filter([32])

    assert len(sub_database) == 1
    assert list(sub_database) == [32]

    p = subject_database[32]

    assert p.subject_id == 32

    assert len(p.events) == 3

    assert p.events[0].code == "Whatever"
    assert p.events[1].code == "Whatever2"

    assert p.events[0].time is None
    assert p.events[1].time == datetime.datetime(2013, 10, 2)

    assert p.events[0].other == "need"
    assert p.events[1].other is None

    assert p.events[0].numeric == 38
    assert p.events[1].numeric is None

    assert p.events[0].datetime_value is None
    assert p.events[1].datetime_value == datetime.datetime(1999, 4, 2, 2, 4, 29, 999999)

    with pytest.raises(KeyError):
        sub_database[64]

    assert len(sub_database.filter([64])) == 0

    table = pd.DataFrame({"subject_id": [64, 32], "other": [1, 1000]})
    results = list(sub_database.map_with_data(h2, table))
    assert results == [[(32, 1000)]]


# Verifies top-level filters deduplicate ids and discard ids absent from the database.
def test_filter_normalizes_requested_ids(subject_database):
    sub_database = subject_database.filter([64, 32, 32, 10_000])

    assert len(sub_database) == 2
    assert list(sub_database) == [32, 64]
    with pytest.raises(KeyError):
        sub_database[10_000]


# Verifies the event container follows Python sequence indexing semantics.
def test_event_sequence_indexing(subject_database):
    events = subject_database[32].events

    assert events[-1].code == "Whatever2"
    assert [event.code for event in events[::-1]] == ["Whatever2", "Whatever2", "Whatever"]
    with pytest.raises(IndexError):
        events[len(events)]


# Verifies simultaneous event and database iterators remain independent.
def test_multiple_live_iterators(subject_database):
    subject = subject_database[32]
    first_events = iter(subject.events)
    second_events = iter(subject.events)

    assert next(first_events).code == "Whatever"
    assert next(second_events).code == "Whatever"
    assert next(first_events).code == "Whatever2"

    database_iterators = [iter(subject_database._database) for _ in range(6)]
    assert [next(iterator) for iterator in database_iterators] == [32] * 6

    property_iterators = [iter(subject.events[0]) for _ in range(6)]
    first_properties = [next(iterator) for iterator in property_iterators]
    assert first_properties == [first_properties[0]] * 6


# Verifies the native filter accepts an empty selection and rejects missing ids.
def test_native_filter_bounds(tmpdir: str, subject_database):
    empty_ids_path = os.path.join(tmpdir, "empty_subject_ids")
    open(empty_ids_path, "wb").close()
    empty_database_path = os.path.join(tmpdir, "empty_database")

    subprocess.run(
        [
            "meds_reader_filter",
            subject_database.path_to_database,
            empty_database_path,
            empty_ids_path,
        ],
        check=True,
    )
    assert os.path.getsize(os.path.join(empty_database_path, "meds_reader.empty")) > 0
    assert not os.path.exists(os.path.join(empty_database_path, "subject_id"))
    assert not os.path.exists(os.path.join(empty_database_path, "meds_reader.length"))
    assert len(meds_reader.SubjectDatabase(empty_database_path)) == 0

    duplicate_ids_path = os.path.join(tmpdir, "duplicate_subject_ids")
    np.array([64, 32, 64, 32], dtype=np.int64).tofile(duplicate_ids_path)
    duplicate_database_path = os.path.join(tmpdir, "duplicate_database")

    subprocess.run(
        [
            "meds_reader_filter",
            subject_database.path_to_database,
            duplicate_database_path,
            duplicate_ids_path,
        ],
        check=True,
    )
    duplicate_database = meds_reader.SubjectDatabase(duplicate_database_path)
    assert list(duplicate_database) == [32, 64]
    assert len(duplicate_database) == 2

    missing_ids_path = os.path.join(tmpdir, "missing_subject_ids")
    np.array([10_000], dtype=np.int64).tofile(missing_ids_path)
    missing_database_path = os.path.join(tmpdir, "missing_database")

    result = subprocess.run(
        [
            "meds_reader_filter",
            subject_database.path_to_database,
            missing_database_path,
            missing_ids_path,
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert "Could not find subject_id 10000" in result.stderr


# Example transform function for testing dataset transforms.
def _example_transform(
    subject: meds_reader.transform.MutableSubject,
) -> meds_reader.transform.MutableSubject:
    subject.subject_id *= 10
    print(subject)
    return subject


# Fails inside a dataset transform worker.
def _failing_transform(subject: meds_reader.transform.MutableSubject):
    raise ValueError(f"intentional transform failure for {subject.subject_id}")


# Verifies mutable transform model defaults never leak across instances.
def test_mutable_transform_defaults_are_isolated():
    first_subject = meds_reader.transform.MutableSubject(1)
    second_subject = meds_reader.transform.MutableSubject(2)
    first_subject.events.append(meds_reader.transform.MutableEvent(None, "first"))

    assert len(second_subject.events) == 0

    first_event = meds_reader.transform.MutableEvent(None, "first")
    second_event = meds_reader.transform.MutableEvent(None, "second")
    first_event.extra = "value"

    assert second_event.extra is None


# Verifies dataset transform and conversion pipeline.
def test_transform(tmpdir: str, meds_dataset: str):

    target = os.path.join(tmpdir, "modified_meds")
    meds_reader_dir = os.path.join(tmpdir, "modified_meds_reader")

    meds_reader.transform.transform_meds_dataset(meds_dataset, target, _example_transform, 2)

    subprocess.run(
        ["meds_reader_convert", target, meds_reader_dir, "--num_threads", "4"],
        check=True,
    )

    sleep_on_windows()

    database = meds_reader.SubjectDatabase(str(meds_reader_dir))

    assert len(database) == 2

    assert list(database) == [32 * 10, 64 * 10]


# Verifies child transform failures are surfaced to the parent process.
def test_transform_propagates_worker_failure(tmpdir: str, meds_dataset: str):
    target = os.path.join(tmpdir, "failed_transform")

    with pytest.raises(RuntimeError, match="MEDS transform worker failure"):
        meds_reader.transform.transform_meds_dataset(
            meds_dataset,
            target,
            _failing_transform,
            2,
        )
