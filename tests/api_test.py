import datetime
import json
import os
import subprocess

import meds
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import meds_reader
import meds_reader.transform

metadata = {"dataset_name": "Testing Dataset!"}


@pytest.fixture
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
            "patient_id": 32,
            "time": None,
            "code": "Whatever",
            "other": "need",
            "numeric": 38,
        },
        {
            "patient_id": 32,
            "time": datetime.datetime(2013, 10, 2),
            "datetime_value": datetime.datetime(1999, 4, 2, 2, 4, 29, 999999),
            "code": "Whatever2",
        },
        {
            "patient_id": 32,
            "time": datetime.datetime(2019, 12, 2),
            "datetime_value": datetime.datetime(1999, 4, 2, 2, 4, 29, 999999),
            "code": "Whatever2",
        },
        {
            "patient_id": 64,
            "time": datetime.datetime(2012, 10, 2),
            "code": "Whatever",
            "other": "need",
            "numeric": 38,
        },
        {
            "patient_id": 64,
            "time": datetime.datetime(2013, 10, 2),
            "datetime_value": datetime.datetime(1999, 4, 2, 2, 4, 29, 999999),
            "code": "Whatever2",
        },
        {
            "patient_id": 64,
            "time": datetime.datetime(2013, 10, 2),
            "datetime_value": datetime.datetime(1999, 4, 2, 2, 4, 29, 999999),
            "code": "Whatever3",
        },
    ]

    custom_properties = [
        ("datetime_value", pa.timestamp("us")),
        ("other", pa.string()),
        ("numeric", pa.float32()),
    ]

    table = pa.Table.from_pylist(entries, schema=meds.schema.data_schema(custom_properties))

    pq.write_table(table, os.path.join(data_dir, "entries.parquet"))
    return os.path.join(tmpdir, "meds")


@pytest.fixture
def patient_database(tmpdir: str, meds_dataset: str):

    meds_reader_dir = os.path.join(tmpdir, "meds_reader")

    subprocess.run(
        ["meds_reader_convert", meds_dataset, meds_reader_dir, "--num_threads", "4"],
        check=True,
    )

    return meds_reader.PatientDatabase(str(meds_reader_dir))


def test_metadata(patient_database):
    with open(os.path.join(patient_database.path_to_database, "metadata", "dataset.json")) as f:
        loaded_metadata = json.load(f)
    assert loaded_metadata == metadata


def test_size(patient_database):
    assert len(patient_database) == 2


def test_missing(patient_database):
    with pytest.raises(KeyError):
        patient_database[34234]


def test_iter(patient_database):
    assert list(patient_database) == [32, 64]


def test_properties(patient_database):
    print(patient_database.properties)
    assert patient_database.properties == {
        "code": pa.string(),
        "datetime_value": pa.timestamp("us"),
        "numeric_value": pa.float32(),
        "other": pa.string(),
        "time": pa.timestamp("us"),
        "numeric": pa.float32(),
    }


def test_missing_property(patient_database):
    p = patient_database[32]
    e = p.events[0]

    with pytest.raises(AttributeError):
        print(e.missing)


def test_lookup(patient_database):
    p = patient_database[32]

    assert p.patient_id == 32

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


def test_filter(patient_database):
    sub_database = patient_database.filter([32])

    assert len(sub_database) == 1
    assert list(sub_database) == [32]

    p = patient_database[32]

    assert p.patient_id == 32

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


def _example_transform(
    patient: meds_reader.transform.MutablePatient,
) -> meds_reader.transform.MutablePatient:
    patient.patient_id *= 10
    print(patient)
    return patient


def test_transform(tmpdir: str, meds_dataset: str):

    target = os.path.join(tmpdir, "modified_meds")
    meds_reader_dir = os.path.join(tmpdir, "modified_meds_reader")

    meds_reader.transform.transform_meds_dataset(meds_dataset, target, _example_transform, 2)

    subprocess.run(
        ["meds_reader_convert", target, meds_reader_dir, "--num_threads", "4"],
        check=True,
    )

    database = meds_reader.PatientDatabase(str(meds_reader_dir))

    assert len(database) == 2

    assert list(database) == [32 * 10, 64 * 10]
