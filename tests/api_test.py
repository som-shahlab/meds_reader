import meds_reader
import pyarrow as pa
import pyarrow.parquet as pq
import os
import subprocess
import json
import datetime
import meds
import pytest


metadata = {"dataset_name": "Testing Dataset!"}


@pytest.fixture
def patient_database(tmpdir: str):
    meds_dir = os.path.join(tmpdir, "meds")
    meds_reader_dir = os.path.join(tmpdir, "meds_reader")

    os.mkdir(meds_dir)

    with open(os.path.join(meds_dir, "metadata.json"), "w") as f:
        json.dump(metadata, f)

    data_dir = os.path.join(meds_dir, "data")
    os.mkdir(data_dir)

    entries = [
        {
            "patient_id": 32,
            "events": [
                {
                    "time": datetime.datetime(2012, 10, 2),
                    "code": "Whatever",
                    "properties": {"other": "need"},
                },
                {
                    "time": datetime.datetime(2013, 10, 2),
                    "code": "Whatever2",
                },
            ],
        }
    ]

    custom_properties = pa.struct([("other", pa.string())])

    table = pa.Table.from_pylist(
        entries, schema=meds.schema.patient_schema(custom_properties)
    )

    pq.write_table(table, os.path.join(data_dir, "entries.parquet"))

    subprocess.run(
        ["convert_to_meds_reader", meds_dir, meds_reader_dir, "--num_threads", "4"],
        check=True,
    )

    return meds_reader.PatientDatabase(str(meds_reader_dir))


def test_metadata(patient_database):
    assert patient_database.metadata == metadata


def test_size(patient_database):
    assert len(patient_database) == 1


def test_missing(patient_database):
    with pytest.raises(KeyError):
        patient_database[34234]


def test_iter(patient_database):
    assert list(patient_database) == [32]


def test_properties(patient_database):
    print(patient_database.properties)
    assert patient_database.properties == {
        "code": pa.string(),
        "datetime_value": pa.timestamp("us"),
        "numeric_value": pa.float32(),
        "other": pa.string(),
        "text_value": pa.string(),
        "time": pa.timestamp("us"),
    }


def test_missing_property(patient_database):
    p = patient_database[32]
    e = p.events[0]

    with pytest.raises(AttributeError):
        print(e.missing)


def test_lookup(patient_database):
    p = patient_database[32]

    assert p.patient_id == 32

    assert len(p.events) == 2

    assert p.events[0].code == "Whatever"
    assert p.events[1].code == "Whatever2"

    assert p.events[0].time == datetime.datetime(2012, 10, 2)
    assert p.events[1].time == datetime.datetime(2013, 10, 2)

    assert p.events[0].other == "need"
    assert p.events[1].other is None
