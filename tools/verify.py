"""
verify.py is a simple check that meds_reader is working correctly on a real dataset.
"""

import meds_reader
import pyarrow.parquet as pq
import time

import random

random.seed(34234)

database = meds_reader.PatientDatabase("../mimic-iv-meds2-reader")
reference = pq.ParquetFile("../mimic-iv-meds2/data/0.parquet")

row_group = reference.read_row_group(random.randint(0, reference.num_row_groups - 1))

events = row_group.schema.field("events").type.value_type
properties = events.field("properties").type

all_properties = {}

for i in range(events.num_fields):
    f = events.field(i)
    all_properties[f.name] = f.type

for i in range(properties.num_fields):
    f = properties.field(i)
    all_properties[f.name] = f.type

del all_properties["properties"]

missing = set(all_properties) - set(database.properties)
extra = set(database.properties) - set(all_properties)

assert len(missing) == 0, f"Had missing properties {missing}"
assert len(extra) == 0, f"Had extra properties {extra}"

assert all_properties == database.properties

num_patients = row_group.shape[0]

random_indices = random.sample(list(range(num_patients)), 200)

row_group_sample = row_group.take(random_indices)

start = time.time()

python_objects = row_group_sample.to_pylist()

end = time.time()

print("Converting at a rate", end - start)

print("Working through", [a["patient_id"] for a in python_objects])


def assert_same(pyarrow_patient, reader_patient):
    assert pyarrow_patient["patient_id"] == reader_patient.patient_id

    assert len(pyarrow_patient["events"]) == len(reader_patient.events)

    for pyarrow_event, reader_event in zip(
        pyarrow_patient["events"], reader_patient.events
    ):
        for property in database.properties:
            if property in pyarrow_event:
                assert pyarrow_event[property] == getattr(reader_event, property)
            else:
                assert pyarrow_event["properties"][property] == getattr(
                    reader_event, property
                )


start = time.time()

for pyarrow_patient in python_objects:
    reader_patient = database[pyarrow_patient["patient_id"]]

    assert_same(pyarrow_patient, reader_patient)

end = time.time()

print("Testing at rate", end - start)
