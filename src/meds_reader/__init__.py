from ._meds_reader import PatientDatabase, Patient, Event
from . import _meds_reader

import importlib.resources
import os
import sys

__doc__ = _meds_reader.__doc__


def convert_to_meds_reader():
    submodules = importlib.resources.files("meds_reader")
    for module in submodules.iterdir():
        if module.name.startswith("meds_reader_convert"):
            with importlib.resources.as_file(module) as executible:
                os.execv(executible, sys.argv)


__all__ = ["PatientDatabase", "Patient", "Event"]
