// Declares the MEDS-to-meds_reader database conversion entry point.
//
// This header exposes the native conversion API used by CLI wrappers.
#pragma once

// Converts a MEDS dataset into a meds_reader database on disk.
void create_database(const char* source, const char* destination,
                     int num_threads);
