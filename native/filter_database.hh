// Declares database filtering entry point.
//
// This header exposes the native filter API used by CLI wrappers.
#pragma once

// Filters a meds_reader database down to a set of subject ids.
void filter_database(const char* source, const char* destination,
                     const char* subject_ids_file, int num_threads);
