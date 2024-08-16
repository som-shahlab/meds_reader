#include <CLI/CLI.hpp>
#include <iostream>

#include "create_database.hh"

int main(int argc, char** argv) {
    CLI::App app{
        "meds_reader_convert is a program for converting a MEDS dataset to a "
        "meds_reader SubjectDatabase.",
        "meds_reader_convert"};
    argv = app.ensure_utf8(argv);

    std::string source_dataset, destination_database;
    app.add_option("source_dataset", source_dataset,
                   "A path to the source MEDS dataset")
        ->required();
    app.add_option(
           "destination_database", destination_database,
           "A path of where to write the resulting meds_reader database.")
        ->required();

    int num_threads = 1;
    app.add_option("--num_threads", num_threads,
                   "The number of threads to use when processing")
        ->capture_default_str();

    CLI11_PARSE(app, argc, argv);

    create_database(source_dataset.c_str(), destination_database.c_str(),
                    num_threads);

    return 0;
}
