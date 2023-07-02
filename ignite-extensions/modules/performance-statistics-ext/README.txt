Apache Ignite Performance Statistics Module
------------------------------

Apache Ignite Performance Statistics module provides the tool to collect performance statistics from the cluster.

Cluster nodes collect performance statistics to the files placed under 'Ignite_work_directory/perf_stat/'.
Performance statistics files are used to build the report offline.

Building the Report
---------------------------
To collect statistics in runtime and to build the performance report follow:

1) Start collecting performance statistics (use IgnitePerformanceStatisticsMBean JMX bean).
2) Collect workload statistics.
3) Stop collecting performance statistics (use IgnitePerformanceStatisticsMBean JMX bean).
4) Collect performance statistics files from all nodes under an empty directory. For example:

    path_to_files
        ├── node-162c7147-fef8-4ea2-bd25-8653c41fc7fa.prf
        ├── node-7b8a7c5c-f3b7-46c3-90da-e66103c00001.prf
        └── node-faedc6c9-3542-4610-ae10-4ff7e0600000.prf

5) Run script

        performance-statistics/build-report.sh path_to_files

    to build the performance report. It will be created in the new directory under the performance statistics files:

        path_to_files/report_yyyy-MM-dd_HH-mm-ss/

    Open 'report_yyyy-MM-dd_HH-mm-ss/index.html' in the browser to see the report.

Print statistics
---------------------------
Ignite provides a tool to print statistics to a console or to a file in JSON format.

Run the script from the release package of the tool to print statistics:

    performance-statistics-tool/print-statistics.sh path_to_files

Note that `path_to_files` is a path to the performance statistics file or files directory.

The script provides the ability to filter operations by operation's type, time, or cache. For more details run the
help command:

    performance-statistics-tool/print-statistics.sh --help
