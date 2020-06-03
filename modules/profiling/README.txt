Apache Ignite Profiling Module
------------------------------

Apache Ignite Profiling module provides cluster profiling tool based on performance logging to profiling files.

To collect statistics in runtime and to build the performance report follow:

 - Start profiling (use IgniteProfilingMBean JMX bean).
 - Collect workload statistics.
 - Stop profiling (use IgniteProfilingMBean JMX bean).
 - Collect profiling files from all nodes under an empty directory.
 - Run script ./bin/profiling.sh path_to_files to build the performance report.
