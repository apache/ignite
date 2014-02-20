// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Net;
    using System.Collections.Generic;

    using U = GridGain.Client.Util.GridClientUtils;

    /** <summary>Node metrics.</summary> */
    internal class GridClientNodeMetrics : IGridClientNodeMetrics {
        /** <summary>Constructs empty node metrics.</summary> */
        public GridClientNodeMetrics() {
            var zero = new TimeSpan(0);

            WaitingJobs = new GridClientNodeMetricsCounter<long, double>(-1, -1);
            ExecutedJobs = new GridClientNodeMetricsCounter<long, double>(-1, -1);
            RejectedJobs = new GridClientNodeMetricsCounter<long, double>(-1, -1);
            CancelledJobs = new GridClientNodeMetricsCounter<long, double>(-1, -1);
            JobWaitTime = new GridClientNodeMetricsCounter<TimeSpan, TimeSpan>(zero, zero);
            JobExecuteTime = new GridClientNodeMetricsCounter<TimeSpan, TimeSpan>(zero, zero);

            UpTime = IdleTimeTotal = zero;
            StartTime = NodeStartTime = LastUpdateTime = U.Epoch;

            CpuCount = -1;
            CpuCurrentLoad = CpuAverageLoad = -1f;
            HeapMemoryInitialized = HeapMemoryUsed = HeapMemoryCommitted = HeapMemoryMaximum =
            NonHeapMemoryInitialized = NonHeapMemoryUsed = NonHeapMemoryCommitted = NonHeapMemoryMaximum = -1L;

            ThreadCount = new GridClientNodeMetricsCounter<long, double>(-1, -1);
            DaemonThreadCount = -1L;

            FileSystemFreeSpace = FileSystemUsableSpace = FileSystemTotalSpace = -1L;

            LastDataVersion = -1;
        }

        /** <summary>Gets waiting jobs metrics.</summary> */
        public GridClientNodeMetricsCounter<long, double> WaitingJobs {
            get;
            private set;
        }

        /** <summary>Gets active jobs metrics.</summary> */
        public GridClientNodeMetricsCounter<long, double> ExecutedJobs {
            get;
            private set;
        }

        /** <summary>Gets rejected jobs metrics.</summary> */
        public GridClientNodeMetricsCounter<long, double> RejectedJobs {
            get;
            private set;
        }

        /** <summary>Gets canceled jobs metrics.</summary> */
        public GridClientNodeMetricsCounter<long, double> CancelledJobs {
            get;
            private set;
        }

        /** <summary>Job wait time metrics.</summary> */
        public GridClientNodeMetricsCounter<TimeSpan, TimeSpan> JobWaitTime {
            get;
            private set;
        }

        /** <summary>Job execute time metrics.</summary> */
        public GridClientNodeMetricsCounter<TimeSpan, TimeSpan> JobExecuteTime {
            get;
            private set;
        }

        /** <summary>The approximate time when the Java virtual machine started.</summary> */
        public DateTime StartTime {
            get;
            set;
        }

        /**
         * <summary>
         * There can be several grid nodes started in one JVM, so JVM start time will be
         * the same for all of them, but node start time will be different.</summary>
         */
        public DateTime NodeStartTime {
            get;
            set;
        }

        /** <summary>Uptime of the Java virtual machine.</summary> */
        public TimeSpan UpTime {
            get;
            set;
        }

        /** <summary>Last update time.</summary> */
        public DateTime LastUpdateTime {
            get;
            set;
        }

        /** <summary>Total busy time.</summary> */
        public TimeSpan BusyTimeTotal {
            get {
                return UpTime - IdleTimeTotal;
            }
        }

        /** <summary>Total idle time.</summary> */
        public TimeSpan IdleTimeTotal {
            get;
            set;
        }

        /**
         * <summary>
         * Gets percentage of time this node is busy executing jobs vs. idling.
         * <para/>
         * Return percentage of time this node is busy (value is less than
         * or equal to {@code 1} and greater than or equal to {@code 0})</summary>
         */
        public double BusyTimePercentage {
            get {
                var up = UpTime.TotalMilliseconds;
                var busy = BusyTimeTotal.TotalMilliseconds;

                return up > 0 && busy >= 0 ? busy / up : -1;
            }
        }

        /**
         * <summary>
         * Gets percentage of time this node is idling vs. executing jobs.
         * <para/>
         * Return percentage of time this node is idle (value is less than
         * or equal to {@code 1} and greater than or equal to {@code 0})</summary>
         */
        public double IdleTimePercentage {
            get {
                var up = UpTime.TotalMilliseconds;
                var idle = IdleTimeTotal.TotalMilliseconds;

                return up > 0 && idle >= 0 ? idle / up : -1;
            }
        }

        /**
         * <summary>
         * Returns the number of CPUs available to the Java Virtual Machine.
         * This method is equivalent to the {@link Runtime#availableProcessors()}
         * method.
         * <para/>
         * Note that this value may change during successive invocations of the
         * virtual machine.
         * <para/>
         * Return the number of processors available to the virtual machine, never smaller than one.</summary>
         */
        public int CpuCount {
            get;
            set;
        }

        /**
         * <summary>
         * Returns the system load average for the last minute.
         * The system load average is the sum of the number of runnable entities
         * queued to the {@linkplain #getTotalCpus available processors}
         * and the number of runnable entities running on the available processors
         * averaged over a period of time.
         * The way in which the load average is calculated is operating system
         * specific but is typically a damped time-dependent average.
         * <para/>
         * If the load average is not available, a negative value is returned.
         * <para/>
         * This method is designed to provide a hint about the system load
         * and may be queried frequently. The load average may be unavailable on
         * some platform where it is expensive to implement this method.
         * <para/>
         * Return the system load average in <code>[0, 1]</code> range or negative
         * value if not available.</summary>
         */
        public double CpuCurrentLoad {
            get;
            set;
        }

        /**
         * <summary>
         * Average of CPU load value in <code>[0, 1]</code> range
         * over all metrics kept in the history.</summary>
         */
        public double CpuAverageLoad {
            get;
            set;
        }

        /**
         * <summary>
         * Returns the amount of heap memory in bytes that the Java virtual machine
         * initially requests from the operating system for memory management.
         * This method returns {@code -1} if the initial memory size is undefined.
         * <para/>
         * Return the initial size of memory in bytes; {@code -1} if undefined.</summary>
         */
        public long HeapMemoryInitialized {
            get;
            set;
        }

        /**
         * <summary>
         * Returns the current heap size that is used for object allocation.
         * The heap consists of one or more memory pools. This value is
         * the sum of {@code used} heap memory values of all heap memory pools.
         * <para/>
         * The amount of used memory in the returned is the amount of memory
         * occupied by both live objects and garbage objects that have not
         * been collected, if any.
         * <para/>
         * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
         * from the time of the node's startup.
         * <para/>
         * Return amount of heap memory used.</summary>
         */
        public long HeapMemoryUsed {
            get;
            set;
        }

        /**
         * <summary>
         * Returns the amount of heap memory in bytes that is committed for
         * the Java virtual machine to use. This amount of memory is
         * guaranteed for the Java virtual machine to use.
         * The heap consists of one or more memory pools. This value is
         * the sum of {@code committed} heap memory values of all heap memory pools.
         * <para/>
         * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
         * from the time of the node's startup.
         * <para/>
         * Return the amount of committed memory in bytes.</summary>
         */
        public long HeapMemoryCommitted {
            get;
            set;
        }

        /**
         * <summary>
         * Returns the maximum amount of heap memory in bytes that can be
         * used for memory management. This method returns {@code -1}
         * if the maximum memory size is undefined.
         * <para/>
         * This amount of memory is not guaranteed to be available
         * for memory management if it is greater than the amount of
         * committed memory. The Java virtual machine may fail to allocate
         * memory even if the amount of used memory does not exceed this
         * maximum size.
         * <para/>
         * This value represents a setting of the heap memory for Java VM and is
         * not a sum of all initial heap values for all memory pools.
         * <para/>
         * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
         * from the time of the node's startup.
         * <para/>
         * Return the maximum amount of memory in bytes; {@code -1} if undefined.</summary>
         */
        public long HeapMemoryMaximum {
            get;
            set;
        }

        /**
         * <summary>
         * Returns the amount of non-heap memory in bytes that the Java virtual machine
         * initially requests from the operating system for memory management.
         * This method returns {@code -1} if the initial memory size is undefined.
         * <para/>
         * This value represents a setting of non-heap memory for Java VM and is
         * not a sum of all initial heap values for all memory pools.
         * <para/>
         * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
         * from the time of the node's startup.
         * <para/>
         * Return the initial size of memory in bytes; {@code -1} if undefined.</summary>
         */
        public long NonHeapMemoryInitialized {
            get;
            set;
        }

        /**
         * <summary>
         * Returns the current non-heap memory size that is used by Java VM.
         * The non-heap memory consists of one or more memory pools. This value is
         * the sum of {@code used} non-heap memory values of all non-heap memory pools.
         * <para/>
         * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
         * from the time of the node's startup.
         * <para/>
         * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
         * from the time of the node's startup.
         * <para/>
         * Return amount of none-heap memory used.</summary>
         */
        public long NonHeapMemoryUsed {
            get;
            set;
        }

        /**
         * <summary>
         * Returns the amount of non-heap memory in bytes that is committed for
         * the Java virtual machine to use. This amount of memory is
         * guaranteed for the Java virtual machine to use.
         * The non-heap memory consists of one or more memory pools. This value is
         * the sum of {@code committed} non-heap memory values of all non-heap memory pools.
         * <para/>
         * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
         * from the time of the node's startup.
         * <para/>
         * Return the amount of committed memory in bytes.</summary>
         */
        public long NonHeapMemoryCommitted {
            get;
            set;
        }

        /**
         * <summary>
         * Returns the maximum amount of non-heap memory in bytes that can be
         * used for memory management. This method returns {@code -1}
         * if the maximum memory size is undefined.
         * <para/>
         * This amount of memory is not guaranteed to be available
         * for memory management if it is greater than the amount of
         * committed memory.  The Java virtual machine may fail to allocate
         * memory even if the amount of used memory does not exceed this
         * maximum size.
         * <para/>
         * This value represents a setting of the non-heap memory for Java VM and is
         * not a sum of all initial non-heap values for all memory pools.
         * <para/>
         * <b>Note:</b> this is <b>not</b> an aggregated metric and it's calculated
         * from the time of the node's startup.
         * <para/>
         * Return the maximum amount of memory in bytes; {@code -1} if undefined.</summary>
         */
        public long NonHeapMemoryMaximum {
            get;
            set;
        }

        /** <summary>The number of live threads including both daemon and non-daemon threads.</summary> */
        public GridClientNodeMetricsCounter<long, double> ThreadCount {
            get;
            private set;
        }

        /** <summary>The number of live daemon threads.</summary> */
        public long DaemonThreadCount {
            get;
            set;
        }

        /** <summary>Number of unallocated bytes in the partition.</summary> */
        public long FileSystemFreeSpace {
            get;
            set;
        }

        /** <summary>Size of the partition.</summary> */
        public long FileSystemTotalSpace {
            get;
            set;
        }

        /** <summary>Number of bytes available to this virtual machine on the partition.</summary> */
        public long FileSystemUsableSpace {
            get;
            set;
        }

        /**
         * <summary>
         * In-memory data grid assigns incremental versions to all cache operations.
         * This method provides the latest data version on the node.</summary>
         */
        public long LastDataVersion {
            get;
            set;
        }
    }
}