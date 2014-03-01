/* @csharp.file.header */

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

    /** <summary>Node metrics.</summary> */
    public interface IGridClientNodeMetrics {
        /** <summary>Gets waiting jobs metrics.</summary> */
        GridClientNodeMetricsCounter<long, double> WaitingJobs {
            get;
        }

        /** <summary>Gets active jobs metrics.</summary> */
        GridClientNodeMetricsCounter<long, double> ExecutedJobs {
            get;
        }

        /** <summary>Gets rejected jobs metrics.</summary> */
        GridClientNodeMetricsCounter<long, double> RejectedJobs {
            get;
        }

        /** <summary>Gets canceled jobs metrics.</summary> */
        GridClientNodeMetricsCounter<long, double> CancelledJobs {
            get;
        }

        /** <summary>Job wait time metrics.</summary> */
        GridClientNodeMetricsCounter<TimeSpan, TimeSpan> JobWaitTime {
            get;
        }

        /** <summary>Job execute time metrics.</summary> */
        GridClientNodeMetricsCounter<TimeSpan, TimeSpan> JobExecuteTime {
            get;
        }

        /** <summary>The approximate time when the Java virtual machine started.</summary> */
        DateTime StartTime {
            get;
        }

        /**
         * <summary>
         * There can be several grid nodes started in one JVM, so JVM start time will be
         * the same for all of them, but node start time will be different.</summary>
         */
        DateTime NodeStartTime {
            get;
        }

        /** <summary>Uptime of the Java virtual machine.</summary> */
        TimeSpan UpTime {
            get;
        }

        /** <summary>Last update time.</summary> */
        DateTime LastUpdateTime {
            get;
        }

        /** <summary>Total busy time.</summary> */
        TimeSpan BusyTimeTotal {
            get;
        }

        /** <summary>Total idle time.</summary> */
        TimeSpan IdleTimeTotal {
            get;
        }

        /**
         * <summary>
         * Gets percentage of time this node is busy executing jobs vs. idling.
         * <para/>
         * Return percentage of time this node is busy (value is less than
         * or equal to {@code 1} and greater than or equal to {@code 0})</summary>
         */
        double BusyTimePercentage {
            get;
        }

        /**
         * <summary>
         * Gets percentage of time this node is idling vs. executing jobs.
         * <para/>
         * Return percentage of time this node is idle (value is less than
         * or equal to {@code 1} and greater than or equal to {@code 0})</summary>
         */
        double IdleTimePercentage {
            get;
        }

        /**
         * <summary>
         * Returns the number of CPUs available to the Java Virtual Machine.
         * This method is equivalent to the <see cref="Environment.ProcessorCount"/>
         * method.
         * <para/>
         * Note that this value may change during successive invocations of the
         * virtual machine.
         * <para/>
         * Return the number of processors available to the virtual machine, never smaller than one.</summary>
         */
        int CpuCount {
            get;
        }

        /**
         * <summary>
         * Returns the system load average for the last minute.
         * The system load average is the sum of the number of runnable entities
         * queued to the available processors (<see cref="CpuCount"/>)
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
        double CpuCurrentLoad {
            get;
        }

        /**
         * <summary>
         * Average of CPU load value in <code>[0, 1]</code> range
         * over all metrics kept in the history.</summary>
         */
        double CpuAverageLoad {
            get;
        }

        /**
         * <summary>
         * Returns the amount of heap memory in bytes that the Java virtual machine
         * initially requests from the operating system for memory management.
         * This method returns {@code -1} if the initial memory size is undefined.
         * <para/>
         * Return the initial size of memory in bytes; {@code -1} if undefined.</summary>
         */
        long HeapMemoryInitialized {
            get;
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
        long HeapMemoryUsed {
            get;
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
        long HeapMemoryCommitted {
            get;
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
        long HeapMemoryMaximum {
            get;
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
        long NonHeapMemoryInitialized {
            get;
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
        long NonHeapMemoryUsed {
            get;
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
        long NonHeapMemoryCommitted {
            get;
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
        long NonHeapMemoryMaximum {
            get;
        }

        /** <summary>The number of live threads including both daemon and non-daemon threads.</summary> */
        GridClientNodeMetricsCounter<long, double> ThreadCount {
            get;
        }

        /** <summary>The number of live daemon threads.</summary> */
        long DaemonThreadCount {
            get;
        }

        /** <summary>Number of unallocated bytes in the partition.</summary> */
        long FileSystemFreeSpace {
            get;
        }

        /** <summary>Size of the partition.</summary> */
        long FileSystemTotalSpace {
            get;
        }

        /** <summary>Number of bytes available to this virtual machine on the partition.</summary> */
        long FileSystemUsableSpace {
            get;
        }

        /**
         * <summary>
         * In-memory data grid assigns incremental versions to all cache operations.
         * This method provides the latest data version on the node.</summary>
         */
        long LastDataVersion {
            get;
        }
    }

    /** <summary>Counter metrics.</summary> */
    public sealed class GridClientNodeMetricsCounter<TCount, TAverage> {
        /** <summary>Constructs mertrics counter.</summary> */
        internal GridClientNodeMetricsCounter() {
        }

        /**
         * <summary>
         * Constructs mertrics counter with preset values.</summary>
         *
         * <param name="count">Initial value for counter values.</param>
         * <param name="average">Initial value for avarage value.</param>
         */
        internal GridClientNodeMetricsCounter(TCount count, TAverage average) {
            Current = Maximum = Total = count;
            Average = average;
        }

        /** <summary>Current metric's value.</summary> */
        public TCount Current {
            get;
            internal set;
        }

        /** <summary>Maximum metric's value.</summary> */
        public TCount Maximum {
            get;
            internal set;
        }

        /** <summary>Total metric's value.</summary> */
        public TCount Total {
            get;
            internal set;
        }

        /** <summary>Average metric's value.</summary> */
        public TAverage Average {
            get;
            internal set;
        }
    }
}