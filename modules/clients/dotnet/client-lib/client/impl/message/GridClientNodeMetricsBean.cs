/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using GridGain.Client.Portable;

    using PU = GridGain.Client.Impl.Portable.GridClientPortableUilts;

    /** <summary>Node metrics bean.</summary> */
    [GridClientPortableId(PU.TYPE_NODE_METRICS_BEAN)]
    internal class GridClientNodeMetricsBean : IGridClientPortable {
        /** Portable type ID. */
        // TODO: GG-8535: Remove in favor of normal IDs.
        public static readonly int PORTABLE_TYPE_ID = 0;

        /** <summary>Constructs client node metrics bean.</summary> */
        public GridClientNodeMetricsBean() {
        }

        public long lastUpdateTime { get; set; }

        /** */
        public int maxActiveJobs { get; set; }

        /** */
        public int curActiveJobs { get; set; }

        /** */
        public float avgActiveJobs { get; set; }

        /** */
        public int maxWaitingJobs { get; set; }

        /** */
        public int curWaitingJobs { get; set; }

        /** */
        public float avgWaitingJobs { get; set; }

        /** */
        public int maxRejectedJobs { get; set; }

        /** */
        public int curRejectedJobs { get; set; }

        /** */
        public float avgRejectedJobs { get; set; }

        /** */
        public int maxCancelledJobs { get; set; }

        /** */
        public int curCancelledJobs { get; set; }

        /** */
        public float avgCancelledJobs { get; set; }

        /** */
        public int totalRejectedJobs { get; set; }

        /** */
        public int totalCancelledJobs { get; set; }

        /** */
        public int totalExecutedJobs { get; set; }

        /** */
        public long maxJobWaitTime { get; set; }

        /** */
        public long curJobWaitTime { get; set; }

        /** */
        public double avgJobWaitTime { get; set; }

        /** */
        public long maxJobExecTime { get; set; }

        /** */
        public long curJobExecTime { get; set; }

        /** */
        public double avgJobExecTime { get; set; }

        /** */
        public int totalExecTasks { get; set; }

        /** */
        public long totalIdleTime { get; set; }

        /** */
        public long curIdleTime { get; set; }

        /** */
        public int availProcs { get; set; }

        /** */
        public double load { get; set; }

        /** */
        public double avgLoad { get; set; }

        /** */
        public double gcLoad { get; set; }

        /** */
        public long heapInit { get; set; }

        /** */
        public long heapUsed { get; set; }

        /** */
        public long heapCommitted { get; set; }

        /** */
        public long heapMax { get; set; }

        /** */
        public long nonHeapInit { get; set; }

        /** */
        public long nonHeapUsed { get; set; }

        /** */
        public long nonHeapCommitted { get; set; }

        /** */
        public long nonHeapMax { get; set; }

        /** */
        public long upTime { get; set; }

        /** */
        public long startTime { get; set; }

        /** */
        public long nodeStartTime { get; set; }

        /** */
        public int threadCnt { get; set; }

        /** */
        public int peakThreadCnt { get; set; }

        /** */
        public long startedThreadCnt { get; set; }

        /** */
        public int daemonThreadCnt { get; set; }

        /** */
        public long fileSysFreeSpace { get; set; }

        /** */
        public long fileSysTotalSpace { get; set; }

        /** */
        public long fileSysUsableSpace { get; set; }

        /** */
        public long lastDataVer { get; set; }

        /** */
        public int sentMsgsCnt { get; set; }

        /** */
        public long sentBytesCnt { get; set; }

        /** */
        public int rcvdMsgsCnt { get; set; }

        /** */
        public long rcvdBytesCnt { get; set; }

        /** <inheritdoc /> */
        public void WritePortable(IGridClientPortableWriter writer)
        {
            IGridClientPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteLong(lastUpdateTime);
            rawWriter.WriteInt(maxActiveJobs);
            rawWriter.WriteInt(curActiveJobs);
            rawWriter.WriteFloat(avgActiveJobs);
            rawWriter.WriteInt(maxWaitingJobs);
            rawWriter.WriteInt(curWaitingJobs);
            rawWriter.WriteFloat(avgWaitingJobs);
            rawWriter.WriteInt(maxRejectedJobs);
            rawWriter.WriteInt(curRejectedJobs);
            rawWriter.WriteFloat(avgRejectedJobs);
            rawWriter.WriteInt(maxCancelledJobs);
            rawWriter.WriteInt(curCancelledJobs);
            rawWriter.WriteFloat(avgCancelledJobs);
            rawWriter.WriteInt(totalRejectedJobs);
            rawWriter.WriteInt(totalCancelledJobs);
            rawWriter.WriteInt(totalExecutedJobs);
            rawWriter.WriteLong(maxJobWaitTime);
            rawWriter.WriteLong(curJobWaitTime);
            rawWriter.WriteDouble(avgJobWaitTime);
            rawWriter.WriteLong(maxJobExecTime);
            rawWriter.WriteLong(curJobExecTime);
            rawWriter.WriteDouble(avgJobExecTime);
            rawWriter.WriteInt(totalExecTasks);
            rawWriter.WriteLong(totalIdleTime);
            rawWriter.WriteLong(curIdleTime);
            rawWriter.WriteInt(availProcs);
            rawWriter.WriteDouble(load);
            rawWriter.WriteDouble(avgLoad);
            rawWriter.WriteDouble(gcLoad);
            rawWriter.WriteLong(heapInit);
            rawWriter.WriteLong(heapUsed);
            rawWriter.WriteLong(heapCommitted);
            rawWriter.WriteLong(heapMax);
            rawWriter.WriteLong(nonHeapInit);
            rawWriter.WriteLong(nonHeapUsed);
            rawWriter.WriteLong(nonHeapCommitted);
            rawWriter.WriteLong(nonHeapMax);
            rawWriter.WriteLong(upTime);
            rawWriter.WriteLong(startTime);
            rawWriter.WriteLong(nodeStartTime);
            rawWriter.WriteInt(threadCnt);
            rawWriter.WriteInt(peakThreadCnt);
            rawWriter.WriteLong(startedThreadCnt);
            rawWriter.WriteInt(daemonThreadCnt);
            rawWriter.WriteLong(fileSysFreeSpace);
            rawWriter.WriteLong(fileSysTotalSpace);
            rawWriter.WriteLong(fileSysUsableSpace);
            rawWriter.WriteLong(lastDataVer);
            rawWriter.WriteInt(sentMsgsCnt);
            rawWriter.WriteLong(sentBytesCnt);
            rawWriter.WriteInt(rcvdMsgsCnt);
            rawWriter.WriteLong(rcvdBytesCnt);
        }

        /** <inheritdoc /> */
        public void ReadPortable(IGridClientPortableReader reader)
        {
            IGridClientPortableRawReader rawReader = reader.RawReader();

            lastUpdateTime = rawReader.ReadLong();
            maxActiveJobs = rawReader.ReadInt();
            curActiveJobs = rawReader.ReadInt();
            avgActiveJobs = rawReader.ReadFloat();
            maxWaitingJobs = rawReader.ReadInt();
            curWaitingJobs = rawReader.ReadInt();
            avgWaitingJobs = rawReader.ReadFloat();
            maxRejectedJobs = rawReader.ReadInt();
            curRejectedJobs = rawReader.ReadInt();
            avgRejectedJobs = rawReader.ReadFloat();
            maxCancelledJobs = rawReader.ReadInt();
            curCancelledJobs = rawReader.ReadInt();
            avgCancelledJobs = rawReader.ReadFloat();
            totalRejectedJobs = rawReader.ReadInt();
            totalCancelledJobs = rawReader.ReadInt();
            totalExecutedJobs = rawReader.ReadInt();
            maxJobWaitTime = rawReader.ReadLong();
            curJobWaitTime = rawReader.ReadLong();
            avgJobWaitTime = rawReader.ReadDouble();
            maxJobExecTime = rawReader.ReadLong();
            curJobExecTime = rawReader.ReadLong();
            avgJobExecTime = rawReader.ReadDouble();
            totalExecTasks = rawReader.ReadInt();
            totalIdleTime = rawReader.ReadLong();
            curIdleTime = rawReader.ReadLong();
            availProcs = rawReader.ReadInt();
            load = rawReader.ReadDouble();
            avgLoad = rawReader.ReadDouble();
            gcLoad = rawReader.ReadDouble();
            heapInit = rawReader.ReadLong();
            heapUsed = rawReader.ReadLong();
            heapCommitted = rawReader.ReadLong();
            heapMax = rawReader.ReadLong();
            nonHeapInit = rawReader.ReadLong();
            nonHeapUsed = rawReader.ReadLong();
            nonHeapCommitted = rawReader.ReadLong();
            nonHeapMax = rawReader.ReadLong();
            upTime = rawReader.ReadLong();
            startTime = rawReader.ReadLong();
            nodeStartTime = rawReader.ReadLong();
            threadCnt = rawReader.ReadInt();
            peakThreadCnt = rawReader.ReadInt();
            startedThreadCnt = rawReader.ReadLong();
            daemonThreadCnt = rawReader.ReadInt();
            fileSysFreeSpace = rawReader.ReadLong();
            fileSysTotalSpace = rawReader.ReadLong();
            fileSysUsableSpace = rawReader.ReadLong();
            lastDataVer = rawReader.ReadLong();
            sentMsgsCnt = rawReader.ReadInt();
            sentBytesCnt = rawReader.ReadLong();
            rcvdMsgsCnt = rawReader.ReadInt();
            rcvdBytesCnt = rawReader.ReadLong();
        }
    }
}
