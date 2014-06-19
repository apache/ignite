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
            writer.WriteLong(lastUpdateTime);
            writer.WriteInt(maxActiveJobs);
            writer.WriteInt(curActiveJobs);
            writer.WriteFloat(avgActiveJobs);
            writer.WriteInt(maxWaitingJobs);
            writer.WriteInt(curWaitingJobs);
            writer.WriteFloat(avgWaitingJobs);
            writer.WriteInt(maxRejectedJobs);
            writer.WriteInt(curRejectedJobs);
            writer.WriteFloat(avgRejectedJobs);
            writer.WriteInt(maxCancelledJobs);
            writer.WriteInt(curCancelledJobs);
            writer.WriteFloat(avgCancelledJobs);
            writer.WriteInt(totalRejectedJobs);
            writer.WriteInt(totalCancelledJobs);
            writer.WriteInt(totalExecutedJobs);
            writer.WriteLong(maxJobWaitTime);
            writer.WriteLong(curJobWaitTime);
            writer.WriteDouble(avgJobWaitTime);
            writer.WriteLong(maxJobExecTime);
            writer.WriteLong(curJobExecTime);
            writer.WriteDouble(avgJobExecTime);
            writer.WriteInt(totalExecTasks);
            writer.WriteLong(totalIdleTime);
            writer.WriteLong(curIdleTime);
            writer.WriteInt(availProcs);
            writer.WriteDouble(load);
            writer.WriteDouble(avgLoad);
            writer.WriteDouble(gcLoad);
            writer.WriteLong(heapInit);
            writer.WriteLong(heapUsed);
            writer.WriteLong(heapCommitted);
            writer.WriteLong(heapMax);
            writer.WriteLong(nonHeapInit);
            writer.WriteLong(nonHeapUsed);
            writer.WriteLong(nonHeapCommitted);
            writer.WriteLong(nonHeapMax);
            writer.WriteLong(upTime);
            writer.WriteLong(startTime);
            writer.WriteLong(nodeStartTime);
            writer.WriteInt(threadCnt);
            writer.WriteInt(peakThreadCnt);
            writer.WriteLong(startedThreadCnt);
            writer.WriteInt(daemonThreadCnt);
            writer.WriteLong(fileSysFreeSpace);
            writer.WriteLong(fileSysTotalSpace);
            writer.WriteLong(fileSysUsableSpace);
            writer.WriteLong(lastDataVer);
            writer.WriteInt(sentMsgsCnt);
            writer.WriteLong(sentBytesCnt);
            writer.WriteInt(rcvdMsgsCnt);
            writer.WriteLong(rcvdBytesCnt);
        }

        /** <inheritdoc /> */
        public void ReadPortable(IGridClientPortableReader reader)
        {
            lastUpdateTime = reader.ReadLong();
            maxActiveJobs = reader.ReadInt();
            curActiveJobs = reader.ReadInt();
            avgActiveJobs = reader.ReadFloat();
            maxWaitingJobs = reader.ReadInt();
            curWaitingJobs = reader.ReadInt();
            avgWaitingJobs = reader.ReadFloat();
            maxRejectedJobs = reader.ReadInt();
            curRejectedJobs = reader.ReadInt();
            avgRejectedJobs = reader.ReadFloat();
            maxCancelledJobs = reader.ReadInt();
            curCancelledJobs = reader.ReadInt();
            avgCancelledJobs = reader.ReadFloat();
            totalRejectedJobs = reader.ReadInt();
            totalCancelledJobs = reader.ReadInt();
            totalExecutedJobs = reader.ReadInt();
            maxJobWaitTime = reader.ReadLong();
            curJobWaitTime = reader.ReadLong();
            avgJobWaitTime = reader.ReadDouble();
            maxJobExecTime = reader.ReadLong();
            curJobExecTime = reader.ReadLong();
            avgJobExecTime = reader.ReadDouble();
            totalExecTasks = reader.ReadInt();
            totalIdleTime = reader.ReadLong();
            curIdleTime = reader.ReadLong();
            availProcs = reader.ReadInt();
            load = reader.ReadDouble();
            avgLoad = reader.ReadDouble();
            gcLoad = reader.ReadDouble();
            heapInit = reader.ReadLong();
            heapUsed = reader.ReadLong();
            heapCommitted = reader.ReadLong();
            heapMax = reader.ReadLong();
            nonHeapInit = reader.ReadLong();
            nonHeapUsed = reader.ReadLong();
            nonHeapCommitted = reader.ReadLong();
            nonHeapMax = reader.ReadLong();
            upTime = reader.ReadLong();
            startTime = reader.ReadLong();
            nodeStartTime = reader.ReadLong();
            threadCnt = reader.ReadInt();
            peakThreadCnt = reader.ReadInt();
            startedThreadCnt = reader.ReadLong();
            daemonThreadCnt = reader.ReadInt();
            fileSysFreeSpace = reader.ReadLong();
            fileSysTotalSpace = reader.ReadLong();
            fileSysUsableSpace = reader.ReadLong();
            lastDataVer = reader.ReadLong();
            sentMsgsCnt = reader.ReadInt();
            sentBytesCnt = reader.ReadLong();
            rcvdMsgsCnt = reader.ReadInt();
            rcvdBytesCnt = reader.ReadLong();
        }
    }
}
