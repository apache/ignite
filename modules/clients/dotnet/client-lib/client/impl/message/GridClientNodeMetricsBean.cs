/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Message {
    using GridGain.Client.Portable;

    /** <summary>Node metrics bean.</summary> */
    internal class GridClientNodeMetricsBean : IGridClientPortableEx {
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
            writer.WriteLong("lastUpdateTime", lastUpdateTime);
            writer.WriteInt("maxActiveJobs", maxActiveJobs);
            writer.WriteInt("curActiveJobs", curActiveJobs);
            writer.WriteFloat("avgActiveJobs", avgActiveJobs);
            writer.WriteInt("maxWaitingJobs", maxWaitingJobs);
            writer.WriteInt("curWaitingJobs", curWaitingJobs);
            writer.WriteFloat("avgWaitingJobs", avgWaitingJobs);
            writer.WriteInt("maxRejectedJobs", maxRejectedJobs);
            writer.WriteInt("curRejectedJobs", curRejectedJobs);
            writer.WriteFloat("avgRejectedJobs", avgRejectedJobs);
            writer.WriteInt("maxCancelledJobs", maxCancelledJobs);
            writer.WriteInt("curCancelledJobs", curCancelledJobs);
            writer.WriteFloat("avgCancelledJobs", avgCancelledJobs);
            writer.WriteInt("totalRejectedJobs", totalRejectedJobs);
            writer.WriteInt("totalCancelledJobs", totalCancelledJobs);
            writer.WriteInt("totalExecutedJobs", totalExecutedJobs);
            writer.WriteLong("maxJobWaitTime", maxJobWaitTime);
            writer.WriteLong("curJobWaitTime", curJobWaitTime);
            writer.WriteDouble("avgJobWaitTime", avgJobWaitTime);
            writer.WriteLong("maxJobExecTime", maxJobExecTime);
            writer.WriteLong("curJobExecTime", curJobExecTime);
            writer.WriteDouble("avgJobExecTime", avgJobExecTime);
            writer.WriteInt("totalExecTasks", totalExecTasks);
            writer.WriteLong("totalIdleTime", totalIdleTime);
            writer.WriteLong("curIdleTime", curIdleTime);
            writer.WriteInt("availProcs", availProcs);
            writer.WriteDouble("load", load);
            writer.WriteDouble("avgLoad", avgLoad);
            writer.WriteDouble("gcLoad", gcLoad);
            writer.WriteLong("heapInit", heapInit);
            writer.WriteLong("heapUsed", heapUsed);
            writer.WriteLong("heapCommitted", heapCommitted);
            writer.WriteLong("heapMax", heapMax);
            writer.WriteLong("nonHeapInit", nonHeapInit);
            writer.WriteLong("nonHeapUsed", nonHeapUsed);
            writer.WriteLong("nonHeapCommitted", nonHeapCommitted);
            writer.WriteLong("nonHeapMax", nonHeapMax);
            writer.WriteLong("upTime", upTime);
            writer.WriteLong("startTime", startTime);
            writer.WriteLong("nodeStartTime", nodeStartTime);
            writer.WriteInt("threadCnt", threadCnt);
            writer.WriteInt("peakThreadCnt", peakThreadCnt);
            writer.WriteLong("startedThreadCnt", startedThreadCnt);
            writer.WriteInt("daemonThreadCnt", daemonThreadCnt);
            writer.WriteLong("fileSysFreeSpace", fileSysFreeSpace);
            writer.WriteLong("fileSysTotalSpace", fileSysTotalSpace);
            writer.WriteLong("fileSysUsableSpace", fileSysUsableSpace);
            writer.WriteLong("lastDataVer", lastDataVer);
            writer.WriteInt("sentMsgsCnt", sentMsgsCnt);
            writer.WriteLong("sentBytesCnt", sentBytesCnt);
            writer.WriteInt("rcvdMsgsCnt", rcvdMsgsCnt);
            writer.WriteLong("rcvdBytesCnt", rcvdBytesCnt);
        }

        /** <inheritdoc /> */
        public void ReadPortable(IGridClientPortableReader reader)
        {
            lastUpdateTime = reader.ReadLong("lastUpdateTime");
            maxActiveJobs = reader.ReadInt("maxActiveJobs");
            curActiveJobs = reader.ReadInt("curActiveJobs");
            avgActiveJobs = reader.ReadFloat("avgActiveJobs");
            maxWaitingJobs = reader.ReadInt("maxWaitingJobs");
            curWaitingJobs = reader.ReadInt("curWaitingJobs");
            avgWaitingJobs = reader.ReadFloat("avgWaitingJobs");
            maxRejectedJobs = reader.ReadInt("maxRejectedJobs");
            curRejectedJobs = reader.ReadInt("curRejectedJobs");
            avgRejectedJobs = reader.ReadFloat("avgRejectedJobs");
            maxCancelledJobs = reader.ReadInt("maxCancelledJobs");
            curCancelledJobs = reader.ReadInt("curCancelledJobs");
            avgCancelledJobs = reader.ReadFloat("avgCancelledJobs");
            totalRejectedJobs = reader.ReadInt("totalRejectedJobs");
            totalCancelledJobs = reader.ReadInt("totalCancelledJobs");
            totalExecutedJobs = reader.ReadInt("totalExecutedJobs");
            maxJobWaitTime = reader.ReadLong("maxJobWaitTime");
            curJobWaitTime = reader.ReadLong("curJobWaitTime");
            avgJobWaitTime = reader.ReadDouble("avgJobWaitTime");
            maxJobExecTime = reader.ReadLong("maxJobExecTime");
            curJobExecTime = reader.ReadLong("curJobExecTime");
            avgJobExecTime = reader.ReadDouble("avgJobExecTime");
            totalExecTasks = reader.ReadInt("totalExecTasks");
            totalIdleTime = reader.ReadLong("totalIdleTime");
            curIdleTime = reader.ReadLong("curIdleTime");
            availProcs = reader.ReadInt("availProcs");
            load = reader.ReadDouble("load");
            avgLoad = reader.ReadDouble("avgLoad");
            gcLoad = reader.ReadDouble("gcLoad");
            heapInit = reader.ReadLong("heapInit");
            heapUsed = reader.ReadLong("heapUsed");
            heapCommitted = reader.ReadLong("heapCommitted");
            heapMax = reader.ReadLong("heapMax");
            nonHeapInit = reader.ReadLong("nonHeapInit");
            nonHeapUsed = reader.ReadLong("nonHeapUsed");
            nonHeapCommitted = reader.ReadLong("nonHeapCommitted");
            nonHeapMax = reader.ReadLong("nonHeapMax");
            upTime = reader.ReadLong("upTime");
            startTime = reader.ReadLong("startTime");
            nodeStartTime = reader.ReadLong("nodeStartTime");
            threadCnt = reader.ReadInt("threadCnt");
            peakThreadCnt = reader.ReadInt("peakThreadCnt");
            startedThreadCnt = reader.ReadLong("startedThreadCnt");
            daemonThreadCnt = reader.ReadInt("daemonThreadCnt");
            fileSysFreeSpace = reader.ReadLong("fileSysFreeSpace");
            fileSysTotalSpace = reader.ReadLong("fileSysTotalSpace");
            fileSysUsableSpace = reader.ReadLong("fileSysUsableSpace");
            lastDataVer = reader.ReadLong("lastDataVer");
            sentMsgsCnt = reader.ReadInt("sentMsgsCnt");
            sentBytesCnt = reader.ReadLong("sentBytesCnt");
            rcvdMsgsCnt = reader.ReadInt("rcvdMsgsCnt");
            rcvdBytesCnt = reader.ReadLong("rcvdBytesCnt");
        }
    }
}
