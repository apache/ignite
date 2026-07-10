package org.apache.ignite.internal.ducktest.utils;

/** Accumulates latency of completed operations. */
public class OpStats {
    /** Sum of recorded latencies, nanoseconds. */
    private long totalNs;

    /** Minimum recorded latency, nanoseconds. */
    private long minNs = Long.MAX_VALUE;

    /** Maximum recorded latency, nanoseconds. */
    private long maxNs;

    /** Number of recorded operations. */
    private long cnt;

    /** Records a single operation latency. */
    public void record(long ns) {
        totalNs += ns;

        minNs = Math.min(minNs, ns);
        maxNs = Math.max(maxNs, ns);

        cnt++;
    }

    /** @return Minimum latency in nanoseconds, or {@code -1} if nothing was recorded. */
    public long minNs() {
        return cnt > 0 ? minNs : -1;
    }

    /** @return Maximum latency in nanoseconds, or {@code -1} if nothing was recorded. */
    public long maxNs() {
        return cnt > 0 ? maxNs : -1;
    }

    /** @return Average latency in nanoseconds, or {@code -1} if nothing was recorded. */
    public double avgNs() {
        return cnt > 0 ? totalNs / (double)cnt : -1;
    }

    /**
     * @return Derived throughput: recorded operations per second of pure operation time,
     * or {@code -1} if nothing was recorded.
     */
    public double tps() {
        return totalNs > 0 ? cnt * 1e9 / totalNs : -1;
    }
}