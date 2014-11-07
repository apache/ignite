/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.dto;

import org.gridgain.grid.*;
import org.gridgain.grid.dr.*;
import org.gridgain.grid.dr.hub.receiver.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;

/**
 * Data transfer object for {@link GridDrReceiverHubOutMetrics}.
 */
public class VisorDrReceiverHubOutMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Amount of batches sent to caches. */
    private int batchesSent;

    /** Amount of cache entries sent to caches. */
    private long entriesSent;

    /** Amount of bytes sent to caches. */
    private long bytesSent;

    /** Amount of batches stored in caches. */
    private int batchesAcked;

    /** Amount of cache entries stored in caches. */
    private long entriesAcked;

    /** Amount of bytes stored in caches. */
    private long bytesAcked;

    /** Average time in milliseconds between sending batch to data nodes and successfully storing it. */
    private double avgBatchAckTime;

    /** Create data transfer object for given metrics. */
    public static VisorDrReceiverHubOutMetrics from(GridDrReceiverHubOutMetrics m) {
        assert m != null;

        VisorDrReceiverHubOutMetrics metrics = new VisorDrReceiverHubOutMetrics();

        metrics.batchesSent(m.batchesSent());
        metrics.entriesSent(m.entriesSent());
        metrics.bytesSent(m.bytesSent());

        metrics.batchesAcked(m.batchesAcked());
        metrics.entriesAcked(m.entriesAcked());
        metrics.bytesAcked(m.bytesAcked());

        metrics.avgBatchAckTime(m.averageBatchAckTime());

        return metrics;
    }

    /** Create aggregated metrics. */
    public static VisorDrReceiverHubOutMetrics aggregated(Grid g) {
        assert g != null;

        GridDr dr = g.dr();

        try {
            return from(dr.receiverHubAggregatedOutMetrics());
        }
        catch (IllegalStateException ignored) {
            return null;
        }
    }

    /**
     * @return Amount of batches sent to caches.
     */
    public int batchesSent() {
        return batchesSent;
    }

    /**
     * @param batchesSent New amount of batches sent to caches.
     */
    public void batchesSent(int batchesSent) {
        this.batchesSent = batchesSent;
    }

    /**
     * @return Amount of cache entries sent to caches.
     */
    public long entriesSent() {
        return entriesSent;
    }

    /**
     * @param entriesSent New amount of cache entries sent to caches.
     */
    public void entriesSent(long entriesSent) {
        this.entriesSent = entriesSent;
    }

    /**
     * @return Amount of bytes sent to caches.
     */
    public long bytesSent() {
        return bytesSent;
    }

    /**
     * @param bytesSent New amount of bytes sent to caches.
     */
    public void bytesSent(long bytesSent) {
        this.bytesSent = bytesSent;
    }

    /**
     * @return Amount of batches stored in caches.
     */
    public int batchesAcked() {
        return batchesAcked;
    }

    /**
     * @param batchesAcked New amount of batches stored in caches.
     */
    public void batchesAcked(int batchesAcked) {
        this.batchesAcked = batchesAcked;
    }

    /**
     * @return Amount of cache entries stored in caches.
     */
    public long entriesAcked() {
        return entriesAcked;
    }

    /**
     * @param entriesAcked New amount of cache entries stored in caches.
     */
    public void entriesAcked(long entriesAcked) {
        this.entriesAcked = entriesAcked;
    }

    /**
     * @return Amount of bytes stored in caches.
     */
    public long bytesAcked() {
        return bytesAcked;
    }

    /**
     * @param bytesAcked New amount of bytes stored in caches.
     */
    public void bytesAcked(long bytesAcked) {
        this.bytesAcked = bytesAcked;
    }

    /**
     * @return Average time in milliseconds between sending batch to data nodes and successfully storing it.
     */
    public double averageBatchAckTime() {
        return avgBatchAckTime;
    }

    /**
     * @param avgBatchAckTime New average time in milliseconds between sending batch to data nodes and successfully
     * storing it.
     */
    public void avgBatchAckTime(double avgBatchAckTime) {
        this.avgBatchAckTime = avgBatchAckTime;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrReceiverHubOutMetrics.class, this);
    }
}
