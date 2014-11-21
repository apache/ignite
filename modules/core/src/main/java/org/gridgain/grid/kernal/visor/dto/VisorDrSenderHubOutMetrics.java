/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.dto;

import org.gridgain.grid.*;
import org.gridgain.grid.dr.*;
import org.gridgain.grid.dr.hub.sender.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for {@link GridDrSenderHubOutMetrics}.
 */
public class VisorDrSenderHubOutMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Amount of batches sent to given replica. */
    private int batchesSent;

    /** Amount of entries sent to given replica. */
    private long entriesSent;

    /** Amount of bytes sent to given replica. */
    private long bytesSent;

    /** Amount of sent batches with received acknowledgement from given replica. */
    private int batchesAcked;

    /** Amount of sent entries with received acknowledgement from given replica. */
    private long entriesAcked;

    /** Amount of sent bytes with received acknowledgement from given replica. */
    private long bytesAcked;

    /** Average time in milliseconds between sending batch for the first time and receiving acknowledgement for it. */
    private double avgBatchAckTime;

    /**
     * @param m Data center replication sender hub remote metrics.
     * @return Data transfer object for {@link GridDrSenderHubOutMetrics}.
     */
    public static VisorDrSenderHubOutMetrics from(GridDrSenderHubOutMetrics m) {
        assert m != null;

        VisorDrSenderHubOutMetrics metrics = new VisorDrSenderHubOutMetrics();

        metrics.batchesSent(m.batchesSent());
        metrics.entriesSent(m.entriesSent());
        metrics.bytesSent(m.bytesSent());

        metrics.bytesAcked(m.batchesAcked());
        metrics.entriesAcked(m.entriesAcked());
        metrics.bytesAcked(m.bytesAcked());

        metrics.averageBatchAckTime(m.averageBatchAckTime());

        return metrics;
    }

    /**
     * @param g Grid to take DR metrics.
     * @return Data transfer object for sender hub outgoing aggregated metrics.
     */
    public static VisorDrSenderHubOutMetrics aggregated(Grid g) {
        assert g != null;

        GridDr dr = g.dr();

        try {
            return from(dr.senderHubAggregatedOutMetrics());
        }
        catch (IllegalStateException ignored) {
            return null;
        }
    }

    /**
     * @param g Grid to take DR metrics.
     * @return Map with data transfer object for sender hub outgoing metrics for all configured remote data centers.
     */
    public static Map<Byte, VisorDrSenderHubOutMetrics> map(Grid g) {
        assert g != null;

        GridDr dr = g.dr();

        GridDrSenderHubConfiguration cfg = g.configuration().getDrSenderHubConfiguration();

        Map<Byte, VisorDrSenderHubOutMetrics> map = new HashMap<>();

        if (cfg != null) {
            for (GridDrSenderHubConnectionConfiguration c : cfg.getConnectionConfiguration()) {
                byte rmtDrId = c.getDataCenterId();

                try {
                    map.put(rmtDrId, from(dr.senderHubAggregatedOutMetrics(rmtDrId)));
                }
                catch (IllegalStateException | IllegalArgumentException ignored) {
                    // No-op.
                }
            }
        }

        return map;
    }

    /**
     * @return Amount of batches sent to given replica.
     */
    public int batchesSent() {
        return batchesSent;
    }

    /**
     * @param batchesSent New amount of batches sent to given replica.
     */
    public void batchesSent(int batchesSent) {
        this.batchesSent = batchesSent;
    }

    /**
     * @return Amount of entries sent to given replica.
     */
    public long entriesSent() {
        return entriesSent;
    }

    /**
     * @param entriesSent New amount of entries sent to given replica.
     */
    public void entriesSent(long entriesSent) {
        this.entriesSent = entriesSent;
    }

    /**
     * @return Amount of bytes sent to given replica.
     */
    public long bytesSent() {
        return bytesSent;
    }

    /**
     * @param bytesSent New amount of bytes sent to given replica.
     */
    public void bytesSent(long bytesSent) {
        this.bytesSent = bytesSent;
    }

    /**
     * @return Amount of sent batches with received acknowledgement from given replica.
     */
    public int batchesAcked() {
        return batchesAcked;
    }

    /**
     * @param batchesAcked New amount of sent batches with received acknowledgement from given replica.
     */
    public void batchesAcked(int batchesAcked) {
        this.batchesAcked = batchesAcked;
    }

    /**
     * @return Amount of sent entries with received acknowledgement from given replica.
     */
    public long entriesAcked() {
        return entriesAcked;
    }

    /**
     * @param entriesAcked New amount of sent entries with received acknowledgement from given replica.
     */
    public void entriesAcked(long entriesAcked) {
        this.entriesAcked = entriesAcked;
    }

    /**
     * @return Amount of sent bytes with received acknowledgement from given replica.
     */
    public long bytesAcked() {
        return bytesAcked;
    }

    /**
     * @param bytesAcked New amount of sent bytes with received acknowledgement from given replica.
     */
    public void bytesAcked(long bytesAcked) {
        this.bytesAcked = bytesAcked;
    }

    /**
     * @return Average time in milliseconds between sending batch for the first time and receiving acknowledgement for
     * it.
     */
    public double averageBatchAckTime() {
        return avgBatchAckTime;
    }

    /**
     * @param avgBatchAckTime New average time in milliseconds between sending batch for the first time and receiving
     * acknowledgement for it.
     */
    public void averageBatchAckTime(double avgBatchAckTime) {
        this.avgBatchAckTime = avgBatchAckTime;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrSenderHubOutMetrics.class, this);
    }
}
