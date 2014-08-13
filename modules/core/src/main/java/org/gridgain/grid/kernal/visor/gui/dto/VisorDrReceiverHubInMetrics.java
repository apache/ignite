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
import org.gridgain.grid.kernal.processors.dr.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for {@link GridDrReceiverHubInMetrics}.
 */
public class VisorDrReceiverHubInMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Amount of batches received. */
    private int batchesReceived;

    /** Amount of cache entries received. */
    private long entriesReceived;

    /** Total amount of bytes received. */
    private long bytesReceived;

    /**
     * @param m  DR receiver hub metrics for incoming data.
     * @return Data transfer object for given metrics.
     */
    public static VisorDrReceiverHubInMetrics from(GridDrReceiverHubInMetrics m) {
        assert m != null;

        VisorDrReceiverHubInMetrics metrics = new VisorDrReceiverHubInMetrics();

        metrics.batchesReceived(m.batchesReceived());
        metrics.entriesReceived(m.entriesReceived());
        metrics.bytesReceived(m.bytesReceived());

        return metrics;
    }

    /**
     * @param g Grid to take DR metrics from.
     * @return Data transfer object for DR receiver hub incoming data data aggregated metrics.
     */
    public static VisorDrReceiverHubInMetrics aggregated(Grid g) {
        assert g != null;

        GridDr dr = g.dr();

        try {
            return from(dr.receiverHubAggregatedInMetrics());
        }
        catch (IllegalStateException ignored) {
            return null;
        }
    }

    /**
     * @param g Grid to take DR metrics from.
     * @return Map of data transfer object for DR receiver hub incoming data data aggregated metrics for all remote data
     * centers.
     */
    public static Map<Byte, VisorDrReceiverHubInMetrics> map(Grid g) {
        assert g != null;

        GridDr dr = g.dr();

        Map<Byte, VisorDrReceiverHubInMetrics> map = new HashMap<>();

        for (byte rmtDrId = 0; rmtDrId < GridDrUtils.MAX_DATA_CENTERS; rmtDrId++) {
            try {
                GridDrReceiverHubInMetrics m = dr.receiverHubAggregatedInMetrics(rmtDrId);

                // Add to map only non empty metrics.
                if (m.batchesReceived() > 0 || m.entriesReceived() > 0 || m.bytesReceived() > 0) {
                    map.put(rmtDrId, from(m));
                }
            }
            catch (IllegalStateException | IllegalArgumentException ignored) {
                // No-op.
            }
        }

        return map;
    }

    /**
     * @return Amount of batches received.
     */
    public int batchesReceived() {
        return batchesReceived;
    }

    /**
     * @param batchesReceived New amount of batches received.
     */
    public void batchesReceived(int batchesReceived) {
        this.batchesReceived = batchesReceived;
    }

    /**
     * @return Amount of cache entries received.
     */
    public long entriesReceived() {
        return entriesReceived;
    }

    /**
     * @param entriesReceived New amount of cache entries received.
     */
    public void entriesReceived(long entriesReceived) {
        this.entriesReceived = entriesReceived;
    }

    /**
     * @return Total amount of bytes received.
     */
    public long bytesReceived() {
        return bytesReceived;
    }

    /**
     * @param bytesReceived New total amount of bytes received.
     */
    public void bytesReceived(long bytesReceived) {
        this.bytesReceived = bytesReceived;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrReceiverHubInMetrics.class, this);
    }
}
