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
import org.gridgain.grid.dr.hub.sender.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for {@link GridDrSenderHubInMetrics}.
 */
public class VisorDrSenderHubInMetrics implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Amount of batches received from given cache. */
    private int batchesRcv;

    /** Amount of cache entries received from given cache. */
    private long entriesRcv;

    /** Amount of bytes received from given cache. */
    private long bytesRcv;

    /** Create data transfer object with given metrics. */
    public static VisorDrSenderHubInMetrics from(GridDrSenderHubInMetrics m) {
        assert m != null;

        VisorDrSenderHubInMetrics metrics = new VisorDrSenderHubInMetrics();

        metrics.batchesReceived(m.batchesReceived());
        metrics.entriesReceived(m.entriesReceived());
        metrics.bytesReceived(m.bytesReceived());

        return metrics;
    }

    /** Create data transfer object for aggregated metrics. */
    public static VisorDrSenderHubInMetrics aggregated(Grid g) {
        assert g != null;

        GridDr dr = g.dr();

        try {
            return from(dr.senderHubAggregatedInMetrics());
        }
        catch (IllegalStateException ignored) {
            return null;
        }
    }

    /** Create data transfer objects for all caches. */
    public static Map<String, VisorDrSenderHubInMetrics> map(Grid g) {
        assert g != null;

        GridDrSenderHubConfiguration cfg = g.configuration().getDrSenderHubConfiguration();

        Map<String, VisorDrSenderHubInMetrics> map = new HashMap<>();

        if (cfg != null) {
            GridDr dr = g.dr();

            for (String cacheName : cfg.getCacheNames()) {
                GridDrSenderHubInMetrics m = dr.senderHubInMetrics(cacheName);

                if (m != null)
                    map.put(cacheName, from(m));
            }
        }

        return map;
    }

    /**
     * @return Amount of batches received from given cache.
     */
    public int batchesReceived() {
        return batchesRcv;
    }

    /**
     * @param batchesRcv New amount of batches received from given cache.
     */
    public void batchesReceived(int batchesRcv) {
        this.batchesRcv = batchesRcv;
    }

    /**
     * @return Amount of cache entries received from given cache.
     */
    public long entriesReceived() {
        return entriesRcv;
    }

    /**
     * @param entriesRcv New amount of cache entries received from given cache.
     */
    public void entriesReceived(long entriesRcv) {
        this.entriesRcv = entriesRcv;
    }

    /**
     * @return Amount of bytes received from given cache.
     */
    public long bytesReceived() {
        return bytesRcv;
    }

    /**
     * @param bytesRcv New amount of bytes received from given cache.
     */
    public void bytesReceived(long bytesRcv) {
        this.bytesRcv = bytesRcv;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDrSenderHubInMetrics.class, this);
    }
}
