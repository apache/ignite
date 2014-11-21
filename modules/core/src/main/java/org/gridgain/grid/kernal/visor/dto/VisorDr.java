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
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Data transfer object for {@link GridDr}.
 */
public class VisorDr implements Serializable {
    /** Maximum amount of data centers. */
    // TODO 9341: use GridDrUtils.MAX_DATA_CENTERS.
    static final int MAX_DATA_CENTERS = 32;

    /** */
    private static final long serialVersionUID = 0L;

    /** Data center ID this grid belongs to. */
    private byte id;

    /** Map with sender hub in metrics for each cache this sender hub works with. */
    private Map<String, VisorDrSenderHubInMetrics> sndHubInMetrics;

    /** Aggregated sender hub in metrics. */
    private VisorDrSenderHubInMetrics sndHubAggInMetrics;

    /** Map with sender hub out metrics for each remote data center. */
    private Map<Byte, VisorDrSenderHubOutMetrics> sndHubOutMetrics;

    /** Aggregated sender hub out metrics. */
    private VisorDrSenderHubOutMetrics sndHubAggOutMetrics;

    /** Map with receiver hub in metrics for each remote data center. */
    private Map<Byte, VisorDrReceiverHubInMetrics> rcvHubInMetrics;

    /** Aggregated receiver hub in metrics. */
    private VisorDrReceiverHubInMetrics rcvHubAggInMetrics;

    /** Receiver hub out metrics. */
    private VisorDrReceiverHubOutMetrics rcvHubOutMetrics;

    /**
     * @param g Grid.
     * @return Data transfer object for DR of given grid.
     */
    public static VisorDr from(Grid g) {
        assert g != null;

        VisorDr dr = new VisorDr();

        dr.id(g.configuration().getDataCenterId());
        dr.senderHubInMetrics(VisorDrSenderHubInMetrics.map(g));
        dr.senderHubAggregatedInMetrics(VisorDrSenderHubInMetrics.aggregated(g));
        dr.senderHubOutMetrics(VisorDrSenderHubOutMetrics.map(g));
        dr.senderHubAggregatedOutMetrics(VisorDrSenderHubOutMetrics.aggregated(g));
        dr.receiverHubInMetrics(VisorDrReceiverHubInMetrics.map(g));
        dr.receiverHubAggregatedInMetrics(VisorDrReceiverHubInMetrics.aggregated(g));
        dr.receiverHubOutMetrics(VisorDrReceiverHubOutMetrics.aggregated(g));

        return dr;
    }

    /**
     * @return Data center ID this grid belongs to.
     */
    public byte id() {
        return id;
    }

    /**
     * @param id New data center ID this grid belongs to.
     */
    public void id(byte id) {
        this.id = id;
    }

    /**
     * @return Map with sender hub in metrics for each cache this sender hub works with.
     */
    public Map<String, VisorDrSenderHubInMetrics> senderHubInMetrics() {
        return sndHubInMetrics;
    }

    /**
     * @param sndHubInMetrics New map with sender hub in metrics for each cache this sender hub works with.
     */
    public void senderHubInMetrics(Map<String, VisorDrSenderHubInMetrics> sndHubInMetrics) {
        this.sndHubInMetrics = sndHubInMetrics;
    }

    /**
     * @return Aggregated sender hub in metrics.
     */
    public VisorDrSenderHubInMetrics senderHubAggregatedInMetrics() {
        return sndHubAggInMetrics;
    }

    /**
     * @param sndHubAggInMetrics New aggregated sender hub in metrics.
     */
    public void senderHubAggregatedInMetrics(VisorDrSenderHubInMetrics sndHubAggInMetrics) {
        this.sndHubAggInMetrics = sndHubAggInMetrics;
    }

    /**
     * @return Map with sender hub out metrics for each remote data center.
     */
    public Map<Byte, VisorDrSenderHubOutMetrics> senderHubOutMetrics() {
        return sndHubOutMetrics;
    }

    /**
     * @param sndHubOutMetrics New map with sender hub out metrics for each remote data center.
     */
    public void senderHubOutMetrics(Map<Byte, VisorDrSenderHubOutMetrics> sndHubOutMetrics) {
        this.sndHubOutMetrics = sndHubOutMetrics;
    }

    /**
     * @return Aggregated sender hub out metrics.
     */
    public VisorDrSenderHubOutMetrics senderHubAggregated() {
        return sndHubAggOutMetrics;
    }

    /**
     * @param sndHubAggOutMetrics New aggregated sender hub out metrics.
     */
    public void senderHubAggregatedOutMetrics(VisorDrSenderHubOutMetrics sndHubAggOutMetrics) {
        this.sndHubAggOutMetrics = sndHubAggOutMetrics;
    }

    /**
     * @return Map with receiver hub in metrics for each remote data center.
     */
    public Map<Byte, VisorDrReceiverHubInMetrics> receiverHubInMetrics() {
        return rcvHubInMetrics;
    }

    /**
     * @param rcvHubInMetrics New map with receiver hub in metrics for each remote data center.
     */
    public void receiverHubInMetrics(Map<Byte, VisorDrReceiverHubInMetrics> rcvHubInMetrics) {
        this.rcvHubInMetrics = rcvHubInMetrics;
    }

    /**
     * @return Aggregated receiver hub in metrics.
     */
    public VisorDrReceiverHubInMetrics receiverHubAggregatedInMetrics() {
        return rcvHubAggInMetrics;
    }

    /**
     * @param rcvHubAggInMetrics New aggregated receiver hub in metrics.
     */
    public void receiverHubAggregatedInMetrics(VisorDrReceiverHubInMetrics rcvHubAggInMetrics) {
        this.rcvHubAggInMetrics = rcvHubAggInMetrics;
    }

    /**
     * @return Receiver hub out metrics.
     */
    public VisorDrReceiverHubOutMetrics receiverHubOutMetrics() {
        return rcvHubOutMetrics;
    }

    /**
     * @param rcvHubOutMetrics New receiver hub out metrics.
     */
    public void receiverHubOutMetrics(VisorDrReceiverHubOutMetrics rcvHubOutMetrics) {
        this.rcvHubOutMetrics = rcvHubOutMetrics;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorDr.class, this);
    }
}
