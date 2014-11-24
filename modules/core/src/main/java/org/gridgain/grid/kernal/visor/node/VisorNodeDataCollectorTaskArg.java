package org.gridgain.grid.kernal.visor.node;

import java.io.*;

/**
 * Data collector task arguments.
 */
public class VisorNodeDataCollectorTaskArg implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Whether task monitoring should be enabled. */
    private boolean taskMonitoringEnabled;

    /** Visor unique key to get last event order from node local storage. */
    private String evtOrderKey;

    /** Visor unique key to get lost events throttle counter from node local storage. */
    private String evtThrottleCntrKey;

    /** cache sample size. */
    private int sample;

    public VisorNodeDataCollectorTaskArg() {
        // No-op.
    }

    /**
     * Create task arguments with given parameters.
     *
     * @param taskMonitoringEnabled Required task monitoring state.
     * @param evtOrderKey Event order key, unique for Visor instance.
     * @param evtThrottleCntrKey Event throttle counter key, unique for Visor instance.
     * @param sample How many entries use in sampling.
     */
    public VisorNodeDataCollectorTaskArg(
        boolean taskMonitoringEnabled,
        String evtOrderKey,
        String evtThrottleCntrKey,
        int sample
    ) {
        this.taskMonitoringEnabled = taskMonitoringEnabled;
        this.evtOrderKey = evtOrderKey;
        this.evtThrottleCntrKey = evtThrottleCntrKey;
        this.sample = sample;
    }

    public boolean taskMonitoringEnabled() {
        return taskMonitoringEnabled;
    }

    public void taskMonitoringEnabled(boolean taskMonitoringEnabled) {
        this.taskMonitoringEnabled = taskMonitoringEnabled;
    }

    public String eventsOrderKey() {
        return evtOrderKey;
    }

    public void eventsOrderKey(String evtOrderKey) {
        this.evtOrderKey = evtOrderKey;
    }

    public String eventsThrottleCntrKey() {
        return evtThrottleCntrKey;
    }

    public void eventsThrottleCntrKey(String evtThrottleCntrKey) {
        this.evtThrottleCntrKey = evtThrottleCntrKey;
    }

    public int sample() {
        return sample;
    }

    public void sample(int sample) {
        this.sample = sample;
    }
}
