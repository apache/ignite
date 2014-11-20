/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.gui.dto;

import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.timeout.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Holder class to store information in node local map between data collector task executions.
 */
public class VisorComputeMonitoringHolder {
    /** Task monitoring events holder key. */
    public static final String COMPUTE_MONITORING_HOLDER_KEY = "VISOR_COMPUTE_MONITORING_KEY";

    /** Visors that collect events (Visor instance key -> collect events since last cleanup check) */
    private final Map<String, Boolean> listenVisor = new HashMap<>();

    /** If cleanup process not scheduled. */
    private boolean cleanupStopped = true;

    /** Timeout between disable events check. */
    protected static final int CLEANUP_TIMEOUT = 2 * 60 * 1000;

    /**
     * Start collect events for Visor instance.
     *
     * @param g grid.
     * @param visorKey unique Visor instance key.
     */
    public void startCollect(GridEx g, String visorKey) {
        synchronized(listenVisor) {
            if (cleanupStopped) {
                scheduleCleanupJob(g);

                cleanupStopped = false;
            }

            listenVisor.put(visorKey, true);

            g.events().enableLocal(VISOR_TASK_EVTS);
        }
    }

    /**
     * Check if collect events may be disable.
     * @param g grid.
     * @return {@code true} if task events should remain enabled.
     */
    private boolean tryDisableEvents(GridEx g) {
        if (!listenVisor.values().contains(true)) {
            listenVisor.clear();

            g.events().disableLocal(VISOR_TASK_EVTS);
        }

        // Return actual state. It could stay the same if events explicitly enabled in configuration.
        return g.allEventsUserRecordable(VISOR_TASK_EVTS);
    }

    /**
     * Disable collect events for Visor instance.
     * @param g grid.
     * @param visorKey uniq Visor instance key.
     */
    public void stopCollect(GridEx g, String visorKey) {
        synchronized(listenVisor) {
            listenVisor.remove(visorKey);

            tryDisableEvents(g);
        }
    }

    /**
     * Schedule cleanup process for events monitoring.
     * @param g grid.
     */
    private void scheduleCleanupJob(final GridEx g) {
        ((GridKernal)g).context().timeout().addTimeoutObject(new GridTimeoutObjectAdapter(CLEANUP_TIMEOUT) {
            @Override public void onTimeout() {
                synchronized(listenVisor) {
                    if (tryDisableEvents(g)) {
                        for (String visorKey : listenVisor.keySet())
                            listenVisor.put(visorKey, false);

                        scheduleCleanupJob(g);
                    }
                    else
                        cleanupStopped = true;
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorComputeMonitoringHolder.class, this);
    }
}
