/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.visor.compute;

import java.util.HashMap;
import java.util.Map;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObjectAdapter;
import org.apache.ignite.internal.util.typedef.internal.S;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.VISOR_TASK_EVTS;

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
     * @param ignite Grid.
     * @param visorKey unique Visor instance key.
     */
    public void startCollect(IgniteEx ignite, String visorKey) {
        synchronized (listenVisor) {
            if (cleanupStopped) {
                scheduleCleanupJob(ignite);

                cleanupStopped = false;
            }

            listenVisor.put(visorKey, Boolean.TRUE);

            ignite.events().enableLocal(VISOR_TASK_EVTS);
        }
    }

    /**
     * Check if collect events may be disable.
     *
     * @param ignite Grid.
     * @return {@code true} if task events should remain enabled.
     */
    private boolean tryDisableEvents(IgniteEx ignite) {
        if (!listenVisor.values().contains(Boolean.TRUE)) {
            listenVisor.clear();

            ignite.events().disableLocal(VISOR_TASK_EVTS);
        }

        // Return actual state. It could stay the same if events explicitly enabled in configuration.
        return ignite.allEventsUserRecordable(VISOR_TASK_EVTS);
    }

    /**
     * Disable collect events for Visor instance.
     *
     * @param g Grid.
     * @param visorKey Unique Visor instance key.
     */
    public void stopCollect(IgniteEx g, String visorKey) {
        synchronized (listenVisor) {
            listenVisor.remove(visorKey);

            tryDisableEvents(g);
        }
    }

    /**
     * Schedule cleanup process for events monitoring.
     *
     * @param ignite grid.
     */
    private void scheduleCleanupJob(final IgniteEx ignite) {
        ignite.context().timeout().addTimeoutObject(new GridTimeoutObjectAdapter(CLEANUP_TIMEOUT) {
            @Override public void onTimeout() {
                synchronized (listenVisor) {
                    if (tryDisableEvents(ignite)) {
                        for (String visorKey : listenVisor.keySet())
                            listenVisor.put(visorKey, Boolean.FALSE);

                        scheduleCleanupJob(ignite);
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
