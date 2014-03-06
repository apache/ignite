/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.discovery.tcp.metricsstore.vm;

import org.gridgain.grid.*;
import org.gridgain.grid.util.*;
import org.gridgain.grid.spi.*;
import org.gridgain.grid.spi.discovery.tcp.metricsstore.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.util.*;

/**
 * Local JVM-based metrics store.
 * <h1 class="header">Configuration</h1>
 * <h2 class="header">Mandatory</h2>
 * There are no mandatory configuration parameters.
 * <h2 class="header">Optional</h2>
 * <ul>
 *     <li>Metrics expire time (see {@link #setMetricsExpireTime(int)}).</li>
 * </ul>
 */
public class GridTcpDiscoveryVmMetricsStore extends GridTcpDiscoveryMetricsStoreAdapter {
    /** Metrics. */
    private final Map<UUID, GridNodeMetrics> metricsMap = new ConcurrentHashMap8<>();

    /** {@inheritDoc} */
    @Override public void updateLocalMetrics(UUID locNodeId, GridNodeMetrics metrics) throws GridSpiException {
        assert locNodeId != null;
        assert metrics != null;

        metricsMap.put(locNodeId, metrics);
    }

    /** {@inheritDoc} */
    @Override protected Map<UUID, GridNodeMetrics> metrics0(Collection<UUID> nodeIds) {
        assert !F.isEmpty(nodeIds);

        return F.view(metricsMap, F.contains(nodeIds));
    }

    /** {@inheritDoc} */
    @Override protected void removeMetrics0(Collection<UUID> nodeIds) throws GridSpiException {
        assert !F.isEmpty(nodeIds);

        for (UUID id : nodeIds)
            metricsMap.remove(id);
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> allNodeIds() throws GridSpiException {
        return metricsMap.keySet();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTcpDiscoveryVmMetricsStore.class, this);
    }
}
