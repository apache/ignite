/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.managers.discovery;

import org.gridgain.grid.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.tostring.*;

import java.util.*;

import static org.gridgain.grid.kernal.GridNodeAttributes.*;

/**
 * Convenient adapter for {@link GridNodeShadow}.
 *
 * @author @java.author
 * @version @java.version
 */
class GridDiscoveryNodeShadowAdapter extends GridMetadataAwareAdapter implements GridNodeShadow {
    /** Node ID. */
    private UUID id;

    /** Node attributes. */
    @GridToStringExclude
    private Map<String, Object> attrs;

    /** Node addresses. */
    @GridToStringInclude
    private Collection<String> addrs;

    /** Node host names. */
    @GridToStringInclude
    private Collection<String> hostNames;

    /** Node startup order. */
    private long order;

    /** Creation timestamp. */
    private long created;

    /** */
    private boolean daemon;

    /** Last metrics snapshot. */
    @GridToStringExclude
    private GridNodeMetrics lastMetrics;

    /**
     * Creates node shadow adapter.
     *
     * @param node Node.
     */
    GridDiscoveryNodeShadowAdapter(GridNode node) {
        assert node != null;

        created = U.currentTimeMillis();
        id = node.id();
        attrs = Collections.unmodifiableMap(node.attributes());
        addrs = Collections.unmodifiableCollection(node.addresses());
        hostNames = Collections.unmodifiableCollection(node.hostNames());
        order = node.order();
        lastMetrics = node.metrics();
        daemon = "true".equalsIgnoreCase(this.<String>attribute(ATTR_DAEMON));
    }

    /** {@inheritDoc} */
    @Override public boolean isDaemon() {
        return daemon;
    }

    /** {@inheritDoc} */
    @Override public long created() {
        return created;
    }

    /** {@inheritDoc} */
    @Override public GridNodeMetrics lastMetrics() {
        return lastMetrics;
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        return id;
    }

    /** {@inheritDoc} */
    @Override public String gridName() {
        return attribute(ATTR_GRID_NAME);
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"unchecked"})
    @Override public <T> T attribute(String name) {
        return (T)attrs.get(name);
    }

    /** {@inheritDoc} */
    @Override public Map<String, Object> attributes() {
        return attrs;
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return order;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> addresses() {
        return addrs;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        return hostNames;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridDiscoveryNodeShadowAdapter.class, this, "gridName", gridName());
    }
}
