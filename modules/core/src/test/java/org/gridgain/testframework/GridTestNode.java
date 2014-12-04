/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.testframework;

import org.apache.ignite.cluster.*;
import org.apache.ignite.product.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.util.lang.*;

import java.util.*;
import java.util.concurrent.atomic.*;

import static org.apache.ignite.product.IgniteProductVersion.*;

/**
 * Test node.
 */
public class GridTestNode extends GridMetadataAwareAdapter implements ClusterNode {
    /** */
    private static final IgniteProductVersion VERSION = fromString("99.99.99");

    /** */
    private static final AtomicInteger consistentIdCtr = new AtomicInteger();

    /** */
    private String addr;

    /** */
    private String hostName;

    /** */
    private Map<String, Object> attrs = new HashMap<>();

    /** */
    private UUID id;

    /** */
    private Object consistentId = consistentIdCtr.incrementAndGet();

    /** */
    private ClusterNodeMetrics metrics;

    /** */
    public GridTestNode() {
        // No-op.

        initAttributes();
    }

    /**
     * @param id Node ID.
     */
    public GridTestNode(UUID id) {
        this.id = id;

        initAttributes();
    }

    /** */
    private void initAttributes() {
        attrs.put(GridNodeAttributes.ATTR_BUILD_VER, "10");
        attrs.put(GridNodeAttributes.ATTR_GRID_NAME, "null");
    }

    /**
     * @param id Node ID.
     * @param metrics Node metrics.
     */
    public GridTestNode(UUID id, ClusterNodeMetrics metrics) {
        this.id = id;
        this.metrics = metrics;

        initAttributes();
    }

    /** {@inheritDoc} */
    @Override public UUID id() {
        assert id != null;

        return id;
    }

    /** {@inheritDoc} */
    @Override public Object consistentId() {
        return consistentId;
    }

    /**
     * @param addr Address.
     */
    public void setPhysicalAddress(String addr) {
        this.addr = addr;
    }

    /**
     * @param hostName Host name.
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /** {@inheritDoc} */
    @Override @SuppressWarnings("unchecked")
    public <T> T attribute(String name) {
        assert name != null;

        return (T)attrs.get(name);
    }

    /**
     * @param name Name.
     * @param val Value.
     */
    public void addAttribute(String name, Object val) {
        attrs.put(name, val);
    }

    /**
     * @param id ID.
     */
    public void setId(UUID id) {
        assert id != null;

        this.id = id;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public Map<String, Object> attributes() {
        return attrs;
    }

    /** {@inheritDoc} */
    @Override public Collection<String> addresses() {
        return Collections.singletonList(addr);
    }

    /** {@inheritDoc} */
    @Override public Collection<String> hostNames() {
        return Collections.singletonList(hostName);
    }

    /**
     * @param key Attribute key.
     * @param val Attribute value.
     */
    public void setAttribute(String key, Object val) {
        attrs.put(key, val);
    }

    /**
     * @param key Attribute key.
     * @return Removed value.
     */
    public Object removeAttribute(String key) {
        return attrs.remove(key);
    }

    /**
     * @param attrs Attributes.
     */
    public void setAttributes(Map<String, Object> attrs) {
        this.attrs.putAll(attrs);
    }

    /** {@inheritDoc} */
    @Override public ClusterNodeMetrics metrics() {
        return metrics;
    }

    /** {@inheritDoc} */
    @Override public long order() {
        return metrics == null ? -1 : metrics.getStartTime();
    }

    /** {@inheritDoc} */
    @Override public IgniteProductVersion version() {
        return VERSION;
    }

    /**
     * Sets node metrics.
     *
     * @param metrics Node metrics.
     */
    public void setMetrics(ClusterNodeMetrics metrics) {
        this.metrics = metrics;
    }

    /** {@inheritDoc} */
    @Override public boolean isLocal() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isDaemon() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean isClient() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        return id.hashCode();
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object obj) {
        assert obj instanceof ClusterNode;

        return ((ClusterNode) obj).id().equals(id);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return id.toString();
//        StringBuilder buf = new StringBuilder();
//
//        buf.append(getClass().getSimpleName());
//        buf.append(" [attrs=").append(attrs);
//        buf.append(", id=").append(id);
//        buf.append(']');
//
//        return buf.toString();
    }
}
