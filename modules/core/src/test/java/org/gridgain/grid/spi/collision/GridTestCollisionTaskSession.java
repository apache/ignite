/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.spi.collision;

import org.apache.ignite.compute.*;
import org.apache.ignite.lang.*;
import org.gridgain.grid.*;

import java.util.*;

/**
 * Test collision task session.
 */
public class GridTestCollisionTaskSession implements ComputeTaskSession {
    /** */
    private Integer pri = 0;

    /** */
    private String priAttrKey;

    /** */
    public GridTestCollisionTaskSession() {
        // No-op.
    }

    /**
     * @param pri Priority.
     * @param priAttrKey Priority attribute key.
     */
    public GridTestCollisionTaskSession(int pri, String priAttrKey) {
        assert priAttrKey != null;

        this.pri = pri;
        this.priAttrKey = priAttrKey;
    }

    /** {@inheritDoc} */
    @Override public UUID getTaskNodeId() {
        assert false;

        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> V waitForAttribute(K key, long timeout) {
        assert false : "Not implemented";

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean waitForAttribute(Object key, Object val, long timeout) throws InterruptedException {
        assert false : "Not implemented";

        return false;
    }

    /** {@inheritDoc} */
    @Override public Map<?, ?> waitForAttributes(Collection<?> keys, long timeout) {
        assert false : "Not implemented";

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean waitForAttributes(Map<?, ?> attrs, long timeout) throws InterruptedException {
        assert false : "Not implemented";

        return false;
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state) throws GridException {
        assert false : "Not implemented";
    }

    @Override public void saveCheckpoint(String key, Object state, GridComputeTaskSessionScope scope, long timeout)
        throws GridException {
        assert false : "Not implemented";
    }

    @Override public void saveCheckpoint(String key, Object state, GridComputeTaskSessionScope scope, long timeout,
        boolean overwrite) throws GridException {
        assert false : "Not implemented";
    }

    /** {@inheritDoc} */
    @Override public <T> T loadCheckpoint(String key) throws GridException {
        assert false : "Not implemented";

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean removeCheckpoint(String key) throws GridException {
        assert false : "Not implemented";

        return false;
    }

    /** {@inheritDoc} */
    @Override public String getTaskName() {
        assert false : "Not implemented";

        return null;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid getId() {
        assert false : "Not implemented";

        return null;
    }

    /** {@inheritDoc} */
    @Override public long getEndTime() {
        return Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public ClassLoader getClassLoader() {
        assert false : "Not implemented";

        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<ComputeJobSibling> getJobSiblings() {
        assert false : "Not implemented";

        return null;
    }

    /** {@inheritDoc} */
    @Override public Collection<ComputeJobSibling> refreshJobSiblings() throws GridException {
        return getJobSiblings();
    }

    /** {@inheritDoc} */
    @Override public ComputeJobSibling getJobSibling(IgniteUuid jobId) {
        assert false : "Not implemented";

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(Object key, Object val) {
        assert false : "Not implemented";
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> V getAttribute(K key) {
        if (priAttrKey != null && priAttrKey.equals(key))
            return (V)pri;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setAttributes(Map<?, ?> attrs) {
        assert false : "Not implemented";
    }

    /** {@inheritDoc} */
    @Override public Map<Object, Object> getAttributes() {
        assert false : "Not implemented";

        return null;
    }

    /** {@inheritDoc} */
    @Override public void addAttributeListener(GridComputeTaskSessionAttributeListener lsnr, boolean rewind) {
        assert false : "Not implemented";
    }

    /** {@inheritDoc} */
    @Override public boolean removeAttributeListener(GridComputeTaskSessionAttributeListener lsnr) {
        assert false : "Not implemented";

        return false;
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> mapFuture() {
        assert false : "Not implemented";

        return null;
    }

    /**
     * @return Priority.
     */
    public int getPriority() {
        return pri;
    }

    /**
     * @return Priority attribute key.
     */
    public String getPriorityAttributeKey() {
        return priAttrKey;
    }

    /**
     * @param priAttrKey Priority attribute key.
     */
    public void setPriorityAttributeKey(String priAttrKey) {
        this.priAttrKey = priAttrKey;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> getTopology() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public long getStartTime() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append(getClass().getName());
        buf.append(" [priority=").append(pri);
        buf.append(", priorityAttrKey='").append(priAttrKey).append('\'');
        buf.append(']');

        return buf.toString();
    }
}
