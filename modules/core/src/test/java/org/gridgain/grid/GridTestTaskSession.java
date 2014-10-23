/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.gridgain.grid.compute.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test task session.
 */
public class GridTestTaskSession implements GridComputeTaskSession {
    /** */
    private String taskName;

    /** */
    private String jobTypeId;

    /** */
    private GridUuid sesId;

    /** */
    private UUID taskNodeId = UUID.randomUUID();

    /** */
    private Map<Object, Object> attrs = new HashMap<>();

    /** */
    private Collection<GridComputeTaskSessionAttributeListener> lsnrs = new ArrayList<>();

    /** */
    private ClassLoader clsLdr = getClass().getClassLoader();

    /** */
    public GridTestTaskSession() {
        /* No-op. */
    }

    /**
     * @param sesId Session ID.
     */
    public GridTestTaskSession(GridUuid sesId) {
        this.sesId = sesId;
    }

    /**
     * @param taskName Task name.
     * @param jobTypeId Job type ID.
     * @param sesId Session ID.
     */
    public GridTestTaskSession(String taskName, String jobTypeId, GridUuid sesId) {
        this.taskName = taskName;
        this.jobTypeId = jobTypeId;
        this.sesId = sesId;
    }

    /** {@inheritDoc} */
    @Override public UUID getTaskNodeId() {
        return taskNodeId;
    }

    /** {@inheritDoc} */
    @Override public <K, V> V waitForAttribute(K key, long timeout) {
        assert false : "Not implemented";

        return null;
    }

    /** {@inheritDoc} */
    @Override public <K, V> boolean waitForAttribute(K key, @Nullable V val, long timeout) throws InterruptedException {
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
    @Override public String getTaskName() {
        return taskName;
    }

    /** {@inheritDoc} */
    @Override public GridUuid getId() {
        return sesId;
    }

    /** {@inheritDoc} */
    @Override public long getEndTime() {
        return Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public ClassLoader getClassLoader() {
        return clsLdr;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Collection<GridComputeJobSibling> getJobSiblings() {
        return null;
    }

    /** {@inheritDoc} */
    @Nullable @Override public Collection<GridComputeJobSibling> refreshJobSiblings() {
        return getJobSiblings();
    }

    /** {@inheritDoc} */
    @Override public GridComputeJobSibling getJobSibling(GridUuid jobId) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(Object key, Object val) {
        attrs.put(key, val);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> V getAttribute(K key) {
        return (V)attrs.get(key);
    }

    /** {@inheritDoc} */
    @Override public void setAttributes(Map<?, ?> attrs) {
        this.attrs.putAll(attrs);
    }

    /** {@inheritDoc} */
    @Override public Map<Object, Object> getAttributes() {
        return Collections.unmodifiableMap(attrs);
    }

    /** {@inheritDoc} */
    @Override public void addAttributeListener(GridComputeTaskSessionAttributeListener lsnr, boolean rewind) {
        lsnrs.add(lsnr);
    }

    /** {@inheritDoc} */
    @Override public boolean removeAttributeListener(GridComputeTaskSessionAttributeListener lsnr) {
        return lsnrs.remove(lsnr);
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state) throws GridException {
        assert false : "Not implemented";
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state, GridComputeTaskSessionScope scope, long timeout)
        throws GridException {
        assert false : "Not implemented";
    }

    /** {@inheritDoc} */
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
    @Nullable @Override public Collection<UUID> getTopology() {
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
        buf.append(" [taskName='").append(taskName).append('\'');
        buf.append(", jobTypeId='").append(jobTypeId).append('\'');
        buf.append(", sesId=").append(sesId);
        buf.append(", clsLdr=").append(clsLdr);
        buf.append(']');

        return buf.toString();
    }
}
