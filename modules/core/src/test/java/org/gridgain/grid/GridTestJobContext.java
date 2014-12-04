/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import org.apache.ignite.lang.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.util.lang.*;
import org.jetbrains.annotations.*;

import java.util.*;

/**
 * Test job context.
 */
public class GridTestJobContext extends GridMetadataAwareAdapter implements GridComputeJobContext {
    /** */
    private final IgniteUuid jobId;

    /** */
    private final Map<Object, Object> attrs = new HashMap<>();

    /** */
    public GridTestJobContext() {
        jobId = IgniteUuid.randomUuid();
    }

    /**
     * @param jobId Job ID.
     */
    public GridTestJobContext(IgniteUuid jobId) {
        this.jobId = jobId;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid getJobId() {
        return jobId;
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(Object key, @Nullable Object val) {
        attrs.put(key, val);
    }

    /** {@inheritDoc} */
    @Override public void setAttributes(Map<?, ?> attrs) {
        this.attrs.putAll(attrs);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> V getAttribute(K key) {
        return (V)attrs.get(key);
    }

    /** {@inheritDoc} */
    @Override public Map<Object, Object> getAttributes() {
        return new HashMap<>(attrs);
    }

    /** {@inheritDoc} */
    @Override public boolean heldcc() {
        return false;
    }

    /** {@inheritDoc} */
    @Override public String cacheName() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T> T affinityKey() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T> T holdcc() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public <T> T holdcc(long timeout) {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void callcc() {
        // No-op.
    }
}
