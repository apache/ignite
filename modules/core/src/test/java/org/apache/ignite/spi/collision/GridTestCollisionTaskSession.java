/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.spi.collision;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.compute.ComputeTaskSessionAttributeListener;
import org.apache.ignite.compute.ComputeTaskSessionScope;
import org.apache.ignite.internal.GridTaskSessionInternal;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

/**
 * Test collision task session.
 */
public class GridTestCollisionTaskSession implements GridTaskSessionInternal {
    /** Task priority. */
    private Integer pri = 0;

    /** Task priority attribute key.  */
    private String priAttrKey;

    /**
     * Default constructor.
     */
    public GridTestCollisionTaskSession() {
        // No-op.
    }

    /**
     * Constructor.
     *
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
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public <K, V> V waitForAttribute(K key, long timeout) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public boolean waitForAttribute(Object key, Object val, long timeout) throws InterruptedException {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public Map<?, ?> waitForAttributes(Collection<?> keys, long timeout) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public boolean waitForAttributes(Map<?, ?> attrs, long timeout) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state, ComputeTaskSessionScope scope, long timeout) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(
        String key,
        Object state,
        ComputeTaskSessionScope scope,
        long timeout,
        boolean overwrite
    ) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public <T> T loadCheckpoint(String key) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public boolean removeCheckpoint(String key) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public String getTaskName() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid getId() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public long getEndTime() {
        return Long.MAX_VALUE;
    }

    /** {@inheritDoc} */
    @Override public ClassLoader getClassLoader() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public Collection<ComputeJobSibling> getJobSiblings() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public Collection<ComputeJobSibling> refreshJobSiblings() {
        return getJobSiblings();
    }

    /** {@inheritDoc} */
    @Override public ComputeJobSibling getJobSibling(IgniteUuid jobId) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(Object key, Object val) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public <K, V> V getAttribute(K key) {
        if (priAttrKey != null && priAttrKey.equals(key))
            return (V)pri;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setAttributes(Map<?, ?> attrs) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public Map<Object, Object> getAttributes() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public void addAttributeListener(ComputeTaskSessionAttributeListener lsnr, boolean rewind) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public boolean removeAttributeListener(ComputeTaskSessionAttributeListener lsnr) {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> mapFuture() {
        throw new UnsupportedOperationException("Not implemented");
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
    @Override public String getCheckpointSpi() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public @Nullable IgniteUuid getJobId() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public boolean isTaskNode() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public void onClosed() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public GridTaskSessionInternal session() {
        throw new UnsupportedOperationException("Not implemented");
    }

    /** {@inheritDoc} */
    @Override public boolean isFullSupport() {
        return true;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return getClass().getName() + " [priority=" + pri + ", priorityAttrKey='" + priAttrKey + '\'' + ']';
    }
}
