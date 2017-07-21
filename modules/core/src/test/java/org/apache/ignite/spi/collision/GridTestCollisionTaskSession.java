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
import org.apache.ignite.compute.ComputeTaskSession;
import org.apache.ignite.compute.ComputeTaskSessionAttributeListener;
import org.apache.ignite.compute.ComputeTaskSessionScope;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgniteUuid;

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
    @Override public void saveCheckpoint(String key, Object state) {
        assert false : "Not implemented";
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state, ComputeTaskSessionScope scope, long timeout) {
        assert false : "Not implemented";
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state, ComputeTaskSessionScope scope, long timeout,
        boolean overwrite) {
        assert false : "Not implemented";
    }

    /** {@inheritDoc} */
    @Override public <T> T loadCheckpoint(String key) {
        assert false : "Not implemented";

        return null;
    }

    /** {@inheritDoc} */
    @Override public boolean removeCheckpoint(String key) {
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
    @Override public Collection<ComputeJobSibling> refreshJobSiblings() {
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
    @Override public void addAttributeListener(ComputeTaskSessionAttributeListener lsnr, boolean rewind) {
        assert false : "Not implemented";
    }

    /** {@inheritDoc} */
    @Override public boolean removeAttributeListener(ComputeTaskSessionAttributeListener lsnr) {
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