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

package org.apache.ignite.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.compute.ComputeJobSibling;
import org.apache.ignite.compute.ComputeTaskSessionAttributeListener;
import org.apache.ignite.compute.ComputeTaskSessionFullSupport;
import org.apache.ignite.compute.ComputeTaskSessionScope;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.lang.IgniteUuid;
import org.jetbrains.annotations.Nullable;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableCollection;

/**
 * Task session.
 */
public class GridTaskSessionImpl implements GridTaskSessionInternal {
    /** */
    private final String taskName;

    /** */
    private final GridDeployment dep;

    /** */
    private final String taskClsName;

    /** */
    private final IgniteUuid sesId;

    /** */
    private final long startTime;

    /** */
    private final long endTime;

    /** */
    private final UUID taskNodeId;

    /** */
    private final GridKernalContext ctx;

    /** */
    private Collection<ComputeJobSibling> siblings;

    /** Guarded by {@link #mux}. */
    private Map<Object, Object> attrs;

    /** */
    private List<ComputeTaskSessionAttributeListener> lsnrs;

    /** */
    private ClassLoader clsLdr;

    /** */
    private volatile boolean closed;

    /** */
    private volatile String cpSpi;

    /** */
    private volatile String failSpi;

    /** */
    private volatile String loadSpi;

    /** */
    private final Object mux = new Object();

    /** */
    private final AtomicInteger usage = new AtomicInteger(1);

    /** Task supports session attributes and checkpoints (there is {@link ComputeTaskSessionFullSupport}). */
    private final boolean fullSup;

    /** */
    private final boolean internal;

    /** */
    private final Collection<UUID> top;

    /** */
    private final IgnitePredicate<ClusterNode> topPred;

    /** */
    private final IgniteFutureImpl mapFut;

    /** */
    private final String execName;

    /**
     * Nodes on which the jobs of the task will be executed.
     * Guarded by {@link #mux}.
     */
    @Nullable private List<UUID> jobNodes;

    /** User who created the session, {@code null} if security is not enabled. */
    @Nullable private final Object login;

    /**
     * Constructor.
     *
     * @param taskNodeId Task node ID.
     * @param taskName Task name.
     * @param dep Deployment.
     * @param taskClsName Task class name.
     * @param sesId Task session ID.
     * @param top Topology.
     * @param topPred Topology predicate.
     * @param startTime Task execution start time.
     * @param endTime Task execution end time.
     * @param siblings Collection of siblings.
     * @param attrs Session attributes.
     * @param ctx Grid Kernal Context.
     * @param fullSup Session full support enabled flag.
     * @param internal Internal task flag.
     * @param execName Custom executor name.
     * @param login User who created the session, {@code null} if security is not enabled.
     */
    public GridTaskSessionImpl(
        UUID taskNodeId,
        String taskName,
        @Nullable GridDeployment dep,
        String taskClsName,
        IgniteUuid sesId,
        @Nullable Collection<UUID> top,
        @Nullable IgnitePredicate<ClusterNode> topPred,
        long startTime,
        long endTime,
        Collection<ComputeJobSibling> siblings,
        @Nullable Map<Object, Object> attrs,
        GridKernalContext ctx,
        boolean fullSup,
        boolean internal,
        @Nullable String execName,
        @Nullable Object login
    ) {
        assert taskNodeId != null;
        assert taskName != null;
        assert sesId != null;
        assert ctx != null;

        this.taskNodeId = taskNodeId;
        this.taskName = taskName;
        this.dep = dep;
        this.top = top;
        this.topPred = topPred;

        // Note that class name might be null here if task was not explicitly
        // deployed.
        this.taskClsName = taskClsName;
        this.sesId = sesId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.siblings = siblings != null ? unmodifiableCollection(siblings) : null;
        this.ctx = ctx;

        if (attrs != null && !attrs.isEmpty()) {
            this.attrs = new HashMap<>(attrs.size(), 1.0f);

            this.attrs.putAll(attrs);
        }

        this.fullSup = fullSup;
        this.internal = internal;
        this.execName = execName;

        mapFut = new IgniteFutureImpl(new GridFutureAdapter());

        this.login = login;
    }

    /** {@inheritDoc} */
    @Override public boolean isFullSupport() {
        return fullSup;
    }

    /**
     * Checks that the task supports session attributes and checkpoints
     * (there is {@link ComputeTaskSessionFullSupport}).
     *
     * @throws IllegalStateException If not supported.
     */
    protected void checkFullSupport() {
        if (!fullSup) {
            throw new IllegalStateException("Sessions attributes and checkpoints are disabled by default " +
                "for better performance (to enable, annotate task class with " +
                "@" + ComputeTaskSessionFullSupport.class.getSimpleName() + " annotation).");
        }
    }

    /**
     * @return {@code True} if session was acquired.
     */
    public boolean acquire() {
        while (true) {
            int cur = usage.get();

            if (cur == 0)
                return false;

            if (usage.compareAndSet(cur, cur + 1))
                return true;
        }
    }

    /**
     * @return {@code True} if session cannot be acquired any more.
     */
    public boolean release() {
        while (true) {
            int cur = usage.get();

            assert cur > 0;

            if (usage.compareAndSet(cur, cur - 1))
                // CASed to 0.
                return cur == 1;
        }
    }

    /** {@inheritDoc} */
    @Override public GridTaskSessionInternal session() {
        return this;
    }

    /** {@inheritDoc} */
    @Nullable @Override public IgniteUuid getJobId() {
        return null;
    }

    /** {@inheritDoc} */
    @Override public void onClosed() {
        if (closed)
            return;

        synchronized (mux) {
            if (closed)
                return;

            closed = true;

            if (fullSup)
                mux.notifyAll();
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isClosed() {
        return closed;
    }

    /**
     * @return Task node ID.
     */
    @Override public UUID getTaskNodeId() {
        return taskNodeId;
    }

    /** {@inheritDoc} */
    @Override public long getStartTime() {
        return startTime;
    }

    /** {@inheritDoc} */
    @Override public <K, V> V waitForAttribute(K key, long timeout) throws InterruptedException {
        A.notNull(key, "key");

        checkFullSupport();

        if (timeout == 0)
            timeout = Long.MAX_VALUE;

        long now = System.currentTimeMillis();

        // Prevent overflow.
        long end = now + timeout < 0 ? Long.MAX_VALUE : now + timeout;

        // Don't wait longer than session timeout.
        if (end > endTime)
            end = endTime;

        timeout = end - now;

        long startNanos = System.nanoTime();

        synchronized (mux) {
            long passedMillis = 0L;

            while (!closed && (attrs == null || !attrs.containsKey(key)) && passedMillis < timeout) {
                mux.wait(timeout - passedMillis);

                passedMillis = U.millisSinceNanos(startNanos);
            }

            if (closed)
                throw new InterruptedException("Session was closed: " + this);

            return attrs != null ? (V)attrs.get(key) : null;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean waitForAttribute(Object key, Object val, long timeout) throws InterruptedException {
        A.notNull(key, "key");

        checkFullSupport();

        if (timeout == 0)
            timeout = Long.MAX_VALUE;

        long now = System.currentTimeMillis();

        // Prevent overflow.
        long end = now + timeout < 0 ? Long.MAX_VALUE : now + timeout;

        // Don't wait longer than session timeout.
        if (end > endTime)
            end = endTime;

        timeout = end - now;

        long startNanos = System.nanoTime();

        synchronized (mux) {
            boolean isFound = false;

            long passedMillis = 0L;

            while (!closed && !(isFound = isAttributeSet(key, val)) && passedMillis < timeout) {
                mux.wait(timeout - passedMillis);

                passedMillis = U.millisSinceNanos(startNanos);
            }

            if (closed)
                throw new InterruptedException("Session was closed: " + this);

            return isFound;
        }
    }

    /** {@inheritDoc} */
    @Override public Map<?, ?> waitForAttributes(Collection<?> keys, long timeout)
        throws InterruptedException {
        A.notNull(keys, "keys");

        checkFullSupport();

        if (keys.isEmpty())
            return emptyMap();

        if (timeout == 0)
            timeout = Long.MAX_VALUE;

        long now = System.currentTimeMillis();

        // Prevent overflow.
        long end = now + timeout < 0 ? Long.MAX_VALUE : now + timeout;

        // Don't wait longer than session timeout.
        if (end > endTime)
            end = endTime;

        timeout = end - now;

        long startNanos = System.nanoTime();

        synchronized (mux) {
            long passedMillis = 0L;

            while (!closed && (attrs == null || !attrs.keySet().containsAll(keys)) && passedMillis < timeout) {
                mux.wait(timeout - passedMillis);

                passedMillis = U.millisSinceNanos(startNanos);
            }

            if (closed)
                throw new InterruptedException("Session was closed: " + this);

            Map<Object, Object> retVal = new HashMap<>(keys.size(), 1.0f);

            if (attrs != null)
                for (Object key : keys)
                    retVal.put(key, attrs.get(key));

            return retVal;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean waitForAttributes(Map<?, ?> attrs, long timeout) throws InterruptedException {
        A.notNull(attrs, "attrs");

        checkFullSupport();

        if (attrs.isEmpty())
            return true;

        if (timeout == 0)
            timeout = Long.MAX_VALUE;

        long now = System.currentTimeMillis();

        // Prevent overflow.
        long end = now + timeout < 0 ? Long.MAX_VALUE : now + timeout;

        // Don't wait longer than session timeout.
        if (end > endTime)
            end = endTime;

        timeout = end - now;

        long startNanos = System.nanoTime();

        synchronized (mux) {
            boolean isFound = false;

            long passedMillis = 0L;

            while (!closed && passedMillis < timeout) {
                isFound = this.attrs != null && this.attrs.entrySet().containsAll(attrs.entrySet());

                if (isFound)
                    break;

                mux.wait(timeout - passedMillis);

                passedMillis = U.millisSinceNanos(startNanos);
            }

            if (closed)
                throw new InterruptedException("Session was closed: " + this);

            return isFound;
        }
    }

    /** {@inheritDoc} */
    @Override public String getTaskName() {
        return taskName;
    }

    /**
     * Returns task class name.
     *
     * @return Task class name.
     */
    public String getTaskClassName() {
        return taskClsName;
    }

    /** {@inheritDoc} */
    @Override public IgniteUuid getId() {
        return sesId;
    }

    /** {@inheritDoc} */
    @Override public long getEndTime() {
        return endTime;
    }

    /**
     * @return Task version.
     */
    public String getUserVersion() {
        return dep == null ? "" : dep.userVersion();
    }

    /** {@inheritDoc} */
    @Override public ClassLoader getClassLoader() {
        synchronized (mux) {
            return clsLdr;
        }
    }

    /**
     * @param clsLdr Class loader.
     */
    public void setClassLoader(ClassLoader clsLdr) {
        assert clsLdr != null;

        synchronized (mux) {
            this.clsLdr = clsLdr;
        }
    }

    /** {@inheritDoc} */
    @Override public boolean isTaskNode() {
        return taskNodeId.equals(ctx.discovery().localNode().id());
    }

    /** {@inheritDoc} */
    @Override public Collection<ComputeJobSibling> refreshJobSiblings() {
        return getJobSiblings();
    }

    /** {@inheritDoc} */
    @Override public Collection<ComputeJobSibling> getJobSiblings() {
        synchronized (mux) {
            return siblings;
        }
    }

    /**
     * @param siblings Siblings.
     */
    public void setJobSiblings(Collection<ComputeJobSibling> siblings) {
        synchronized (mux) {
            this.siblings = unmodifiableCollection(siblings);
        }
    }

    /**
     * @param siblings Siblings.
     */
    public void addJobSiblings(Collection<ComputeJobSibling> siblings) {
        assert isTaskNode();

        synchronized (mux) {
            Collection<ComputeJobSibling> tmp = new ArrayList<>(this.siblings.size() + siblings.size());

            tmp.addAll(this.siblings);
            tmp.addAll(siblings);

            this.siblings = unmodifiableCollection(tmp);
        }
    }

    /** {@inheritDoc} */
    @Override public ComputeJobSibling getJobSibling(IgniteUuid jobId) {
        A.notNull(jobId, "jobId");

        Collection<ComputeJobSibling> tmp = getJobSiblings();

        for (ComputeJobSibling sibling : tmp)
            if (sibling.getJobId().equals(jobId))
                return sibling;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(Object key, Object val) {
        A.notNull(key, "key");

        checkFullSupport();

        setAttributes(Collections.singletonMap(key, val));
    }

    /** {@inheritDoc} */
    @Override public <K, V> V getAttribute(K key) {
        A.notNull(key, "key");

        checkFullSupport();

        synchronized (mux) {
            return attrs != null ? (V)attrs.get(key) : null;
        }
    }

    /** {@inheritDoc} */
    @Override public void setAttributes(Map<?, ?> attrs) {
        A.notNull(attrs, "attrs");

        checkFullSupport();

        if (attrs.isEmpty())
            return;

        // Note that there is no mux notification in this block.
        // The reason is that we wait for ordered attributes to
        // come back from task prior to notification. The notification
        // will happen in 'setInternal(...)' method.
        synchronized (mux) {
            if (this.attrs == null)
                this.attrs = new HashMap<>(attrs.size(), 1.0f);

            this.attrs.putAll(attrs);
        }

        if (isTaskNode()) {
            try {
                ctx.task().setAttributes(this, attrs);
            }
            catch (IgniteCheckedException e) {
                throw U.convertException(e);
            }
        }
    }

    /** {@inheritDoc} */
    @Override public Map<Object, Object> getAttributes() {
        checkFullSupport();

        synchronized (mux) {
            return attrs == null || attrs.isEmpty() ? emptyMap() : U.sealMap(attrs);
        }
    }

    /**
     * @param attrs Attributes to set.
     */
    public void setInternal(Map<?, ?> attrs) {
        A.notNull(attrs, "attrs");

        checkFullSupport();

        if (attrs.isEmpty())
            return;

        List<ComputeTaskSessionAttributeListener> lsnrs;

        synchronized (mux) {
            if (this.attrs == null)
                this.attrs = new HashMap<>(attrs.size(), 1.0f);

            this.attrs.putAll(attrs);

            lsnrs = this.lsnrs;

            mux.notifyAll();
        }

        if (lsnrs != null)
            for (Map.Entry<?, ?> entry : attrs.entrySet())
                for (ComputeTaskSessionAttributeListener lsnr : lsnrs)
                    lsnr.onAttributeSet(entry.getKey(), entry.getValue());
    }

    /** {@inheritDoc} */
    @Override public void addAttributeListener(ComputeTaskSessionAttributeListener lsnr, boolean rewind) {
        A.notNull(lsnr, "lsnr");

        checkFullSupport();

        Map<Object, Object> attrs = null;

        List<ComputeTaskSessionAttributeListener> lsnrs;

        synchronized (mux) {
            lsnrs = this.lsnrs != null ?
                new ArrayList<ComputeTaskSessionAttributeListener>(this.lsnrs.size() + 1) :
                new ArrayList<ComputeTaskSessionAttributeListener>(1);

            if (this.lsnrs != null)
                lsnrs.addAll(this.lsnrs);

            lsnrs.add(lsnr);

            this.lsnrs = lsnrs;

            if (rewind && this.attrs != null)
                attrs = new HashMap<>(this.attrs);
        }

        if (attrs != null)
            for (Map.Entry<Object, Object> entry : attrs.entrySet())
                for (ComputeTaskSessionAttributeListener l : lsnrs)
                    l.onAttributeSet(entry.getKey(), entry.getValue());
    }

    /** {@inheritDoc} */
    @Override public boolean removeAttributeListener(ComputeTaskSessionAttributeListener lsnr) {
        A.notNull(lsnr, "lsnr");

        checkFullSupport();

        synchronized (mux) {
            if (lsnrs == null)
                return false;

            List<ComputeTaskSessionAttributeListener> lsnrs = new ArrayList<>(this.lsnrs);

            boolean rmv = lsnrs.remove(lsnr);

            this.lsnrs = lsnrs.isEmpty() ? null : lsnrs;

            return rmv;
        }
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state) {
        saveCheckpoint(key, state, ComputeTaskSessionScope.SESSION_SCOPE, 0);
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state, ComputeTaskSessionScope scope, long timeout) {
        saveCheckpoint(key, state, scope, timeout, true);
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state, ComputeTaskSessionScope scope,
        long timeout, boolean overwrite) {
        saveCheckpoint0(this, key, state, scope, timeout, overwrite);
    }

    /**
     * @param ses Session.
     * @param key Key.
     * @param state State.
     * @param scope Scope.
     * @param timeout Timeout.
     * @param overwrite Overwrite.
     * @throws IgniteException If failed.
     */
    protected void saveCheckpoint0(GridTaskSessionInternal ses, String key, Object state, ComputeTaskSessionScope scope,
        long timeout, boolean overwrite) throws IgniteException {
        assert ses != null; // Internal call, so assert should be enough.

        A.notNull(key, "key");
        A.ensure(timeout >= 0, "timeout >= 0");

        if (closed)
            throw new IgniteException("Failed to save checkpoint (session closed): " + ses);

        checkFullSupport();

        try {
            ctx.checkpoint().storeCheckpoint(ses, key, state, scope, timeout, overwrite);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public <T> T loadCheckpoint(String key) {
        return loadCheckpoint0(this, key);
    }

    /**
     * @param ses Session.
     * @param key Key.
     * @return Checkpoint.
     * @throws IgniteException If failed.
     */
    protected <T> T loadCheckpoint0(GridTaskSessionInternal ses, String key) {
        assert ses != null; // Internal call, so assert should be enough.

        A.notNull(key, "key");

        if (closed)
            throw new IgniteException("Failed to load checkpoint (session closed): " + ses);

        checkFullSupport();

        try {
            return (T)ctx.checkpoint().loadCheckpoint(ses, key);
        }
        catch (IgniteCheckedException e) {
            throw U.convertException(e);
        }
    }

    /** {@inheritDoc} */
    @Override public boolean removeCheckpoint(String key) {
        return removeCheckpoint0(this, key);
    }

    /**
     * @param ses Session.
     * @param key Key.
     * @return {@code True} if removed.
     * @throws IgniteException If failed.
     */
    protected boolean removeCheckpoint0(GridTaskSessionInternal ses, String key) throws IgniteException {
        assert ses != null; // Internal call, so assert should be enough.

        A.notNull(key, "key");

        if (closed)
            throw new IgniteException("Failed to remove checkpoint (session closed): " + ses);

        checkFullSupport();

        return ctx.checkpoint().removeCheckpoint(ses, key);
    }

    /**
     * @return Topology predicate.
     */
    @Nullable public IgnitePredicate<ClusterNode> getTopologyPredicate() {
        return topPred;
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> getTopology() {
        if (topPred != null)
            return F.viewReadOnly(ctx.discovery().allNodes(), F.node2id(), topPred);

        return top != null ? top : F.nodeIds(ctx.discovery().allNodes());
    }

    /**
     * @param key Key.
     * @param val Value.
     * @return {@code true} if key/value pair was set.
     */
    private boolean isAttributeSet(Object key, Object val) {
        assert Thread.holdsLock(mux);
        assert fullSup;

        if (attrs != null && attrs.containsKey(key)) {
            Object stored = attrs.get(key);

            if (val == null && stored == null)
                return true;

            if (val != null && stored != null)
                return val.equals(stored);
        }

        return false;
    }

    /** {@inheritDoc} */
    @Override public String getCheckpointSpi() {
        return cpSpi;
    }

    /**
     * @param cpSpi Checkpoint SPI name.
     */
    public void setCheckpointSpi(String cpSpi) {
        this.cpSpi = cpSpi;
    }

    /**
     * @return Failover SPI name.
     */
    public String getFailoverSpi() {
        return failSpi;
    }

    /**
     * @param failSpi Failover SPI name.
     */
    public void setFailoverSpi(String failSpi) {
        this.failSpi = failSpi;
    }

    /**
     * @return Load balancing SPI name.
     */
    public String getLoadBalancingSpi() {
        return loadSpi;
    }

    /**
     * @param loadSpi Load balancing SPI name.
     */
    public void setLoadBalancingSpi(String loadSpi) {
        this.loadSpi = loadSpi;
    }

    /**
     * @return Task internal version.
     */
    public long getSequenceNumber() {
        return dep == null ? 0 : dep.sequenceNumber();
    }

    /**
     * @return Deployment.
     */
    public GridDeployment deployment() {
        return dep;
    }

    /**
     * Task map callback.
     */
    public void onMapped() {
        ((GridFutureAdapter)mapFut.internalFuture()).onDone();
    }

    /**
     * Finish task callback.
     */
    public void onDone() {
        ((GridFutureAdapter)mapFut.internalFuture()).onDone();
    }

    /** {@inheritDoc} */
    @Override public IgniteFuture<?> mapFuture() {
        return mapFut;
    }

    /**
     * @return {@code True} if task is internal.
     */
    public boolean isInternal() {
        return internal;
    }

    /**
     * @return Custom executor name.
     */
    @Nullable public String executorName() {
        return execName;
    }

    /**
     * Sets nodes on which the jobs of the task will be executed.
     *
     * @param jobNodes Nodes on which the jobs of the task will be executed.
     */
    public void jobNodes(Collection<UUID> jobNodes) {
        synchronized (mux) {
            this.jobNodes = F.isEmpty(jobNodes) ? emptyList() : new ArrayList<>(jobNodes);
        }
    }

    /**
     * @return Nodes on which the jobs of the task will be executed.
     */
    public List<UUID> jobNodesSafeCopy() {
        synchronized (mux) {
            return F.isEmpty(jobNodes) ? emptyList() : new ArrayList<>(jobNodes);
        }
    }

    /**
     * @return All session attributes, without checks.
     */
    public Map<Object, Object> attributesSafeCopy() {
        synchronized (mux) {
            return F.isEmpty(attrs) ? emptyMap() : new HashMap<>(attrs);
        }
    }

    /**
     * @return User who created the session, {@code null} if security is not enabled.
     */
    public Object login() {
        return login;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTaskSessionImpl.class, this);
    }
}
