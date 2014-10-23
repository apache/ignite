/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.managers.deployment.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.util.*;
import java.util.concurrent.atomic.*;

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
    private final GridUuid sesId;

    /** */
    private final long startTime;

    /** */
    private final long endTime;

    /** */
    private final UUID taskNodeId;

    /** */
    private final GridKernalContext ctx;

    /** */
    private Collection<GridComputeJobSibling> siblings;

    /** */
    private Map<Object, Object> attrs;

    /** */
    private List<GridComputeTaskSessionAttributeListener> lsnrs;

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

    /** */
    private final boolean fullSup;

    /** */
    private final Collection<UUID> top;

    /** */
    private final UUID subjId;

    /**
     * @param taskNodeId Task node ID.
     * @param taskName Task name.
     * @param dep Deployment.
     * @param taskClsName Task class name.
     * @param sesId Task session ID.
     * @param top Topology.
     * @param startTime Task execution start time.
     * @param endTime Task execution end time.
     * @param siblings Collection of siblings.
     * @param attrs Session attributes.
     * @param ctx Grid Kernal Context.
     * @param fullSup Session full support enabled flag.
     * @param subjId Subject ID.
     */
    public GridTaskSessionImpl(
        UUID taskNodeId,
        String taskName,
        @Nullable GridDeployment dep,
        String taskClsName,
        GridUuid sesId,
        @Nullable Collection<UUID> top,
        long startTime,
        long endTime,
        Collection<GridComputeJobSibling> siblings,
        @Nullable Map<Object, Object> attrs,
        GridKernalContext ctx,
        boolean fullSup,
        UUID subjId) {
        assert taskNodeId != null;
        assert taskName != null;
        assert sesId != null;
        assert ctx != null;

        this.taskNodeId = taskNodeId;
        this.taskName = taskName;
        this.dep = dep;
        this.top = top;

        // Note that class name might be null here if task was not explicitly
        // deployed.
        this.taskClsName = taskClsName;
        this.sesId = sesId;
        this.startTime = startTime;
        this.endTime = endTime;
        this.siblings = siblings != null ? Collections.unmodifiableCollection(siblings) : null;
        this.ctx = ctx;

        if (attrs != null && !attrs.isEmpty()) {
            this.attrs = new HashMap<>(attrs.size(), 1.0f);

            this.attrs.putAll(attrs);
        }

        this.fullSup = fullSup;
        this.subjId = subjId;
    }

    /** {@inheritDoc} */
    @Override public boolean isFullSupport() {
        return fullSup;
    }

    /** {@inheritDoc} */
    @Override public UUID subjectId() {
        return subjId;
    }

    /**
     *
     */
    protected void checkFullSupport() {
        if (!fullSup)
            throw new IllegalStateException("Sessions attributes and checkpoints are disabled by default " +
                "for better performance (to enable, annotate task class with " +
                "@GridComputeTaskSessionFullSupport annotation).");
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
    @Nullable @Override public GridUuid getJobId() {
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
    @SuppressWarnings("unchecked")
    @Override public <K, V> V waitForAttribute(K key, long timeout) throws InterruptedException {
        A.notNull(key, "key");

        checkFullSupport();

        if (timeout == 0)
            timeout = Long.MAX_VALUE;

        long now = U.currentTimeMillis();

        // Prevent overflow.
        long end = now + timeout < 0 ? Long.MAX_VALUE : now + timeout;

        // Don't wait longer than session timeout.
        if (end > endTime)
            end = endTime;

        synchronized (mux) {
            while (!closed && (attrs == null || !attrs.containsKey(key)) && now < end) {
                mux.wait(end - now);

                now = U.currentTimeMillis();
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

        long now = U.currentTimeMillis();

        // Prevent overflow.
        long end = now + timeout < 0 ? Long.MAX_VALUE : now + timeout;

        // Don't wait longer than session timeout.
        if (end > endTime)
            end = endTime;

        synchronized (mux) {
            boolean isFound = false;

            while (!closed && !(isFound = isAttributeSet(key, val)) && now < end) {
                mux.wait(end - now);

                now = U.currentTimeMillis();
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
            return Collections.emptyMap();

        if (timeout == 0)
            timeout = Long.MAX_VALUE;

        long now = U.currentTimeMillis();

        // Prevent overflow.
        long end = now + timeout < 0 ? Long.MAX_VALUE : now + timeout;

        // Don't wait longer than session timeout.
        if (end > endTime)
            end = endTime;

        synchronized (mux) {
            while (!closed && (attrs == null || !attrs.keySet().containsAll(keys)) && now < end) {
                mux.wait(end - now);

                now = U.currentTimeMillis();
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

        long now = U.currentTimeMillis();

        // Prevent overflow.
        long end = now + timeout < 0 ? Long.MAX_VALUE : now + timeout;

        // Don't wait longer than session timeout.
        if (end > endTime)
            end = endTime;

        synchronized (mux) {
            boolean isFound = false;

            while (!closed && now < end) {
                isFound = this.attrs != null && this.attrs.entrySet().containsAll(attrs.entrySet());

                if (isFound)
                    break;

                mux.wait(end - now);

                now = U.currentTimeMillis();
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
    @Override public GridUuid getId() {
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
    @Override public Collection<GridComputeJobSibling> refreshJobSiblings() throws GridException {
        return getJobSiblings();
    }

    /** {@inheritDoc} */
    @Override public Collection<GridComputeJobSibling> getJobSiblings() throws GridException {
        synchronized (mux) {
            return siblings;
        }
    }

    /**
     * @param siblings Siblings.
     */
    public void setJobSiblings(Collection<GridComputeJobSibling> siblings) {
        synchronized (mux) {
            this.siblings = Collections.unmodifiableCollection(siblings);
        }
    }

    /**
     * @param siblings Siblings.
     */
    public void addJobSiblings(Collection<GridComputeJobSibling> siblings) {
        assert isTaskNode();

        synchronized (mux) {
            Collection<GridComputeJobSibling> tmp = new ArrayList<>(this.siblings);

            tmp.addAll(siblings);

            this.siblings = Collections.unmodifiableCollection(tmp);
        }
    }

    /** {@inheritDoc} */
    @Override public GridComputeJobSibling getJobSibling(GridUuid jobId) throws GridException {
        A.notNull(jobId, "jobId");

        Collection<GridComputeJobSibling> tmp = getJobSiblings();

        for (GridComputeJobSibling sibling : tmp)
            if (sibling.getJobId().equals(jobId))
                return sibling;

        return null;
    }

    /** {@inheritDoc} */
    @Override public void setAttribute(Object key, Object val) throws GridException {
        A.notNull(key, "key");

        checkFullSupport();

        setAttributes(Collections.singletonMap(key, val));
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <K, V> V getAttribute(K key) {
        A.notNull(key, "key");

        checkFullSupport();

        synchronized (mux) {
            return attrs != null ? (V)attrs.get(key) : null;
        }
    }

    /** {@inheritDoc} */
    @Override public void setAttributes(Map<?, ?> attrs) throws GridException {
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

        if (isTaskNode())
            ctx.task().setAttributes(this, attrs);
    }

    /** {@inheritDoc} */
    @Override public Map<Object, Object> getAttributes() {
        checkFullSupport();

        synchronized (mux) {
            return attrs == null || attrs.isEmpty() ? Collections.emptyMap() : U.sealMap(attrs);
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

        List<GridComputeTaskSessionAttributeListener> lsnrs;

        synchronized (mux) {
            if (this.attrs == null)
                this.attrs = new HashMap<>(attrs.size(), 1.0f);

            this.attrs.putAll(attrs);

            lsnrs = this.lsnrs;

            mux.notifyAll();
        }

        if (lsnrs != null)
            for (Map.Entry<?, ?> entry : attrs.entrySet())
                for (GridComputeTaskSessionAttributeListener lsnr : lsnrs)
                    lsnr.onAttributeSet(entry.getKey(), entry.getValue());
    }

    /** {@inheritDoc} */
    @Override public void addAttributeListener(GridComputeTaskSessionAttributeListener lsnr, boolean rewind) {
        A.notNull(lsnr, "lsnr");

        checkFullSupport();

        Map<Object, Object> attrs = null;

        List<GridComputeTaskSessionAttributeListener> lsnrs;

        synchronized (mux) {
            lsnrs = this.lsnrs != null ?
                new ArrayList<GridComputeTaskSessionAttributeListener>(this.lsnrs.size() + 1) :
                new ArrayList<GridComputeTaskSessionAttributeListener>(1);

            if (this.lsnrs != null)
                lsnrs.addAll(this.lsnrs);

            lsnrs.add(lsnr);

            this.lsnrs = lsnrs;

            if (rewind && this.attrs != null)
                attrs = new HashMap<>(this.attrs);
        }

        if (attrs != null)
            for (Map.Entry<Object, Object> entry : attrs.entrySet())
                for (GridComputeTaskSessionAttributeListener l : lsnrs)
                    l.onAttributeSet(entry.getKey(), entry.getValue());
    }

    /** {@inheritDoc} */
    @Override public boolean removeAttributeListener(GridComputeTaskSessionAttributeListener lsnr) {
        A.notNull(lsnr, "lsnr");

        checkFullSupport();

        synchronized (mux) {
            if (lsnrs == null)
                return false;

            List<GridComputeTaskSessionAttributeListener> lsnrs = new ArrayList<>(this.lsnrs);

            boolean rmv = lsnrs.remove(lsnr);

            this.lsnrs = lsnrs.isEmpty() ? null : lsnrs;

            return rmv;
        }
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state) throws GridException {
        saveCheckpoint(key, state, GridComputeTaskSessionScope.SESSION_SCOPE, 0);
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state, GridComputeTaskSessionScope scope, long timeout)
        throws GridException {
        saveCheckpoint(key, state, scope, timeout, true);
    }

    /** {@inheritDoc} */
    @Override public void saveCheckpoint(String key, Object state, GridComputeTaskSessionScope scope,
        long timeout, boolean overwrite) throws GridException {
        saveCheckpoint0(this, key, state, scope, timeout, overwrite);
    }

    /**
     * @param ses Session.
     * @param key Key.
     * @param state State.
     * @param scope Scope.
     * @param timeout Timeout.
     * @param overwrite Overwrite.
     * @throws GridException If failed.
     */
    protected void saveCheckpoint0(GridTaskSessionInternal ses, String key, Object state, GridComputeTaskSessionScope scope,
        long timeout, boolean overwrite) throws GridException {
        assert ses != null; // Internal call, so assert should be enough.

        A.notNull(key, "key");
        A.ensure(timeout >= 0, "timeout >= 0");

        if (closed)
            throw new GridException("Failed to save checkpoint (session closed): " + ses);

        checkFullSupport();

        ctx.checkpoint().storeCheckpoint(ses, key, state, scope, timeout, overwrite);
    }

    /** {@inheritDoc} */
    @SuppressWarnings("unchecked")
    @Override public <T> T loadCheckpoint(String key) throws GridException {
        return loadCheckpoint0(this, key);
    }

    /**
     * @param ses Session.
     * @param key Key.
     * @return Checkpoint.
     * @throws GridException If failed.
     */
    protected  <T> T loadCheckpoint0(GridTaskSessionInternal ses, String key) throws GridException {
        assert ses != null; // Internal call, so assert should be enough.

        A.notNull(key, "key");

        if (closed)
            throw new GridException("Failed to load checkpoint (session closed): " + ses);

        checkFullSupport();

        return (T)ctx.checkpoint().loadCheckpoint(ses, key);
    }

    /** {@inheritDoc} */
    @Override public boolean removeCheckpoint(String key) throws GridException {
        return removeCheckpoint0(this, key);
    }

    /**
     * @param ses Session.
     * @param key Key.
     * @return {@code True} if removed.
     * @throws GridException If failed.
     */
    protected boolean removeCheckpoint0(GridTaskSessionInternal ses, String key) throws GridException {
        assert ses != null; // Internal call, so assert should be enough.

        A.notNull(key, "key");

        if (closed)
            throw new GridException("Failed to remove checkpoint (session closed): " + ses);

        checkFullSupport();

        return ctx.checkpoint().removeCheckpoint(ses, key);
    }

    /** {@inheritDoc} */
    @Override public Collection<UUID> getTopology() {
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridTaskSessionImpl.class, this);
    }
}
