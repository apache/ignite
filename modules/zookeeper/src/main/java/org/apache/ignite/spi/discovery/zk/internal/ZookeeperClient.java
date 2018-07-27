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

package org.apache.ignite.spi.discovery.zk.internal;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.jetbrains.annotations.Nullable;

/**
 * Zookeeper Client.
 */
public class ZookeeperClient implements Watcher {
    /** */
    private static final long RETRY_TIMEOUT =
        IgniteSystemProperties.getLong("IGNITE_ZOOKEEPER_DISCOVERY_RETRY_TIMEOUT", 2000);

    /** */
    private static final int MAX_RETRY_COUNT =
        IgniteSystemProperties.getInteger("IGNITE_ZOOKEEPER_DISCOVERY_MAX_RETRY_COUNT", 10);

    /** */
    private final AtomicInteger retryCount = new AtomicInteger();

    /** */
    private static final int MAX_REQ_SIZE = 1048528;

    /** */
    private static final List<ACL> ZK_ACL = ZooDefs.Ids.OPEN_ACL_UNSAFE;

    /** */
    private static final byte[] EMPTY_BYTES = {};

    /** */
    private final ZooKeeper zk;

    /** */
    private final IgniteLogger log;

    /** */
    private ConnectionState state = ConnectionState.Disconnected;

    /** */
    private long connLossTimeout;

    /** */
    private volatile long connStartTime;

    /** */
    private final Object stateMux = new Object();

    /** */
    private final IgniteRunnable connLostC;

    /** */
    private final Timer connTimer;

    /** */
    private final ArrayDeque<ZkAsyncOperation> retryQ = new ArrayDeque<>();

    /** */
    private volatile boolean closing;

    /**
     * @param log Logger.
     * @param connectString ZK connection string.
     * @param sesTimeout ZK session timeout.
     * @param connLostC Lost connection callback.
     * @throws Exception If failed.
     */
    ZookeeperClient(IgniteLogger log, String connectString, int sesTimeout, IgniteRunnable connLostC) throws Exception {
        this(null, log, connectString, sesTimeout, connLostC);
    }

    /**
     * @param igniteInstanceName Ignite instance name.
     * @param log Logger.
     * @param connectString ZK connection string.
     * @param sesTimeout ZK session timeout.
     * @param connLostC Lost connection callback.
     * @throws Exception If failed.
     */
    ZookeeperClient(String igniteInstanceName,
        IgniteLogger log,
        String connectString,
        int sesTimeout,
        IgniteRunnable connLostC)
        throws Exception
    {
        this.log = log.getLogger(getClass());
        this.connLostC = connLostC;

        connLossTimeout = sesTimeout;

        long connStartTime = this.connStartTime = System.currentTimeMillis();

        connTimer = new Timer("zk-client-timer-" + igniteInstanceName);

        String threadName = Thread.currentThread().getName();

        // ZK generates internal threads' names using current thread name.
        Thread.currentThread().setName("zk-" + igniteInstanceName);

        try {
            zk = new ZooKeeper(connectString, sesTimeout, this);
        }
        finally {
            Thread.currentThread().setName(threadName);
        }

        synchronized (stateMux) {
            if (connStartTime == this.connStartTime && state == ConnectionState.Disconnected)
                scheduleConnectionCheck();
        }
    }

    /**
     * @return Zookeeper client.
     */
    ZooKeeper zk() {
        return zk;
    }

    /**
     * @return {@code True} if connected to ZooKeeper.
     */
    boolean connected() {
        synchronized (stateMux) {
            return state == ConnectionState.Connected;
        }
    }

    /** */
    String state() {
        synchronized (stateMux) {
            return state.toString();
        }
    }

    /** {@inheritDoc} */
    @Override public void process(WatchedEvent evt) {
        if (closing)
            return;

        if (evt.getType() == Event.EventType.None) {
            ConnectionState newState;

            synchronized (stateMux) {
                if (state == ConnectionState.Lost) {
                    U.warn(log, "Received event after connection was lost [evtState=" + evt.getState() + "]");

                    return;
                }

                if (!zk.getState().isAlive())
                    return;

                Event.KeeperState zkState = evt.getState();

                switch (zkState) {
                    case SaslAuthenticated:
                        return; // No-op.

                    case AuthFailed:
                        newState = state;

                        break;

                    case Disconnected:
                        newState = ConnectionState.Disconnected;

                        break;

                    case SyncConnected:
                        newState = ConnectionState.Connected;

                        break;

                    case Expired:
                        U.warn(log, "Session expired, changing state to Lost");

                        newState = ConnectionState.Lost;

                        break;

                    default:
                        U.error(log, "Unexpected state for ZooKeeper client, close connection: " + zkState);

                        newState = ConnectionState.Lost;
                }

                if (newState != state) {
                    if (log.isInfoEnabled())
                        log.info("ZooKeeper client state changed [prevState=" + state + ", newState=" + newState + ']');

                    state = newState;

                    if (newState == ConnectionState.Disconnected) {
                        connStartTime = System.currentTimeMillis();

                        scheduleConnectionCheck();
                    }
                    else if (newState == ConnectionState.Connected) {
                        retryCount.set(0);

                        stateMux.notifyAll();
                    }
                    else
                        assert state == ConnectionState.Lost : state;
                }
                else
                    return;
            }

            if (newState == ConnectionState.Lost) {
                closeClient();

                notifyConnectionLost();
            }
            else if (newState == ConnectionState.Connected) {
                for (ZkAsyncOperation op : retryQ)
                    op.execute();
            }
        }
    }

    /**
     *
     */
    private void notifyConnectionLost() {
        if (!closing && state == ConnectionState.Lost && connLostC != null)
            connLostC.run();

        connTimer.cancel();
    }

    /**
     * @param path Path.
     * @return {@code True} if node exists.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    boolean exists(String path) throws ZookeeperClientFailedException, InterruptedException {
        for (;;) {
            long connStartTime = this.connStartTime;

            try {
                return zk.exists(path, false) != null;
            }
            catch (Exception e) {
                onZookeeperError(connStartTime, e);
            }
        }
    }

    /**
     * @param paths Paths to create.
     * @param createMode Create mode.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    void createAll(List<String> paths, CreateMode createMode)
        throws ZookeeperClientFailedException, InterruptedException {
        if (paths.isEmpty())
            return;

        List<List<Op>> batches = new LinkedList<>();

        int batchSize = 0;

        List<Op> batch = new LinkedList<>();

        for (String path : paths) {
            //TODO ZK: https://issues.apache.org/jira/browse/IGNITE-8187
            int size = requestOverhead(path) + 48 /* overhead */;

            assert size <= MAX_REQ_SIZE;

            if (batchSize + size > MAX_REQ_SIZE) {
                batches.add(batch);

                batch = new LinkedList<>();

                batchSize = 0;
            }

            batch.add(Op.create(path, EMPTY_BYTES, ZK_ACL, createMode));

            batchSize += size;
        }

        batches.add(batch);

        for (List<Op> ops : batches) {
            for (;;) {
                long connStartTime = this.connStartTime;

                try {
                    zk.multi(ops);

                    break;
                }
                catch (KeeperException.NodeExistsException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to create nodes using bulk operation: " + e);

                    for (Op op : ops)
                        createIfNeeded(op.getPath(), null, createMode);

                    break;
                }
                catch (Exception e) {
                    onZookeeperError(connStartTime, e);
                }
            }
        }
    }

    /**
     * @param path Path.
     * @param data Data.
     * @param overhead Extra overhead.
     * @return {@code True} If data size exceeds max request size and should be splitted into multiple parts.
     */
    boolean needSplitNodeData(String path, byte[] data, int overhead) {
        return requestOverhead(path) + data.length + overhead > MAX_REQ_SIZE;
    }

    /**
     * @param path Path.
     * @param data Data.
     * @param overhead Extra overhead.
     * @return Splitted data.
     */
    List<byte[]> splitNodeData(String path, byte[] data, int overhead) {
        int partSize = MAX_REQ_SIZE - requestOverhead(path) - overhead;

        int partCnt = data.length / partSize;

        if (data.length % partSize != 0)
            partCnt++;

        assert partCnt > 1 : "Do not need split";

        List<byte[]> parts = new ArrayList<>(partCnt);

        int remaining = data.length;

        for (int i = 0; i < partCnt; i++) {
            int partSize0 = Math.min(remaining, partSize);

            byte[] part = new byte[partSize0];

            System.arraycopy(data, i * partSize, part, 0, part.length);

            remaining -= partSize0;

            parts.add(part);
        }

        assert remaining == 0 : remaining;

        return parts;
    }

    /**
     * TODO ZK: https://issues.apache.org/jira/browse/IGNITE-8187
     * @param path Request path.
     * @return Marshalled request overhead.
     */
    private int requestOverhead(String path) {
        return path.length();
    }

    /**
     * @param path Path.
     * @param data Data.
     * @param createMode Create mode.
     * @return Created path.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    String createIfNeeded(String path, byte[] data, CreateMode createMode)
        throws ZookeeperClientFailedException, InterruptedException
    {
        assert !createMode.isSequential() : createMode;

        if (data == null)
            data = EMPTY_BYTES;

        for (;;) {
            long connStartTime = this.connStartTime;

            try {
                return zk.create(path, data, ZK_ACL, createMode);
            }
            catch (KeeperException.NodeExistsException e) {
                if (log.isDebugEnabled())
                    log.debug("Node already exists: " + path);

                return path;
            }
            catch (Exception e) {
                onZookeeperError(connStartTime, e);
            }
        }
    }

    /**
     * @param checkPrefix Unique prefix to check in case of retry.
     * @param parentPath Parent node path.
     * @param path Node to create.
     * @param data Node data.
     * @param createMode Create mode.
     * @return Create path.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    String createSequential(String checkPrefix, String parentPath, String path, byte[] data, CreateMode createMode)
        throws ZookeeperClientFailedException, InterruptedException
    {
        assert createMode.isSequential() : createMode;

        if (data == null)
            data = EMPTY_BYTES;

        boolean first = true;

        for (;;) {
            long connStartTime = this.connStartTime;

            try {
                if (!first) {
                    List<String> children = zk.getChildren(parentPath, false);

                    for (int i = 0; i < children.size(); i++) {
                        String child = children.get(i);

                        if (children.get(i).startsWith(checkPrefix)) {
                            String resPath = parentPath + "/" + child;

                            if (log.isDebugEnabled())
                                log.debug("Check before retry, node already created: " + resPath);

                            return resPath;
                        }
                    }
                }

                return zk.create(path, data, ZK_ACL, createMode);
            }
            catch (KeeperException.NodeExistsException e) {
                assert !createMode.isSequential() : createMode;

                if (log.isDebugEnabled())
                    log.debug("Node already exists: " + path);

                return path;
            }
            catch (Exception e) {
                onZookeeperError(connStartTime, e);
            }

            first = false;
        }
    }

    /**
     * @param path Path.
     * @return Children nodes.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    List<String> getChildren(String path)
        throws ZookeeperClientFailedException, InterruptedException
    {
        for (;;) {
            long connStartTime = this.connStartTime;

            try {
                return zk.getChildren(path, false);
            }
            catch (Exception e) {
                onZookeeperError(connStartTime, e);
            }
        }
    }

    /**
     * @param path Path.
     * @return Children nodes.
     * @throws KeeperException.NoNodeException If provided path does not exist.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    List<String> getChildrenIfPathExists(String path) throws
        KeeperException.NoNodeException, InterruptedException, ZookeeperClientFailedException {
        for (;;) {
            long connStartTime = this.connStartTime;

            try {
                return zk.getChildren(path, false);
            }
            catch (KeeperException.NoNodeException e) {
                throw e;
            }
            catch (Exception e) {
                onZookeeperError(connStartTime, e);
            }
        }
    }


    /**
     * @param path Path.
     * @throws InterruptedException If interrupted.
     * @throws KeeperException In case of error.
     * @return {@code True} if given path exists.
     */
    boolean existsNoRetry(String path) throws InterruptedException, KeeperException {
        return zk.exists(path, false) != null;
    }

    /**
     * @param path Path.
     * @param ver Expected version.
     * @throws InterruptedException If interrupted.
     * @throws KeeperException In case of error.
     */
    void deleteIfExistsNoRetry(String path, int ver) throws InterruptedException, KeeperException {
        try {
            zk.delete(path, ver);
        }
        catch (KeeperException.NoNodeException e) {
            // No-op if znode does not exist.
        }
    }

    /**
     * @param path Path.
     * @param ver Version.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    void deleteIfExists(String path, int ver)
        throws ZookeeperClientFailedException, InterruptedException
    {
        try {
            delete(path, ver);
        }
        catch (KeeperException.NoNodeException e) {
            // No-op if znode does not exist.
        }
    }

    /**
     * @param parent Parent path.
     * @param paths Children paths.
     * @param ver Version.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    void deleteAll(@Nullable String parent, List<String> paths, int ver)
        throws ZookeeperClientFailedException, InterruptedException {
        if (paths.isEmpty())
            return;

        List<List<Op>> batches = new LinkedList<>();

        int batchSize = 0;

        List<Op> batch = new LinkedList<>();

        for (String path : paths) {
            String path0 = parent != null ? parent + "/" + path : path;

            //TODO ZK: https://issues.apache.org/jira/browse/IGNITE-8187
            int size = requestOverhead(path0) + 17 /* overhead */;

            assert size <= MAX_REQ_SIZE;

            if (batchSize + size > MAX_REQ_SIZE) {
                batches.add(batch);

                batch = new LinkedList<>();

                batchSize = 0;
            }

            batch.add(Op.delete(path0, ver));

            batchSize += size;
        }

        batches.add(batch);

        for (List<Op> ops : batches) {
            for (;;) {
                long connStartTime = this.connStartTime;

                try {
                    zk.multi(ops);

                    break;
                }
                catch (KeeperException.NoNodeException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to delete nodes using bulk operation: " + e);

                    for (Op op : ops)
                        deleteIfExists(op.getPath(), ver);

                    break;
                }
                catch (Exception e) {
                    onZookeeperError(connStartTime, e);
                }
            }
        }
    }

    /**
     * @param path Path.
     * @param ver Version.
     * @throws KeeperException.NoNodeException If target node does not exist.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    private void delete(String path, int ver)
        throws KeeperException.NoNodeException, ZookeeperClientFailedException, InterruptedException
    {
        for (;;) {
            long connStartTime = this.connStartTime;

            try {
                zk.delete(path, ver);

                return;
            }
            catch (KeeperException.NoNodeException e) {
                throw e;
            }
            catch (Exception e) {
                onZookeeperError(connStartTime, e);
            }
        }
    }

    /**
     * @param path Path.
     * @param data Data.
     * @param ver Version.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     * @throws KeeperException.NoNodeException If node does not exist.
     * @throws KeeperException.BadVersionException If version does not match.
     */
    void setData(String path, byte[] data, int ver)
        throws ZookeeperClientFailedException, InterruptedException, KeeperException.NoNodeException,
        KeeperException.BadVersionException
    {
        if (data == null)
            data = EMPTY_BYTES;

        for (;;) {
            long connStartTime = this.connStartTime;

            try {
                zk.setData(path, data, ver);

                return;
            }
            catch (KeeperException.BadVersionException | KeeperException.NoNodeException e) {
                throw e;
            }
            catch (Exception e) {
                onZookeeperError(connStartTime, e);
            }
        }
    }

    /**
     * @param path Path.
     * @param stat Optional {@link Stat} instance to return znode state.
     * @return Data.
     * @throws KeeperException.NoNodeException If target node does not exist.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    byte[] getData(String path, @Nullable Stat stat)
        throws KeeperException.NoNodeException, ZookeeperClientFailedException, InterruptedException {
        for (;;) {
            long connStartTime = this.connStartTime;

            try {
                return zk.getData(path, false, stat);
            }
            catch (KeeperException.NoNodeException e) {
                throw e;
            }
            catch (Exception e) {
                onZookeeperError(connStartTime, e);
            }
        }
    }

    /**
     * @param path Path.
     * @return Data.
     * @throws KeeperException.NoNodeException If target node does not exist.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    byte[] getData(String path)
        throws KeeperException.NoNodeException, ZookeeperClientFailedException, InterruptedException
    {
        return getData(path, null);
    }

    /**
     * @param path Path.
     */
    void deleteIfExistsAsync(String path) {
        new DeleteIfExistsOperation(path).execute();
    }

    /**
     * @param path Path.
     * @param watcher Watcher.
     * @param cb Callback.
     */
    void existsAsync(String path, Watcher watcher, AsyncCallback.StatCallback cb) {
        ExistsOperation op = new ExistsOperation(path, watcher, cb);

        zk.exists(path, watcher, new StatCallbackWrapper(op), null);
    }

    /**
     * @param path Path.
     * @param watcher Watcher.
     * @param cb Callback.
     */
    void getChildrenAsync(String path, Watcher watcher, AsyncCallback.Children2Callback cb) {
        GetChildrenOperation op = new GetChildrenOperation(path, watcher, cb);

        zk.getChildren(path, watcher, new ChildrenCallbackWrapper(op), null);
    }

    /**
     * @param path Path.
     * @param watcher Watcher.
     * @param cb Callback.
     */
    void getDataAsync(String path, Watcher watcher, AsyncCallback.DataCallback cb) {
        GetDataOperation op = new GetDataOperation(path, watcher, cb);

        zk.getData(path, watcher, new DataCallbackWrapper(op), null);
    }

    /**
     * @param path Path.
     * @param data Data.
     * @param createMode Create mode.
     * @param cb Callback.
     */
    private void createAsync(String path, byte[] data, CreateMode createMode, AsyncCallback.StringCallback cb) {
        if (data == null)
            data = EMPTY_BYTES;

        CreateOperation op = new CreateOperation(path, data, createMode, cb);

        zk.create(path, data, ZK_ACL, createMode, new CreateCallbackWrapper(op), null);
    }

    /**
     *
     */
    void onCloseStart() {
        closing = true;

        synchronized (stateMux) {
            stateMux.notifyAll();
        }
    }

    /**
     *
     */
    public void close() {
        closeClient();
    }

    /**
     * @param prevConnStartTime Time when connection was established.
     * @param e Error.
     * @throws ZookeeperClientFailedException If connection to zk was lost.
     * @throws InterruptedException If interrupted.
     */
    private void onZookeeperError(long prevConnStartTime, Exception e)
        throws ZookeeperClientFailedException, InterruptedException
    {
        ZookeeperClientFailedException err = null;

        synchronized (stateMux) {
            if (closing)
                throw new ZookeeperClientFailedException("ZooKeeper client is closed.");

            U.warn(log, "Failed to execute ZooKeeper operation [err=" + e + ", state=" + state + ']');

            if (state == ConnectionState.Lost) {
                U.error(log, "Operation failed with unexpected error, connection lost: " + e, e);

                throw new ZookeeperClientFailedException(e);
            }

            boolean retry = (e instanceof KeeperException) && needRetry(((KeeperException)e).code().intValue());

            if (retry) {
                long remainingTime;

                if (state == ConnectionState.Connected && connStartTime == prevConnStartTime) {
                    state = ConnectionState.Disconnected;

                    connStartTime = System.currentTimeMillis();

                    remainingTime = connLossTimeout;
                }
                else {
                    assert connStartTime != 0;

                    assert state == ConnectionState.Disconnected : state;

                    remainingTime = connLossTimeout - (System.currentTimeMillis() - connStartTime);

                    if (remainingTime <= 0) {
                        state = ConnectionState.Lost;

                        U.warn(log, "Failed to establish ZooKeeper connection, close client " +
                            "[timeout=" + connLossTimeout + ']');

                        err = new ZookeeperClientFailedException(e);
                    }
                }

                if (err == null) {
                    U.warn(log, "ZooKeeper operation failed, will retry [err=" + e +
                        ", retryTimeout=" + RETRY_TIMEOUT +
                        ", connLossTimeout=" + connLossTimeout +
                        ", path=" + ((KeeperException)e).getPath() +
                        ", remainingWaitTime=" + remainingTime + ']');

                    stateMux.wait(RETRY_TIMEOUT);

                    if (closing)
                        throw new ZookeeperClientFailedException("ZooKeeper client is closed.");
                }
            }
            else {
                U.error(log, "Operation failed with unexpected error, close ZooKeeper client: " + e, e);

                state = ConnectionState.Lost;

                err = new ZookeeperClientFailedException(e);
            }
        }

        if (err != null) {
            closeClient();

            notifyConnectionLost();

            throw err;
        }
    }

    /**
     * @param code Zookeeper error code.
     * @return {@code True} if can retry operation.
     */
    private boolean needRetry(int code) {
        boolean retryByErrorCode = code == KeeperException.Code.CONNECTIONLOSS.intValue() ||
            code == KeeperException.Code.SESSIONMOVED.intValue() ||
            code == KeeperException.Code.OPERATIONTIMEOUT.intValue();

        if (retryByErrorCode) {
            if (MAX_RETRY_COUNT <= 0 || retryCount.incrementAndGet() < MAX_RETRY_COUNT)
                return true;
            else
                return false;
        }
        else
            return false;
    }

    /**
     *
     */
    private void closeClient() {
        try {
            zk.close();
        }
        catch (Exception closeErr) {
            U.warn(log, "Failed to close ZooKeeper client: " + closeErr, closeErr);
        }

        connTimer.cancel();
    }

    /**
     *
     */
    private void scheduleConnectionCheck() {
        assert state == ConnectionState.Disconnected : state;

        connTimer.schedule(new ConnectionTimeoutTask(connStartTime), connLossTimeout);
    }

    /**
     *
     */
    interface ZkAsyncOperation {
        /**
         *
         */
        void execute();
    }

    /**
     *
     */
    class GetChildrenOperation implements ZkAsyncOperation {
        /** */
        private final String path;

        /** */
        private final Watcher watcher;

        /** */
        private final AsyncCallback.Children2Callback cb;

        /**
         * @param path Path.
         * @param watcher Watcher.
         * @param cb Callback.
         */
        GetChildrenOperation(String path, Watcher watcher, AsyncCallback.Children2Callback cb) {
            this.path = path;
            this.watcher = watcher;
            this.cb = cb;
        }

        /** {@inheritDoc} */
        @Override public void execute() {
            getChildrenAsync(path, watcher, cb);
        }
    }

    /**
     *
     */
    class GetDataOperation implements ZkAsyncOperation {
        /** */
        private final String path;

        /** */
        private final Watcher watcher;

        /** */
        private final AsyncCallback.DataCallback cb;

        /**
         * @param path Path.
         * @param watcher Watcher.
         * @param cb Callback.
         */
        GetDataOperation(String path, Watcher watcher, AsyncCallback.DataCallback cb) {
            this.path = path;
            this.watcher = watcher;
            this.cb = cb;
        }

        /** {@inheritDoc} */
        @Override public void execute() {
            getDataAsync(path, watcher, cb);
        }
    }

    /**
     *
     */
    class ExistsOperation implements ZkAsyncOperation {
        /** */
        private final String path;

        /** */
        private final Watcher watcher;

        /** */
        private final AsyncCallback.StatCallback cb;

        /**
         * @param path Path.
         * @param watcher Watcher.
         * @param cb Callback.
         */
        ExistsOperation(String path, Watcher watcher, AsyncCallback.StatCallback cb) {
            this.path = path;
            this.watcher = watcher;
            this.cb = cb;
        }

        /** {@inheritDoc} */
        @Override public void execute() {
            existsAsync(path, watcher, cb);
        }
    }

    /**
     *
     */
    class CreateOperation implements ZkAsyncOperation {
        /** */
        private final String path;

        /** */
        private final byte[] data;

        /** */
        private final CreateMode createMode;

        /** */
        private final AsyncCallback.StringCallback cb;

        /**
         * @param path path.
         * @param data Data.
         * @param createMode Create mode.
         * @param cb Callback.
         */
        CreateOperation(String path, byte[] data, CreateMode createMode, AsyncCallback.StringCallback cb) {
            this.path = path;
            this.data = data;
            this.createMode = createMode;
            this.cb = cb;
        }

        /** {@inheritDoc} */
        @Override public void execute() {
            createAsync(path, data, createMode, cb);
        }
    }

    /**
     *
     */
    class DeleteIfExistsOperation implements AsyncCallback.VoidCallback, ZkAsyncOperation {
        /** */
        private final String path;

        /**
         * @param path Path.
         */
        DeleteIfExistsOperation(String path) {
            this.path = path;
        }

        /** {@inheritDoc} */
        @Override public void execute() {
            zk.delete(path, -1, this, null);
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx) {
            if (closing)
                return;

            if (rc == KeeperException.Code.NONODE.intValue())
                return;

            if (needRetry(rc)) {
                U.warn(log, "Failed to execute async operation, connection lost. Will retry after connection restore [" +
                    "path=" + path + ']');

                retryQ.add(this);
            }
            else if (rc == KeeperException.Code.SESSIONEXPIRED.intValue())
                U.warn(log, "Failed to execute async operation, connection lost [path=" + path + ']');
            else
                assert rc == 0 : KeeperException.Code.get(rc);
        }
    }

    /**
     *
     */
    class CreateCallbackWrapper implements AsyncCallback.StringCallback {
        /** */
        final CreateOperation op;

        /**
         * @param op Operation.
         */
        CreateCallbackWrapper(CreateOperation op) {
            this.op = op;
        }

        @Override public void processResult(int rc, String path, Object ctx, String name) {
            if (closing)
                return;

            if (rc == KeeperException.Code.NODEEXISTS.intValue())
                return;

            if (needRetry(rc)) {
                U.warn(log, "Failed to execute async operation, connection lost. Will retry after connection restore [path=" + path + ']');

                retryQ.add(op);
            }
            else if (rc == KeeperException.Code.SESSIONEXPIRED.intValue())
                U.warn(log, "Failed to execute async operation, connection lost [path=" + path + ']');
            else {
                if (op.cb != null)
                    op.cb.processResult(rc, path, ctx, name);
            }
        }
    }

    /**
     *
     */
    class ChildrenCallbackWrapper implements AsyncCallback.Children2Callback {
        /** */
        private final GetChildrenOperation op;

        /**
         * @param op Operation.
         */
        private ChildrenCallbackWrapper(GetChildrenOperation op) {
            this.op = op;
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, List<String> children, Stat stat) {
            if (closing)
                return;

            if (needRetry(rc)) {
                U.warn(log, "Failed to execute async operation, connection lost. Will retry after connection restore [path=" + path + ']');

                retryQ.add(op);
            }
            else if (rc == KeeperException.Code.SESSIONEXPIRED.intValue())
                U.warn(log, "Failed to execute async operation, connection lost [path=" + path + ']');
            else
                op.cb.processResult(rc, path, ctx, children, stat);
        }
    }

    /**
     *
     */
    class DataCallbackWrapper implements AsyncCallback.DataCallback {
        /** */
        private final GetDataOperation op;

        /**
         * @param op Operation.
         */
        private DataCallbackWrapper(GetDataOperation op) {
            this.op = op;
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, byte[] data, Stat stat) {
            if (closing)
                return;

            if (needRetry(rc)) {
                U.warn(log, "Failed to execute async operation, connection lost. Will retry after connection restore [path=" + path + ']');

                retryQ.add(op);
            }
            else if (rc == KeeperException.Code.SESSIONEXPIRED.intValue())
                U.warn(log, "Failed to execute async operation, connection lost [path=" + path + ']');
            else
                op.cb.processResult(rc, path, ctx, data, stat);
        }
    }

    /**
     *
     */
    class StatCallbackWrapper implements AsyncCallback.StatCallback {
        /** */
        private final ExistsOperation op;

        /**
         * @param op Operation.
         */
        private StatCallbackWrapper(ExistsOperation op) {
            this.op = op;
        }

        /** {@inheritDoc} */
        @Override public void processResult(int rc, String path, Object ctx, Stat stat) {
            if (closing)
                return;

            if (needRetry(rc)) {
                U.warn(log, "Failed to execute async operation, connection lost. Will retry after connection restore [path=" + path + ']');

                retryQ.add(op);
            }
            else if (rc == KeeperException.Code.SESSIONEXPIRED.intValue())
                U.warn(log, "Failed to execute async operation, connection lost [path=" + path + ']');
            else
                op.cb.processResult(rc, path, ctx, stat);
        }
    }

    /**
     *
     */
    private class ConnectionTimeoutTask extends TimerTask {
        /** */
        private final long connectStartTime;

        /**
         * @param connectStartTime Time was connection started.
         */
        ConnectionTimeoutTask(long connectStartTime) {
            this.connectStartTime = connectStartTime;
        }

        /** {@inheritDoc} */
        @Override public void run() {
            boolean connLoss = false;

            synchronized (stateMux) {
                if (closing)
                    return;

                if (state == ConnectionState.Disconnected &&
                    ZookeeperClient.this.connStartTime == connectStartTime) {

                    state = ConnectionState.Lost;

                    U.warn(log, "Failed to establish ZooKeeper connection, close client " +
                        "[timeout=" + connLossTimeout + ']');

                    connLoss = true;
                }
            }

            if (connLoss) {
                closeClient();

                notifyConnectionLost();
            }
        }
    }

    /**
     *
     */
    private enum ConnectionState {
        /** */
        Connected,
        /** */
        Disconnected,
        /** */
        Lost
    }
}
