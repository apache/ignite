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

import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;

/**
 *
 */
public class ZookeeperClient implements Watcher {
    /** */
    private static final long RETRY_TIMEOUT = 1000;

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
    private long connectStartTime;

    /** */
    private final Object stateMux = new Object();

    /** */
    private final IgniteRunnable connLostC;

    /** */
    private final Timer connTimer;

    ZookeeperClient(IgniteLogger log, String connectString, int sesTimeout, IgniteRunnable connLostC) throws Exception {
        this(null, log, connectString, sesTimeout, connLostC);
    }

    /**
     * @param log Logger.
     * @param connectString ZK connection string.
     * @param sesTimeout ZK session timeout.
     * @throws Exception If failed.
     */
    ZookeeperClient(String igniteInstanceName, IgniteLogger log, String connectString, int sesTimeout, IgniteRunnable connLostC)
        throws Exception {
        this.log = log.getLogger(getClass());
        this.connLostC = connLostC;

        connLossTimeout = sesTimeout;

        connectStartTime = System.currentTimeMillis();

        String threadName = Thread.currentThread().getName();

        // ZK generates internal threads' names using current thread name.
        Thread.currentThread().setName("zk-" + igniteInstanceName);

        try {
            zk = new ZooKeeper(connectString, sesTimeout, this);
        }
        finally {
            Thread.currentThread().setName(threadName);
        }

        connTimer = new Timer("zk-timer-" + igniteInstanceName);

        scheduleConnectionCheck();
    }

    /** {@inheritDoc} */
    @Override public void process(WatchedEvent evt) {
        if (evt.getType() == Event.EventType.None) {
            boolean connLost = false;

            synchronized (stateMux) {
                if (state == ConnectionState.Lost) {
                    U.warn(log, "Received event after connection was lost [evtState=" + evt.getState() + "]");

                    return;
                }

                ConnectionState newState;

                Event.KeeperState zkState = evt.getState();

                switch (zkState) {
                    case Disconnected:
                        newState = ConnectionState.Disconnected;

                        break;

                    case SyncConnected:
                        newState = ConnectionState.Connected;

                        break;

                    case Expired:
                        newState = ConnectionState.Lost;

                        break;

                    default:
                        U.error(log, "Unexpected state for zookeeper client, close connection: " + zkState);

                        newState = ConnectionState.Lost;
                }

                if (newState != state) {
                    log.info("Zookeeper client state changed [prevState=" + state + ", newState=" + newState + ']');

                    state = newState;

                    if (newState == ConnectionState.Disconnected) {
                        connectStartTime = System.currentTimeMillis();

                        scheduleConnectionCheck();
                    }
                    else if (newState == ConnectionState.Connected)
                        stateMux.notifyAll();
                    else {
                        assert state == ConnectionState.Lost;

                        closeClient();

                        connLost = true;
                    }
                }
            }

            if (connLost)
                notifyConnectionLost();
        }
    }

    /**
     *
     */
    private void notifyConnectionLost() {
        if (state == ConnectionState.Lost && connLostC != null)
            connLostC.run();
    }

    public void createIfNeeded(String path, byte[] data, CreateMode createMode)
        throws ZookeeperClientFailedException, InterruptedException {
        if (data == null)
            data = EMPTY_BYTES;

        for (;;) {
            try {
                zk.create(path, data, ZK_ACL, createMode);

                break;
            }
            catch (KeeperException.NodeExistsException e) {
                log.info("Node already exists: " + path);

                break;
            }
            catch (Exception e) {
                onZookeeperError(e);
            }
        }
    }

    public void getChildrenAsync(String path, boolean watch, AsyncCallback.Children2Callback cb, Object ctx) {
        zk.getChildren(path, watch, cb, ctx);
    }

    /**
     *
     */
    public void close() {
        closeClient();
    }

    /**
     * @param e Error.
     */
    private void onZookeeperError(Exception e) throws ZookeeperClientFailedException, InterruptedException {
        ZookeeperClientFailedException err = null;

        synchronized (stateMux) {
            U.warn(log, "Failed to execute zookeeper operation [err=" + e + ", state=" + state + ']');

            if (zk.getState() == ZooKeeper.States.CLOSED)
                throw new ZookeeperClientFailedException(e);

            if (state == ConnectionState.Lost) {
                U.error(log, "Operation failed with unexpected error, connection lost: " + e, e);

                throw new ZookeeperClientFailedException(e);
            }

            if (e instanceof KeeperException.ConnectionLossException) {
                long remainingTime;

                if (state == ConnectionState.Connected) {
                    state = ConnectionState.Disconnected;

                    connectStartTime = System.currentTimeMillis();

                    remainingTime = connLossTimeout;
                }
                else {
                    assert connectStartTime != 0;

                    assert state == ConnectionState.Disconnected;

                    remainingTime = connLossTimeout - (System.currentTimeMillis() - connectStartTime);

                    if (remainingTime <= 0) {
                        state = ConnectionState.Lost;

                        U.warn(log, "Failed to establish zookeeper connection, close client " +
                            "[timeout=" + connLossTimeout + ']');

                        closeClient();

                        err = new ZookeeperClientFailedException(e);
                    }
                }

                if (err == null) {
                    U.warn(log, "Zookeeper operation failed, will retry [err=" + e +
                        ", retryTimeout=" + RETRY_TIMEOUT +
                        ", connLossTimeout=" + connLossTimeout +
                        ", remainingWaitTime=" + remainingTime + ']');

                    stateMux.wait(RETRY_TIMEOUT);
                }
            }
            else {
                U.error(log, "Operation failed with unexpected error, close client: " + e, e);

                closeClient();

                state = ConnectionState.Lost;

                throw new ZookeeperClientFailedException(e);
            }
        }

        if (err != null) {
            notifyConnectionLost();

            throw err;
        }
    }

    /**
     *
     */
    private void closeClient() {
        try {
            zk.close();
        }
        catch (Exception closeErr) {
            U.warn(log, "Failed to close zookeeper client: " + closeErr, closeErr);
        }

        connTimer.cancel();
    }

    /**
     *
     */
    private void scheduleConnectionCheck() {
        assert state == ConnectionState.Disconnected : state;

        connTimer.schedule(new ConnectionTimeoutTask(connectStartTime), connLossTimeout);
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
                if (state == ConnectionState.Disconnected &&
                    ZookeeperClient.this.connectStartTime == connectStartTime) {

                    state = ConnectionState.Lost;

                    U.warn(log, "Failed to establish zookeeper connection, close client " +
                        "[timeout=" + connLossTimeout + ']');

                    connLoss = true;

                    closeClient();
                }
            }

            if (connLoss)
                notifyConnectionLost();
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
