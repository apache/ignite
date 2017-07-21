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

package org.apache.ignite.internal.client;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.ignite.internal.client.impl.GridClientImpl;

/**
 * Client factory opens and closes clients. It also tracks all currently opened clients as well.
 */
public class GridClientFactory {
    /** Map that contain all opened clients. */
    private static ConcurrentMap<UUID, GridClientImpl> openClients = new ConcurrentHashMap<>();

    /** Lock to prevent concurrent adding of clients while stopAll is working. */
    private static ReadWriteLock busyLock = new ReentrantReadWriteLock();

    /**
     * Ensure singleton.
     */
    private GridClientFactory() {
        // No-op.
    }

    /**
     * Starts a client with given configuration. Starting client will be assigned a randomly generated
     * UUID which can be obtained by {@link GridClient#id()} method.
     *
     * @param cfg Client configuration.
     * @return Started client.
     * @throws GridClientException If client could not be created.
     */
    public static GridClient start(GridClientConfiguration cfg) throws GridClientException {
        busyLock.readLock().lock();

        try {
            UUID clientId = UUID.randomUUID();

            GridClientImpl client = new GridClientImpl(clientId, cfg, false);

            GridClientImpl old = openClients.putIfAbsent(clientId, client);

            assert old == null : "Random unique UUID generation failed.";

            return client;
        }
        finally {
            busyLock.readLock().unlock();
        }
    }

    /**
     * Waits for all open clients to finish their operations and stops them, This method
     * is equivalent to {@code stopAll(true)} method invocation.
     *
     * @see #stopAll(boolean)
     */
    public static void stopAll() {
        stopAll(true);
    }

    /**
     * Stops all currently open clients.
     *
     * @param wait If {@code true} then each client will wait to finish all ongoing requests before
     *      closing (however, no new requests will be accepted). If {@code false}, clients will be
     *      closed immediately and all ongoing requests will be failed.
     */
    @SuppressWarnings("TooBroadScope")
    public static void stopAll(boolean wait) {
        ConcurrentMap<UUID, GridClientImpl> old;

        busyLock.writeLock().lock();

        try {
            old = openClients;

            openClients = new ConcurrentHashMap<>();
        }
        finally {
            busyLock.writeLock().unlock();
        }

        for (GridClientImpl client : old.values())
            client.stop(wait);
    }

    /**
     * Waits for all pending requests for a particular client to be completed (no new requests will be
     * accepted) and then closes the client. This method is equivalent to {@code stop(clientId, true)}.
     *
     * @param clientId Identifier of client to close.
     * @see #stop(UUID, boolean)
     */
    public static void stop(UUID clientId) {
        stop(clientId, true);
    }

    /**
     * Stops particular client.
     *
     * @param clientId Client identifier to close.
     * @param wait If {@code true} then client will wait to finish all ongoing requests before
     *      closing (however, no new requests will be accepted). If {@code false}, client will be
     *      closed immediately and all ongoing requests will be failed.
     */
    public static void stop(UUID clientId, boolean wait) {
        busyLock.readLock().lock();

        try {
            GridClientImpl client = openClients.remove(clientId);

            if (client != null)
                client.stop(wait);
        }
        finally {
            busyLock.readLock().unlock();
        }
    }
}