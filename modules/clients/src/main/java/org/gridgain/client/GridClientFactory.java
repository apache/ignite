/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.client;

import org.gridgain.client.impl.*;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.*;

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

            GridClientImpl client = new GridClientImpl(clientId, cfg);

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
