/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client {
    using System;
    using System.Threading;
    using System.Collections.Generic;
    using GridGain.Client.Impl;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;

    /**
     * <summary>
     * Client factory opens and closes clients. It also tracks all currently
     * opened clients as well.</summary>
     */
    public static class GridClientFactory {
        /** <summary>Map that contain all opened clients.</summary> */
        private static readonly IDictionary<Guid, GridClientImpl> openClients = new Dictionary<Guid, GridClientImpl>();

        /**
         * <summary>
         * Starts a client with given configuration. Starting client will be assigned a randomly generated
         * Guid which can be obtained by <see cref="IGridClient.Id"/> method.</summary>
         *
         * <param name="cfg">Client configuration.</param>
         * <returns>Started client.</returns>
         * <exception cref="GridClientException">If client could not be created.</exception>
         */
        public static IGridClient Start(IGridClientConfiguration cfg) {
            Guid clientId = Guid.NewGuid();

            GridClientImpl client = new GridClientImpl(clientId, cfg);

            try {
                lock (openClients) {
                    openClients.Add(clientId, client);
                }
            }
            // If such clientId already exists in the collection.
            catch (ArgumentException e) {
                StopSilent(client, false);

                throw new InvalidOperationException("System generates duplicated guid for client: " + clientId, e);
            }

            return client;
        }

        /**
         * <summary>
         * Waits for all open clients to finish their operations and stops them, This method
         * is equivalent to <c>stopAll(true)</c> method invocation.</summary>
         *
         * <see cref="StopAll(bool)"/>
         */
        public static void StopAll() {
            StopAll(true);
        }

        /**
         * <summary>
         * Stops all currently open clients.</summary>
         *
         * <param name="wait">If <c>true</c> then each client will wait to finish all ongoing requests before</param>
         *      closing (however, no new requests will be accepted). If <c>false</c>, clients will be
         *      closed immediately and all ongoing requests will be failed.
         */
        public static void StopAll(bool wait) {
            ICollection<GridClientImpl> clients;

            lock(openClients) {
                clients = new List<GridClientImpl>(openClients.Values);

                openClients.Clear();
            }

            var closes = new List<IGridClientFuture>();

            foreach (GridClientImpl client in clients) {
                var c = client; // Use local variable in closure instead of iterator.

                closes.Add(U.Async(() => StopSilent(c, wait)));
            }

            foreach (var fut in closes)
                fut.WaitDone();
        }

        /**
         * <summary>
         * Waits for all pending requests for a particular client to be completed (no new requests will be
         * accepted) and then closes the client. This method is equivalent to <c>stop(clientId, true)</c>.</summary>
         *
         * <param name="clientId">Identifier of client to close.</param>
         * <see cref="Stop(Guid, bool)"/>
         */
        public static void Stop(Guid clientId) {
            Stop(clientId, true);
        }

        /**
         * <summary>
         * Stops particular client.</summary>
         *
         * <param name="clientId">Client identifier to close.</param>
         * <param name="wait">If <c>true</c> then client will wait to finish all ongoing requests before</param>
         *      closing (however, no new requests will be accepted). If <c>false</c>, client will be
         *      closed immediately and all ongoing requests will be failed.
         */
        public static void Stop(Guid clientId, bool wait) {
            GridClientImpl client;

            lock(openClients) {
                if (openClients.TryGetValue(clientId, out client))
                    openClients.Remove(clientId);
            }

            if (client != null)
                client.stop(wait);
        }


        /**
         * <summary>
         * Stops client silently.</summary>
         *
         * <param name="client">Client to stop.</param>
         * <param name="waitCompletion">If <c>true</c> will wait for all pending requests to be proceeded.</param>
         */
        private static void StopSilent(GridClientImpl client, bool waitCompletion) {
            U.DoSilent<Exception>(() => client.stop(waitCompletion), e =>
                Dbg.WriteLine("Client stop process failed (exception ignored) [client=" + client + ", e=" + e.Message + "]"));
        }
    }
}
