// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.IO;
    using System.Net;
    using System.Threading;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using GridGain.Client;
    using GridGain.Client.Ssl;
    using GridGain.Client.Impl.Marshaller;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using C = GridGain.Client.Impl.IGridClientConnection;
    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;

    /** <summary>Connections manager.</summary> */
    internal class GridClientConnectionManager {
        /** <summary>Default timeout to close idle connections</summary> */
        private static readonly TimeSpan DefaultIdleTimeout = TimeSpan.FromSeconds(30);

        /** <summary>Connect lock.</summary> */
        private readonly ReaderWriterLock guard = new ReaderWriterLock();

        /** <summary>Active connections.</summary> */
        private readonly IDictionary<IPEndPoint, C> conns = new Dictionary<IPEndPoint, C>();

        /** <summary>Connection protocol.</summary> */
        private readonly GridClientProtocol proto;

        /** <summary>SSL context.</summary> */
        private readonly IGridClientSslContext sslCtx;

        /** <summary>TCP connection timeouts</summary> */
        private readonly int connectTimeout;

        /** <summary>Client id.</summary> */
        private readonly Guid clientId;

        /** <summary>Connection credentials.</summary> */
        private readonly Object credentials;

        /** <summary>Client topology.</summary> */
        private readonly GridClientTopology top;

        /** <summary>Router endpoints to use instead of topology info.</summary> */
        private readonly ICollection<IPEndPoint> routers;

        /** <summary>Closed flag.</summary> */
        private volatile bool closed;

        /**
         * <summary>
         * Constructs connection manager.</summary>
         *
         * <param name="clientId">Client ID.</param>
         * <param name="top">Topology.</param>
         * <param name="routers">Routers or empty collection to use endpoints from topology info.</param>
         * <param name="credentials">Connection credentials.</param>
         * <param name="proto">Connection protocol.</param>
         * <param name="sslCtx">SSL context to enable secured connection or <c>null</c> to use unsecured one.</param>
         * <param name="connectTimeout">TCP connection timeout.</param>
         */
        public GridClientConnectionManager(Guid clientId, GridClientTopology top, ICollection<IPEndPoint> routers,
            Object credentials, GridClientProtocol proto, IGridClientSslContext sslCtx, int connectTimeout) {
            Dbg.Assert(clientId != null, "clientId != null");
            Dbg.Assert(top != null, "top != null");
            Dbg.Assert(routers != null, "routers != null");
            Dbg.Assert(connectTimeout >= 0, "connectTimeout > 0");

            this.clientId = clientId;
            this.credentials = credentials;
            this.top = top;
            this.routers = routers;
            this.proto = proto;
            this.sslCtx = sslCtx;
            this.connectTimeout = connectTimeout;
        }

        /**
         * <summary>
         * Gets active communication facade.</summary>
         *
         * <param name="node">Remote node to which connection should be established.</param>
         * <returns>Communication facade.</returns>
         * <exception cref="GridClientServerUnreachableException">If none of the servers can be reached after the exception.</exception>
         * <exception cref="GridClientClosedException">If client was closed manually.</exception>
         */
        public C connection(IGridClientNode node) {
            // Use router's connections if defined.
            if (routers.Count != 0)
                return connection(routers);

            // Use node's connection, if node is available over rest.
            var srvs = node.AvailableAddresses(proto);

            if (srvs.Count == 0) {
                throw new GridClientServerUnreachableException("No available endpoints to connect " +
                    "(is rest enabled for this node?): " + node);
            }

            return connection(srvs);
        }

        /**
         * <summary>
         * Gets active communication facade.</summary>
         *
         * <param name="srvs">Remote nodes to which connection should be established.</param>
         * <returns>Communication facade.</returns>
         * <exception cref="GridClientServerUnreachableException">If none of the servers can be reached after the exception.</exception>
         * <exception cref="GridClientClosedException">If client was closed manually.</exception>
         */
        public C connection(ICollection<IPEndPoint> srvs) {
            guard.AcquireReaderLock(Timeout.Infinite);

            try {
                checkClosed();

                if (srvs.Count == 0)
                    throw new GridClientServerUnreachableException("Failed to establish connection to the grid" +
                        " (address list is empty).");

                C conn;

                /* Search for existent connection. */
                foreach (IPEndPoint endPoint in srvs) {
                    if (!conns.TryGetValue(endPoint, out conn))
                        continue;

                    A.NotNull(conn, "connection");

                    /* Ignore closed connections. */
                    if (conn.CloseIfIdle(DefaultIdleTimeout)) {
                        closeIdle(DefaultIdleTimeout);

                        continue;
                    }

                    return conn;
                }

                /* Create new connection. */
                conn = connect(srvs);

                var cookie = guard.UpgradeToWriterLock(Timeout.Infinite);

                try {
                    var endPoint = conn.ServerAddress;

                    if (conns.ContainsKey(endPoint)) {
                        /* Destroy new connection. */
                        conn.Close(false);

                        /* Recover connection from the cache. */
                        conns.TryGetValue(endPoint, out conn);
                    }
                    else
                        /* Save connection in cache. */
                        conns.Add(conn.ServerAddress, conn);
                }
                finally {
                    guard.DowngradeFromWriterLock(ref cookie);
                }

                return conn;
            }
            finally {
                guard.ReleaseReaderLock();
            }
        }

        /**
         * <summary>
         * Creates a connected facade and returns it. Called either from constructor or inside
         * a write lock.</summary>
         *
         * <param name="srvs">List of server addresses that this method will try to connect to.</param>
         * <returns>Connected client facade.</returns>
         * <exception cref="GridClientServerUnreachableException">If none of the servers can be reached.</exception>
         */
        private C connect(ICollection<IPEndPoint> srvs) {
            Dbg.Assert(guard.IsReaderLockHeld);

            if (srvs.Count == 0)
                throw new GridClientServerUnreachableException("Failed to establish connection to the grid node (address " +
                    "list is empty).");

            IOException cause = null;

            foreach (IPEndPoint srv in srvs) {
                try {
                    C cnn = connect(srv);

                    Dbg.WriteLine("Connection successfully opened: " + srv);

                    return cnn;
                }
                catch (IOException e) {
                    Dbg.WriteLine("Unable to connect to grid node [srvAddr=" + srv + ", msg=" + e.Message + ']');

                    cause = e;
                }
            }

            A.NotNull(cause, "cause");

            throw new GridClientServerUnreachableException("Failed to connect to any of the servers in list: " 
                + String.Join<IPEndPoint>(",", srvs), cause);
        }

        /**
         * <summary>
         * Create new connection to specified server.</summary>
         */
        private C connect(IPEndPoint srv) {
            Dbg.Assert(guard.IsReaderLockHeld);

            switch (proto) {
                case GridClientProtocol.Tcp: {
                    return new GridClientTcpConnection(clientId, srv, sslCtx, connectTimeout,
                        new GridClientProtobufMarshaller(), credentials, top);
                }

                case GridClientProtocol.Http: {
                    return new GridClientHttpConnection(clientId, srv, sslCtx, credentials, top);
                }

                default:
                    throw new GridClientServerUnreachableException("Failed to create client" +
                        " (protocol is not supported): " + proto);
            }
        }

        /**
         * <summary>
         * Handles communication connection failure. Tries to reconnect to any of the servers specified and
         * throws an exception if none of the servers can be reached.</summary>
         *
         * <param name="node">Remote node for which this connection belongs to.</param>
         * <param name="conn">Facade that caused the exception.</param>
         * <param name="e">Exception.</param>
         */
        public void onFacadeFailed(IGridClientNode node, C conn, GridClientConnectionResetException e) {
            Dbg.WriteLine("Connection with remote node was terminated" +
                "[node=" + node + ", srvAddr=" + conn.ServerAddress + ", errMsg=" + e.Message + ']');

            guard.AcquireWriterLock(Timeout.Infinite);

            try {
                IPEndPoint srv = conn.ServerAddress;
                C cached;

                if (conns.TryGetValue(srv, out cached) && cached.Equals(conn))
                    conns.Remove(srv);
            }
            finally {
                guard.ReleaseWriterLock();
            }

            /* Close connection to failed node outside the writer lock. */
            closeSilent(conn, false);
        }

        /**
         * <summary>
         * Closes all opened connections.</summary>
         *
         * <param name="waitCompletion">If <c>true</c> will wait for all pending requests to be proceeded.</param>
         */
        public void closeAll(bool waitCompletion) {
            ICollection<C> closeConns;

            guard.AcquireWriterLock(Timeout.Infinite);

            try {
                if (closed)
                    return;

                /* Mark manager as closed. */
                closed = true;

                /* Remove all connections from cache. */
                closeConns = removeConnections(conn => true);
            }
            finally {
                guard.ReleaseWriterLock();
            }

            /* Close old connection outside the writer lock. */
            foreach (C conn in closeConns)
                closeSilent(conn, waitCompletion);
        }

        /**
         * <summary>
         * Close all idle connections more then "timeout" milliseconds.</summary>
         *
         * <param name="timeout">Idle timeout in ms.</param>
         */
        public void closeIdle(TimeSpan timeout) {
            LockCookie cookie = default(LockCookie);

            if (guard.IsReaderLockHeld)
                cookie = guard.UpgradeToWriterLock(Timeout.Infinite);
            else
                guard.AcquireWriterLock(Timeout.Infinite);

            try {
                removeConnections(conn => conn.CloseIfIdle(timeout));
            }
            finally {
                if (cookie == default(LockCookie))
                    guard.ReleaseWriterLock();
                else
                    guard.DowngradeFromWriterLock(ref cookie);
            }
        }

        /**
         * <summary>
         * Guarded remove connections from the cache. Expects writer lock held.</summary>
         *
         * <param name="condition">Condition for connections to remove.</param>
         * <returns>Removed connections from the cache.</returns>
         */
        private ICollection<C> removeConnections(Predicate<C> condition) {
            Dbg.Assert(guard.IsWriterLockHeld);

            var removed = new Dictionary<IPEndPoint, C>();

            /* Filter connections to remove. */
            foreach (var pair in conns)
                if (condition(pair.Value))
                    removed.Add(pair.Key, pair.Value);

            /* Remove from cache. */
            foreach (var key in removed.Keys)
                conns.Remove(key);

            return removed.Values;
        }

        /**
         * <summary>
         * Checks and throws an exception if this client was closed.</summary>
         *
         * <exception cref="GridClientClosedException">If client was closed.</exception>
         */
        private void checkClosed() {
            if (closed)
                throw new GridClientClosedException("Client was closed (no public methods of client can be used anymore).");
        }

        /**
         * <summary>
         * Silent connection close.</summary>
         *
         * <param name="conn">Connection to close.</param>
         * <param name="waitCompletion">If <c>true</c> will wait for all pending requests to be proceeded.</param>
         */
        private static void closeSilent(C conn, bool waitCompletion) {
            U.DoSilent<Exception>(() => conn.Close(waitCompletion), e =>
                Dbg.WriteLine("Failed to close connection [conn=" + conn + ", e=" + e.Message + "]"));
        }
    }
}
