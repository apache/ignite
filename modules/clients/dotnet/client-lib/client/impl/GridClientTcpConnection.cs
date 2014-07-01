/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Threading;
    using System.Collections;
    using System.Collections.Generic;
    using System.Collections.Concurrent;
    using System.IO;
    using System.Net;
    using System.Net.Sockets;
    using System.Net.Security;
    using System.Security;

    using GridGain.Client;
    using GridGain.Client.Impl.Message;
    using GridGain.Client.Impl.Portable;
    using GridGain.Client.Portable;
    using GridGain.Client.Util;
    using GridGain.Client.Ssl;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;

    using CacheOp = GridGain.Client.Impl.Message.GridClientCacheRequestOperation;

    /**
     * <summary>
     * This class performs request to grid over tcp protocol. Serialization is performed
     * with marshaller provided.</summary>
     */
    internal class GridClientTcpConnection : GridClientConnectionAdapter {
        /** <summary>Ping packet.</summary> */
        private static readonly byte[] PING_PACKET = new byte[] { (byte)0x90, 0x00, 0x00, 0x00, 0x00 };

        /** <summary>Ping is sent every 3 seconds.</summary> */
        private static readonly TimeSpan PING_SND_TIME = TimeSpan.FromSeconds(3);

        /** <summary>Connection is considered to be half-opened if server did not respond to ping in 7 seconds.</summary> */
        private static readonly TimeSpan PING_RES_TIMEOUT = TimeSpan.FromSeconds(7);

        /** <summary>Socket read timeout.</summary> */
        private static readonly TimeSpan SOCK_READ_TIMEOUT = TimeSpan.FromSeconds(2);

        /** <summary>Request ID counter (should be modified only with Interlocked class).</summary> */
        private long reqIdCntr = 1;

        /** <summary>Requests that are waiting for response. Guarded by intrinsic lock.</summary> */
        private readonly IDictionary<long, GridClientTcpRequestFuture> pendingReqs = new ConcurrentDictionary<long, GridClientTcpRequestFuture>();

        /** <summary>Node by node id requests. Map for reducing server load.</summary> */
        private ConcurrentDictionary<Guid, GridClientTcpRequestFuture<IGridClientNode>> refreshNodeReqs =
            new ConcurrentDictionary<Guid, GridClientTcpRequestFuture<IGridClientNode>>();

        /** <summary>Authenticated session token.</summary> */
        protected byte[] sesTok;

        /** <summary>Lock for graceful shutdown.</summary> */
        private ReaderWriterLock closeLock = new ReaderWriterLock();

        /** <summary>Closed flag.</summary> */
        private volatile bool closed = false;

        /** <summary>Closed by idle request flag.</summary> */
        private volatile bool closedIdle = false;

        /** <summary>Reader thread management flag to wait completion on connection closing.</summary> */
        private volatile bool waitCompletion = true;

        /** <summary>Message marshaller</summary> */
        private GridClientPortableMarshaller marshaller;

        /** <summary>Underlying tcp client.</summary> */
        private readonly TcpClient tcp;

        /** <summary>Output stream.</summary> */
        private readonly Stream outStream;

        /** <summary>Input stream.</summary> */
        private readonly Stream inStream;

        /** <summary>SSL stream flag.</summary> */
        private readonly bool isSslStream;
                
        /** <summary>Last stream reading failed with timeout.</summary> */
        internal bool lastReadTimedOut = false;

        /** <summary>Timestamp of last packet send event.</summary> */
        private DateTime lastPacketSndTime;

        /** <summary>Timestamp of last packet receive event.</summary> */
        private DateTime lastPacketRcvTime;

        /** <summary>Ping send time. (reader thread only).</summary> */
        private DateTime lastPingSndTime;

        /** <summary>Ping receive time (reader thread only).</summary> */
        private DateTime lastPingRcvTime;

        /** <summary>Reader thread.</summary> */
        private readonly Thread rdr;

        /** <summary>Writer thread.</summary> */
        private readonly Thread writer;

        /** <summary>Queue to pass messages to writer.</summary> */
        private BlockingCollection<MemoryStream> q = new BlockingCollection<MemoryStream>();

        /**
         * <summary>
         * Creates a client facade, tries to connect to remote server, in case
         * of success starts reader thread.</summary>
         *
         * <param name="clientId">Client identifier.</param>
         * <param name="srvAddr">Server to connect to.</param>
         * <param name="sslCtx">SSL context to use if SSL is enabled, <c>null</c> otherwise.</param>
         * <param name="connectTimeout">Connect timeout.</param>
         * <param name="marshaller">Marshaller to use in communication.</param>
         * <param name="credentials">Client credentials.</param>
         * <param name="top">Topology instance.</param>
         * <exception cref="IOException">If connection could not be established.</exception>
         */
        public GridClientTcpConnection(Guid clientId, IPEndPoint srvAddr, IGridClientSslContext sslCtx, int connectTimeout,
            GridClientPortableMarshaller marshaller, Object credentials, GridClientTopology top)
            : base(clientId, srvAddr, sslCtx, credentials, top) {
            this.marshaller = marshaller;

            if (connectTimeout <= 0)
                connectTimeout = (int) SOCK_READ_TIMEOUT.TotalMilliseconds;

            Dbg.Assert(connectTimeout > 0, "connectTimeout > 0");

            int retries = 3;
            Exception firstCause = null;

            do {
                if (retries-- <= 0)
                    throw new IOException("Cannot open socket for address: " + srvAddr, firstCause);

                if (tcp != null)
                    Dbg.WriteLine("Connection to address {0} failed, try re-connect", srvAddr);

                try {
                    // Create a TCP/IP client socket.
                    tcp = new TcpClient();

                    tcp.ReceiveTimeout = connectTimeout;
                    tcp.SendTimeout = connectTimeout;
                    tcp.NoDelay = true;

                    // Open connection.
                    tcp.Connect(srvAddr);
                }
                catch (Exception x) {
                    if (firstCause == null)
                        firstCause = x;

                    if (tcp != null && tcp.Connected)
                        tcp.Close();

                    continue;
                }
            } while (!tcp.Connected);

            if (sslCtx == null) {
                isSslStream = false;

                outStream = new BufferedStream(tcp.GetStream(), Math.Min(tcp.SendBufferSize, 32768));
                inStream = new BufferedStream(tcp.GetStream(), Math.Min(tcp.ReceiveBufferSize, 32768));
            }
            else {
                isSslStream = true;

                Stream sslStream = sslCtx.CreateStream(tcp);

                outStream = new BufferedStream(sslStream, Math.Min(tcp.SendBufferSize, 32768));
                inStream = new BufferedStream(sslStream, Math.Min(tcp.ReceiveBufferSize, 32768));

                lock (outStream) {
                    // Flush client authentication packets (if any).
                    outStream.Flush();
                }
            }

            // Process handshake.
            outStream.Write(GridClientHandshake.BYTES, 0, GridClientHandshake.BYTES.Length);

            outStream.Flush();

            int hndRes = inStream.ReadByte();

            if (hndRes != GridClientHandshake.CODE_OK)
            {
                throw new GridClientException("Failed to perform handshake with server [srvAddr=" + srvAddr + 
                    ", errorCode=" + hndRes + ']'); 
            }

            // Avoid immediate attempt to close by idle.
            lastPacketSndTime = lastPacketRcvTime = lastPingSndTime = lastPingRcvTime = U.Now;

            rdr = new Thread(readPackets);

            rdr.Name = "grid-tcp-connection-rdr--client#" + clientId + "--addr#" + srvAddr;

            //Dbg.WriteLine("Start thread: " + rdr.Name);

            rdr.Start();

            writer = new Thread(writePackets);

            writer.Name = "grid-tcp-connection-writer--client#" + clientId + "--addr#" + srvAddr;

            writer.Start();
        }

        /**
         * <summary>
         * Closes this client. No methods of this class can be used after this method was called.
         * Any attempt to perform request on closed client will case <see cref="GridClientConnectionResetException"/>.
         * All pending requests are failed without waiting for response.</summary>
         *
         * <param name="waitCompletion">If <c>true</c> this method will wait for all pending requests to be completed.</param>
         */
        override sealed public void Close(bool waitCompletion) {
            closeLock.AcquireWriterLock(Timeout.Infinite);

            try {
                // Skip, if already closed.
                if (closed)
                    return;

                // Mark connection as closed.
                this.closed = true;
                this.closedIdle = false;
                this.waitCompletion = waitCompletion;
            }
            finally {
                closeLock.ReleaseWriterLock();
            }

            shutdown();
        }

        /** <inheritdoc/> */
        override public bool CloseIfIdle(TimeSpan timeout) {
            closeLock.AcquireWriterLock(Timeout.Infinite);

            try {
                // Skip, if already closed.
                if (closed)
                    return true;

                // Skip, if inactivity is less then timeout
                if (U.Now - lastNetworkActivityTimestamp() < timeout)
                    return false;

                // Skip, if there are several pending requests.
                if (pendingReqs.Count != 0)
                    return false;

                // Mark connection as closed.
                closed = true;
                closedIdle = true;
                waitCompletion = true;
            }
            finally {
                closeLock.ReleaseWriterLock();
            }

            shutdown();

            return true;
        }

        /** <summary>Closes all resources and fails all pending requests.</summary> */
        private void shutdown() {
            Dbg.Assert(closed);
            Dbg.Assert(!Thread.CurrentThread.Equals(rdr));

            rdr.Interrupt();
            writer.Interrupt();

            rdr.Join();
            writer.Join();

            lock (outStream) {
                // It's unclear from documentation why this call is needed,
                // but without it we get IOException on the server.
                U.DoSilent<Exception>(() => tcp.Client.Disconnect(false), null);

                U.DoSilent<Exception>(() => outStream.Close(), null);
                U.DoSilent<Exception>(() => tcp.Close(), null);
            }

            foreach (GridClientFuture fut in pendingReqs.Values)
                fut.Fail(() => {
                    throw new GridClientClosedException("Failed to perform request" +
                        " (connection was closed before response was received): " + ServerAddress);
                });

            Dbg.WriteLine("Connection closed: " + ServerAddress);
        }

        /**
         * <summary>
         * Gets last network activity for this connection.</summary>
         *
         * <returns>Last network activity for this connection.</returns>
         */
        private DateTime lastNetworkActivityTimestamp() {
            return lastPacketSndTime > lastPacketRcvTime ? lastPacketSndTime : lastPacketRcvTime;
        }

        /**
         * <summary>
         * Makes request to server via tcp protocol and returns a future that will be completed when
         * response is received.</summary>
         *
         * <param name="msg">Message to request,</param>
         * <returns>Response object.</returns>
         * <exception cref="GridClientConnectionResetException">If request failed.</exception>
         * <exception cref="GridClientClosedException">If client was closed.</exception>
         */
        private IGridClientFuture<T> makeRequest<T>(GridClientRequest msg) {
            Dbg.Assert(msg != null);

            GridClientTcpRequestFuture<T> res = new GridClientTcpRequestFuture<T>(msg);

            makeRequest(res);

            return res;
        }

        /**
         * <summary>
         * Makes request to server via tcp protocol and returns a future
         * that will be completed when response is received.</summary>
         *
         * <param name="fut">Future that will handle response.</param>
         * <returns>Response object.</returns>
         * <exception cref="GridClientConnectionResetException">If request failed.</exception>
         * <exception cref="GridClientClosedException">If client was closed.</exception>
         */
        private void makeRequest<T>(GridClientTcpRequestFuture<T> fut) {
            long reqId;

            // Validate this connection is alive.
            closeLock.AcquireReaderLock(Timeout.Infinite);

            try {
                if (closed) {
                    if (closedIdle)
                        throw new GridClientConnectionIdleClosedException("Connection was closed by idle thread (will " +
                            "reconnect): " + ServerAddress);

                    throw new GridClientConnectionResetException("Failed to perform request (connection was closed before " +
                        "message is sent): " + ServerAddress);
                }

                // Update request properties.
                reqId = Interlocked.Increment(ref reqIdCntr);

                fut.Message.RequestId = reqId;
                fut.Message.ClientId = ClientId;
                fut.Message.SessionToken = sesTok;

                // Add request to pending queue.
                pendingReqs.Add(reqId, fut);
            }
            finally {
                closeLock.ReleaseReaderLock();
            }

            sendPacket(fut.Message);
        }

        /**
         * <summary>
         * Handles incoming response message.</summary>
         *
         * <param name="msg">Incoming response message.</param>
         */
        private void handleResponse(GridClientResponse msg) {
            GridClientTcpRequestFuture fut;

            if (!pendingReqs.TryGetValue(msg.RequestId, out fut))
                return;

            // Update authentication session token.
            if (msg.SessionToken != null)
                sesTok = msg.SessionToken;

            if (fut.State == GridClientTcpRequestFuture.INITIAL) {
                // If request initially failed due to authentication - try to re-authenticate session.
                if (msg.Status == GridClientResponseStatus.AuthFailure) {
                    fut.State = GridClientTcpRequestFuture.AUTHORISING;

                    sendCredentials(msg.RequestId);
                }
                else
                    finishRequest(fut, msg);
            }
            else if (fut.State == GridClientTcpRequestFuture.AUTHORISING) {
                if (msg.Status == GridClientResponseStatus.Success) {
                    // Send the original message under authorised session.
                    fut.State = GridClientTcpRequestFuture.RETRY;

                    fut.Message.SessionToken = sesTok;

                    sendPacket(fut.Message);
                }
                else
                    // Let the request fail straight way.
                    finishRequest(fut, msg);
            }
            else  // State == RETRY
                // Finish request with whatever server responded.
                finishRequest(fut, msg);
        }

        /**
         * <summary>
         * Sends credentials to the remote server.</summary>
         */
        private void sendCredentials(long reqId) {
            GridClientAuthenticationRequest req = new GridClientAuthenticationRequest(Guid.Empty);

            req.Credentials = Credentials;
            req.RequestId = reqId;
            req.ClientId = ClientId;

            sendPacket(req);
        }

        /**
         * <summary>
         * Finishes user request to the grid.</summary>
         *
         * <param name="fut">Future used to pass result to the user.</param>
         * <param name="resp">Incoming response message.</param>
         */
        private void finishRequest(GridClientFuture fut, GridClientResponse resp) {
            switch (resp.Status) {
                case GridClientResponseStatus.Success:
                    fut.Done(resp.Result);

                    break;

                case GridClientResponseStatus.Failed:
                    var error = resp.ErrorMessage;

                    if (error == null)
                        error = "Unknown server error";

                    fut.Fail(() => {
                        throw new GridClientException(error);
                    });

                    break;

                case GridClientResponseStatus.AuthFailure:
                    fut.Fail(() => {
                        throw new GridClientAuthenticationException("Authentication failed (session expired?)" +
                        " errMsg=" + resp.ErrorMessage + ", [srv=" + ServerAddress + "]");
                    });

                    U.Async(() => Close(false));

                    break;

                case GridClientResponseStatus.AuthorizationFailure:
                    fut.Fail(() => {
                        throw new GridClientException("Client authorization failed: " + resp.ErrorMessage);
                    });

                    break;

                default:
                    fut.Fail(() => {
                        throw new GridClientException("Unknown server response status code: " + resp.Status);
                    });

                    break;
            }

            pendingReqs.Remove(resp.RequestId);
        }

        /**
         * <summary>
         * Tries to send packet over network.</summary>
         *
         * <param name="msg">Message being sent.</param>
         * <exception cref="IOException">If client was closed before message was sent over network.</exception>
         */
        private void sendPacket(GridClientRequest msg) {
            try {
                MemoryStream buf = new MemoryStream(1024);
                
                // Header.
                buf.WriteByte((byte)0x90);

                // Reserve place for size.
                buf.WriteByte((byte)0);
                buf.WriteByte((byte)0);
                buf.WriteByte((byte)0);
                buf.WriteByte((byte)0);

                buf.Write(GridClientUtils.ToBytes(msg.RequestId), 0, 8);
                buf.Write(GridClientUtils.ToBytes(msg.ClientId), 0, 16);
                buf.Write(GridClientUtils.ToBytes(msg.DestNodeId), 0, 16);

                marshaller.Marshal(msg, buf);

                int len = (int)buf.Length;

                buf.Seek(1, SeekOrigin.Begin);

                buf.Write(U.ToBytes(len - 1 - 4), 0, 4);

                buf.Position = len;

                q.Add(buf);
            }
            catch (Exception e) {
                // In case of Exception we should shutdown the whole client since connection is broken
                // and future for this reques will never be completed.
                Close(false);

                throw new GridClientConnectionResetException("Failed to send message over network (will try to " +
                    "reconnect): " + ServerAddress, e);
            }
        }

        /** <inheritdoc/> */
        override public IGridClientFuture<Boolean> CachePutAll<K, V>(
            String cacheName, 
            ISet<GridClientCacheFlag> cacheFlags, 
            IDictionary<K, V> entries, 
            Guid destNodeId
        ) {
            Dbg.Assert(entries != null);

            GridClientCacheRequest req = new GridClientCacheRequest(CacheOp.PutAll, destNodeId);

            req.CacheName = cacheName;
            req.CacheFlags = encodeCacheFlags(cacheFlags);
            req.Values = entries.ToObjectMap();

            return makeRequest<Boolean>(req);
        }

        /** <inheritdoc/> */
        override public IGridClientFuture<IDictionary<K, V>> CacheGetAll<K, V>(
            String cacheName, 
            ISet<GridClientCacheFlag> cacheFlags, 
            IEnumerable<K> keys, 
            Guid destNodeId
        ) {
            Dbg.Assert(keys != null);

            GridClientCacheRequest req = new GridClientCacheRequest(CacheOp.GetAll, destNodeId);

            req.CacheName = cacheName;
            req.CacheFlags = encodeCacheFlags(cacheFlags);
            req.Keys = new HashSet<K>(keys);

            GridClientTcpRequestFuture<IDictionary<K, V>> fut = new GridClientTcpRequestFuture<IDictionary<K, V>>(req);

            fut.DoneConverter = o => {
                if (o == null)
                    return null;

                var map = o as IDictionary;

                if (map == null)
                    throw new ArgumentException("Expects dictionary, but received: " + o);

                IDictionary<K, V> res = new Dictionary<K, V>(map.Count);

                foreach (DictionaryEntry entry in map)
                {
                    V val = ((IGridClientPortableObject)entry.Value).Deserialize<V>();

                    res.Add((K)(entry.Key), val);
                }

                return res;
            };

            makeRequest<IDictionary<K, V>>(fut);

            return fut;
        }

        /** <inheritdoc/> */
        override public IGridClientFuture<Boolean> CacheRemove<K>(
            String cacheName, 
            ISet<GridClientCacheFlag> cacheFlags, 
            K key, 
            Guid destNodeId
        ) {
            GridClientCacheRequest req = new GridClientCacheRequest(CacheOp.Rmv, destNodeId);

            req.CacheName = cacheName;
            req.CacheFlags = encodeCacheFlags(cacheFlags);
            req.Key = key;

            return makeRequest<Boolean>(req);
        }

        /** <inheritdoc/> */
        override public IGridClientFuture<Boolean> CacheRemoveAll<K>(
            String cacheName, 
            ISet<GridClientCacheFlag> cacheFlags, 
            IEnumerable<K> keys, 
            Guid destNodeId
        ) {
            Dbg.Assert(keys != null);

            GridClientCacheRequest req = new GridClientCacheRequest(CacheOp.RmvAll, destNodeId);

            req.CacheName = cacheName;
            req.CacheFlags = encodeCacheFlags(cacheFlags);
            req.Keys = new HashSet<K>(keys);

            return makeRequest<Boolean>(req);
        }

        /** <inheritdoc/> */
        override public IGridClientFuture<Boolean> CacheReplace<K, V>(
            String cacheName, 
            ISet<GridClientCacheFlag> cacheFlags, 
            K key, V val, 
            Guid destNodeId
        ) {
            Dbg.Assert(key != null);
            Dbg.Assert(val != null);

            GridClientCacheRequest req = new GridClientCacheRequest(CacheOp.Replace, destNodeId);

            req.CacheName = cacheName;
            req.CacheFlags = encodeCacheFlags(cacheFlags);
            req.Key = key;
            req.Value = val;

            return makeRequest<Boolean>(req);
        }

        /** <inheritdoc/> */
        override public IGridClientFuture<Boolean> CacheAppend<K, V>(
            String cacheName, 
            ISet<GridClientCacheFlag> cacheFlags, 
            K key, 
            V val, 
            Guid destNodeId
        ) {
            Dbg.Assert(key != null);
            Dbg.Assert(val != null);

            GridClientCacheRequest req = new GridClientCacheRequest(CacheOp.Append, destNodeId);

            req.CacheName = cacheName;
            req.CacheFlags = encodeCacheFlags(cacheFlags);
            req.Key = key;
            req.Value = val;

            return makeRequest<Boolean>(req);
        }

        /** <inheritdoc/> */
        override public IGridClientFuture<Boolean> CachePrepend<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, V val, Guid destNodeId) {
            Dbg.Assert(key != null);
            Dbg.Assert(val != null);

            GridClientCacheRequest req = new GridClientCacheRequest(CacheOp.Prepend, destNodeId);

            req.CacheName = cacheName;
            req.CacheFlags = encodeCacheFlags(cacheFlags);
            req.Key = key;
            req.Value = val;

            return makeRequest<Boolean>(req);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CacheCompareAndSet<K, V>(
            String cacheName, 
            ISet<GridClientCacheFlag> cacheFlags, 
            K key, 
            V val1, 
            V val2, 
            Guid destNodeId
        ) {
            Dbg.Assert(key != null);

            GridClientCacheRequest msg = new GridClientCacheRequest(CacheOp.Cas, destNodeId);

            msg.CacheName = cacheName;
            msg.CacheFlags = encodeCacheFlags(cacheFlags);
            msg.Key = key;
            msg.Value = val1;
            msg.Value2 = val2;

            return makeRequest<Boolean>(msg);
        }

        /** <inheritdoc/> */
        override public IGridClientFuture<IGridClientDataMetrics> CacheMetrics(String cacheName, ISet<GridClientCacheFlag> cacheFlags, Guid destNodeId) {
            GridClientCacheRequest req = new GridClientCacheRequest(CacheOp.Metrics, destNodeId);

            req.CacheName = cacheName;
            req.CacheFlags = encodeCacheFlags(cacheFlags);

            GridClientTcpRequestFuture<IGridClientDataMetrics> fut = new GridClientTcpRequestFuture<IGridClientDataMetrics>(req);

            fut.DoneConverter = o => {
                if (o == null)
                    return null;

                var map = o as IDictionary<IGridClientPortableObject, IGridClientPortableObject>;

                if (map == null)
                    throw new ArgumentException("Expects dictionary, but received: " + o);

                var m = new Dictionary<String, Object>();

                foreach (KeyValuePair<IGridClientPortableObject, IGridClientPortableObject> entry in map)
                {
                    String key = entry.Key.Deserialize<String>();
                    Object val = entry.Value.Deserialize<Object>();

                    m[key] = val;
                }                    

                return parseCacheMetrics(m);
            };

            makeRequest<IGridClientDataMetrics>(fut);

            return fut;
        }

        /** <inheritdoc/> */
        override public IGridClientFuture<T> Execute<T>(String taskName, Object arg, Guid destNodeId) {
            GridClientTaskRequest msg = new GridClientTaskRequest(destNodeId);

            msg.TaskName = taskName;
            msg.Argument = arg;

            IGridClientFuture<GridClientTaskResultBean> fut = makeRequest<GridClientTaskResultBean>(msg);

            return new GridClientFinishedFuture<T>(() => (T)fut.Result.Result);
        }

        /**
         * <summary>
         * Convert response data into grid node bean.</summary>
         *
         * <param name="o">Response bean to convert into grid node.</param>
         * <returns>Converted grid node.</returns>
         */
        private IGridClientNode futureNodeConverter(Object o) {
            var bean = o as GridClientNodeBean;

            if (bean == null)
                return null;

            IGridClientNode node = nodeBeanToNode(bean);

            // Update node in topology.
            node = Top.UpdateNode(node);

            return node;
        }

        /** <inheritdoc/> */
        override public IGridClientFuture<IGridClientNode> Node(Guid id, bool includeAttrs, bool includeMetrics, Guid destNodeId) {
            Dbg.Assert(id != null);

            GridClientTcpRequestFuture<IGridClientNode> fut;

            // Return request that is in progress.
            if (refreshNodeReqs.TryGetValue(id, out fut))
                return fut;

            GridClientTopologyRequest msg = new GridClientTopologyRequest(destNodeId);

            msg.NodeId = id;
            msg.IncludeAttributes = includeAttrs;
            msg.IncludeMetrics = includeMetrics;

            fut = new GridClientTcpRequestFuture<IGridClientNode>(msg);

            fut.DoneCallback = () => {
                GridClientTcpRequestFuture<IGridClientNode> removed;

                //Clean up the node id requests map.
                Dbg.Assert(refreshNodeReqs.TryRemove(id, out removed));

                Dbg.Assert(fut.Equals(removed));
            };

            fut.DoneConverter = this.futureNodeConverter;

            GridClientTcpRequestFuture<IGridClientNode> actual = refreshNodeReqs.GetOrAdd(id, fut);

            // If another thread put actual request into cache.
            if (!actual.Equals(fut))
                // Ignore created future, use one from cache.
                return actual;

            makeRequest<IGridClientNode>(fut);

            return fut;
        }

        /** <inheritdoc/> */
        override public IGridClientFuture<IGridClientNode> Node(String ipAddr, bool includeAttrs, bool includeMetrics, Guid destNodeId) {
            GridClientTopologyRequest msg = new GridClientTopologyRequest(destNodeId);

            msg.NodeIP = ipAddr;
            msg.IncludeAttributes = includeAttrs;
            msg.IncludeMetrics = includeMetrics;

            GridClientTcpRequestFuture<IGridClientNode> fut = new GridClientTcpRequestFuture<IGridClientNode>(msg);

            fut.DoneConverter = this.futureNodeConverter;

            makeRequest(fut);

            return fut;
        }

        /** <inheritdoc/> */
        override public IGridClientFuture<IList<IGridClientNode>> Topology(bool includeAttrs, bool includeMetrics, Guid destNodeId) {
            GridClientTopologyRequest msg = new GridClientTopologyRequest(destNodeId);

            msg.IncludeAttributes = includeAttrs;
            msg.IncludeMetrics = includeMetrics;

            GridClientTcpRequestFuture<IList<IGridClientNode>> fut = new GridClientTcpRequestFuture<IList<IGridClientNode>>(msg);

            fut.DoneConverter = o => {
                var it = o as IEnumerable<IGridClientPortableObject>;

                if (it == null)
                    return null;

                IList<IGridClientNode> nodes = new List<IGridClientNode>();

                foreach (IGridClientPortableObject beanObj in it) 
                {
                    GridClientNodeBean bean = beanObj.Deserialize<GridClientNodeBean>();

                    nodes.Add(nodeBeanToNode(bean));
                }

                Top.UpdateTopology(nodes);

                return nodes;
            };

            makeRequest(fut);

            return fut;
        }

        /** <inheritdoc/> */
        override public IGridClientFuture<IList<String>> Log(String path, int fromLine, int toLine, Guid destNodeId) {
            GridClientLogRequest msg = new GridClientLogRequest(destNodeId);

            msg.From = fromLine;
            msg.To = toLine;
            msg.Path = path;

            GridClientTcpRequestFuture<IList<String>> fut = new GridClientTcpRequestFuture<IList<String>>(msg);

            fut.DoneConverter = o => {
                if (o == null)
                    return null;

                var list = o as IList;

                if (list  == null)
                    throw new ArgumentException("Expects string's collection, but received: " + o);

                var result = new List<String>();

                foreach (var i in list)
                    result.Add(i as String);

                return result;
            };

            makeRequest<IList<String>>(fut);

            return fut;
        }

        /**
         * <summary>
         * Creates client node instance from message.</summary>
         *
         * <param name="nodeBean">Node bean message.</param>
         * <returns>Created node.</returns>
         */
        private GridClientNodeImpl nodeBeanToNode(GridClientNodeBean nodeBean) {
            if (nodeBean == null)
                return null;

            Guid nodeId = nodeBean.NodeId;

            GridClientNodeImpl node = new GridClientNodeImpl(nodeId);

            node.TcpAddresses.AddAll<String>(nodeBean.TcpAddresses);
            node.TcpPort = nodeBean.TcpPort;
            node.ConsistentId = nodeBean.ConsistentId;
            node.ReplicaCount = nodeBean.ReplicaCount;

            if (nodeBean.Caches != null && nodeBean.Caches.Count > 0)
                node.Caches.AddAll<KeyValuePair<String, GridClientCacheMode>>(parseCacheModes(nodeBean.Caches));

            if (nodeBean.Attributes != null && nodeBean.Attributes.Count > 0)
                node.Attributes.AddAll<KeyValuePair<String, Object>>(nodeBean.Attributes);

            if (nodeBean.Metrics != null)
                node.Metrics = parseNodeMetrics(nodeBean.Metrics);

            return node;
        }

        /** <summary>Writer thread.</summary> */
        private void writePackets() {
            try {
                bool take = true;

                while (true) {
                    MemoryStream msg;

                    if (take) {
                        msg = q.Take();

                        take = false;
                    }
                    else
                        q.TryTake(out msg);

                    if (msg == null) {
                        take = true;

                        lock (outStream) {
                            outStream.Flush();
                        }

                        continue;
                    }

                    lock (outStream) {
                        msg.WriteTo(outStream);
                    }
                }
            }
            catch (IOException e) {
                if (!closed)
                    Dbg.WriteLine("Failed to write data to socket (will close connection)" +
                        " [addr=" + ServerAddress + ", e=" + e.Message + "]");
            }
            catch (Exception e) {
                if (!closed)
                    Dbg.WriteLine("Unexpected throwable in connection writer thread (will close connection)" +
                        " [addr={0}, e={1}]", ServerAddress, e);
            }
            finally {
                U.Async(() => Close(false));
            }
        }

        /** <summary>Reader thread.</summary> */
        private void readPackets() {
            try {
                bool running = true;
                byte[] lenByte = new byte[4];
                byte[] head = new byte[40];

                while (running) {
                    // Note that only this thread removes futures from map.
                    // So if we see closed condition, it is safe to check map size since no more futures
                    // will be added to the map.
                    if (closed) {
                        // Exit if either all requests processed or we do not wait for completion.
                        if (!waitCompletion)
                            break;

                        if (pendingReqs.Count == 0)
                            break;
                    }

                    // Header.
                    int symbol;

                    try {
                        if (lastReadTimedOut) {
                            lastReadTimedOut = false;

                            if (isSslStream)
                                // Recover SSL stream state after socket exception.
                                skipSslDataRecordHeader();
                        }

                        symbol = inStream.ReadByte();
                    }
                    catch (Exception e) {
                        if (e.InnerException is SocketException)
                            e = e.InnerException;

                        var sockEx = e as SocketException;

                        if (sockEx != null && sockEx.ErrorCode == 10060) {
                            checkPing();

                            lastReadTimedOut = true;

                            continue;
                        }

                        // All other exceptions are interpreted as stream ends.
                        throw;
                    }

                    // Connection closed.
                    if (symbol == -1) {
                        Dbg.WriteLine("Connection closed by remote host " +
                                "[srvAddr=" + ServerAddress + ", symbol=" + symbol + "]");

                        break;
                    }

                    // Check for correct header.
                    if ((byte)symbol != (byte)0x90) {
                        Dbg.WriteLine("Failed to parse incoming message (unexpected header received, will close) " +
                                "[srvAddr=" + ServerAddress + ", symbol=" + symbol + "]");

                        break;
                    }

                    U.ReadFully(inStream, lenByte, 0, 4);

                    int len = U.BytesToInt32(lenByte, 0);

                    if (len == 0) {
                        // Ping received.
                        lastPingRcvTime = U.Now;

                        continue;
                    }

                    if (len < 40) {
                        Dbg.WriteLine("Invalid packet received [len=" + len + "]");
                        
                        break;
                    }

                    U.ReadFully(inStream, head, 0, 40);

                    long reqId = U.BytesToInt64(head, 0);
                    Guid clientId = U.BytesToGuid(head, 8);
                    Guid destNodeId = U.BytesToGuid(head, 24);

                    byte[] msgBytes = new byte[len - 40];

                    U.ReadFully(inStream, msgBytes, 0, msgBytes.Length);

                    GridClientResponse msg = marshaller.Unmarshal(msgBytes).Deserialize<GridClientResponse>();

                    msg.RequestId = reqId;
                    msg.ClientId = clientId;
                    msg.DestNodeId = destNodeId;

                    lastPacketRcvTime = U.Now;

                    handleResponse(msg);
                }
            }
            catch (IOException e) {
                if (!closed)
                    Dbg.WriteLine("Failed to read data from remote host (will close connection)" +
                        " [addr=" + ServerAddress + ", e=" + e.Message + "]");
            }
            catch (Exception e) {
                Dbg.WriteLine("Unexpected throwable in connection reader thread (will close connection)" +
                    " [addr={0}, e={1}]", ServerAddress, e);
            }
            finally {
                U.Async(() => Close(false));
            }
        }

        /**
         * <summary>
         * Checks last ping send time and last ping receive time.</summary>
         *
         * <exception cref="IOException">If</exception>
         */
        private void checkPing() {
            DateTime now = U.Now;

            DateTime lastRcvTime = lastPacketRcvTime > lastPingRcvTime ? lastPacketRcvTime : lastPingRcvTime;

            if (now - lastPingSndTime > PING_SND_TIME) {
                lock (outStream) {
                    outStream.Write(PING_PACKET, 0, PING_PACKET.Length);
                    outStream.Flush();
                }

                lastPingSndTime = now;
            }

            if (now - lastRcvTime > PING_RES_TIMEOUT)
                throw new IOException("Did not receive any packets within ping response interval (connection is " +
                    "considered to be half-opened) [lastPingSendTime=" + lastPingSndTime + ", lastReceiveTime=" +
                    lastRcvTime + ", addr=" + ServerAddress + ']');
        }

        /**
         * <summary>
         * After read exception happens inside SSL stream, underlaying stream passes SSL service information
         * upto application level (our code). We should validate and skip this information.</summary>
         */
        private void skipSslDataRecordHeader() {
            //
            // Format of an SSL record
            // - Byte  0   = SSL record type
            // - Bytes 1-2 = SSL version (major/minor)
            // - Bytes 3-4 = Length of data in the record (excluding the header itself). The maximum SSL supports is 16384 (16K).
            //
            // Byte 0 in the record has the following record type values:
            // - SSL3_RT_CHANGE_CIPHER_SPEC  20 (x'14')
            // - SSL3_RT_ALERT               21 (x'15')
            // - SSL3_RT_HANDSHAKE           22 (x'16')
            // - SSL3_RT_APPLICATION_DATA    23 (x'17')
            //
            // For more details see: http://publib.boulder.ibm.com/infocenter/tpfhelp/current/index.jsp?topic=%2Fcom.ibm.ztpf-ztpfdf.doc_put.cur%2Fgtps5%2Fs5rcd.html
            //

            byte[] data = new byte[5];

            for (int pos = 0; pos < 5; ) {
                int read = tcp.GetStream().Read(data, pos, 5 - pos);

                if (read == 0)
                    return; // EOF.

                pos += read;
            }

            if (data[0] != (byte)23)
                throw new IOException("Invalid SSL data record header byte: " + data[0]);

            if (data[1] != (byte)3)
                throw new IOException("Unsupported SSL version: " + data[1]);

            if (data[2] != (byte)0 && data[2] != (byte)1)
                throw new IOException("Unsupported SSL type: " + data[2]);
        }
    }

    /**
     * <summary>
     * Tcp specific future without generic type specified.
     *
     * No synchronisation applied here since new instances are safely transferred to connection reader thread
     * and then only modified from there.</summary>
     */
    abstract class GridClientTcpRequestFuture : GridClientFuture {
        /** */
        public const int INITIAL = 0;

        /** */
        public const int AUTHORISING = 1;

        /** */
        public const int RETRY = 2;

        /** */
        private readonly GridClientRequest msg;

        /**
         * <param name="msg">Message to send.</param>
         */
        public GridClientTcpRequestFuture(GridClientRequest msg) {
            this.msg = msg;
            State = INITIAL;
        }

        /**
         * <summary>
         * Original user request.</summary>
         */
        public GridClientRequest Message {
            get { return msg; }
        }

        /**
         * <summary>
         * Authentication state. One of INITIAL,  AUTHORISING or RETRY.</summary>
         */
        public int State {
            get;
            set;
        }
    }

    /**
     * <summary>
     * Tcp specific future with generic type specified. Note that in C# you need to provide
     * two classes if you need wildcard-analogue from Java.</summary>
     */
    class GridClientTcpRequestFuture<T> : GridClientTcpRequestFuture, IGridClientFuture<T> {
        /** <summary>Future task execution result.</summary> */
        private T res;

        /**
         * <param name="msg">Message to send.</param>
         */
        public GridClientTcpRequestFuture(GridClientRequest msg)
            : base(msg) {
        }

        /** <summary>Successfull done converter from object to expected type.</summary> */
        public Func<Object, T> DoneConverter {
            get;
            set;
        }

        /**
         * <summary>
         * Synchronously waits for task completion and returns execution result.</summary>
         *
         * <exception cref="GridClientException">If task execution fails with exception.</exception>
         */
        public T Result {
            get {
                WaitDone();

                lock (this) {
                    return res;
                }
            }
        }

        /**
         * <summary>
         * Callback to notify that future is finished successfully.</summary>
         *
         * <param name="res">Result (can be <c>null</c>).</param>
         */
        public override void Done(Object res) {
            try {
                Done(DoneConverter == null ? (T)res : DoneConverter(res));
            }
            catch (Exception e) {
                Fail(() => {
                    throw new GridClientException(e.Message, e);
                });
            }
        }

        /**
         * <summary>
         * Callback to notify that future is finished successfully.</summary>
         *
         * <param name="res">Result (can be <c>null</c>).</param>
         */
        public void Done(T res) {
            Done(() => this.res = res);
        }
    }
}
