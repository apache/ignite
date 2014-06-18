/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl {
    using System;
    using System.Net;
    using System.Text;
    using System.Threading;
    using System.Collections.Generic;
    using System.Web;
    using System.Web.Script.Serialization;

    using GridGain.Client;
    using GridGain.Client.Ssl;
    using GridGain.Client.Util;
    using GridGain.Client.Impl.Message;

    using sc = System.Collections;
    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;


    /** <summary>Java client implementation.</summary> */
    internal class GridClientHttpConnection : GridClientConnectionAdapter {
        /** <summary>Busy lock for graceful close.</summary> */
        private ReaderWriterLock busyLock = new ReaderWriterLock();

        /** <summary>Pending requests.</summary> */
        private IList<GridClientFuture> pendingReqs = new List<GridClientFuture>();

        /** <summary>Closed flag.</summary> */
        private bool closed = false;

        /** <summary>Session token.</summary> */
        private volatile String sessionToken;

        /** <summary>Request ID counter (should be modified only with Interlocked class).</summary> */
        private long reqIdCntr = 1;

        /**
         * <summary>
         * Creates HTTP client connection.</summary>
         *
         * <param name="clientId">Client identifier.</param>
         * <param name="srvAddr">Server address on which HTTP REST handler resides.</param>
         * <param name="sslCtx">SSL context to use if SSL is enabled, <c>null</c> otherwise.</param>
         * <param name="credentials">Client credentials.</param>
         * <param name="top">Topology to use.</param>
         * <exception cref="System.IO.IOException">If input-output exception occurs.</exception>
         */
        public GridClientHttpConnection(Guid clientId, IPEndPoint srvAddr, IGridClientSslContext sslCtx,
            Object credentials, GridClientTopology top)
            : base(clientId, srvAddr, sslCtx, credentials, top) {
            // Validate server is available.
            var args = new Dictionary<String, Object>();

            args.Add("cmd", "noop");

            try {
                LoadString(args, null);
            }
            catch (System.Net.WebException e) {
                if (e.InnerException is System.Security.Authentication.AuthenticationException)
                    Dbg.Print("To use invalid certificates for secured HTTP client protocol provide system-wide" +
                        " setting: System.Net.ServicePointManager.ServerCertificateValidationCallback");

                throw; // Re-throw base exception.
            }
        }

        /** <inheritdoc /> */
        override public void Close(bool waitCompletion) {
            busyLock.AcquireWriterLock(Timeout.Infinite);

            try {
                if (closed)
                    return;

                closed = true;
            }
            finally {
                busyLock.ReleaseWriterLock();
            }

            IList<GridClientFuture> reqs;

            lock (pendingReqs) {
                reqs = new List<GridClientFuture>(pendingReqs);

                pendingReqs.Clear();
            }

            if (waitCompletion)
                foreach (GridClientFuture req in reqs)
                    try {
                        req.WaitDone();
                    }
                    catch (GridClientException e) {
                        Dbg.WriteLine("Failed to get waiting result: {0}", e);
                    }
            else
                foreach (GridClientFuture req in reqs)
                    req.Fail(() => {
                        throw new GridClientClosedException("Failed to perform request (connection was closed" +
                            " before response was received): " + ServerAddress);
                    });

            Dbg.WriteLine("Connection closed: " + ServerAddress);
        }

        /**
         * <summary>
         * Closes client only if there are no pending requests in map.</summary>
         *
         * <returns>Idle timeout.</returns>
         * <returns><c>True</c> if client was closed.</returns>
         */
        override public bool CloseIfIdle(TimeSpan timeout) {
            return closed;
        }

        /**
         * <summary>
         * Creates new future and passes it to the MakeJettyRequest.</summary>
         *
         * <param name="cacheName">Cache name.</param>
         * <param name="cacheFlags">Cache flags.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <param name="args">Request parameters.</param>
         * <param name="converter">Response converter to pass into generated future.</param>
         * <returns>Future.</returns>
         * <exception cref="GridClientClosedException">If client was manually closed.</exception>
         */
        private IGridClientFuture<T> MakeJettyRequest<T>(Guid destNodeId, String cacheName, ISet<GridClientCacheFlag> cacheFlags, IDictionary<String, Object> args, Func<Object, T> converter) {
            if (cacheName != null)
                args.Add("cacheName", cacheName);

            if (cacheFlags != null && cacheFlags.Count > 0)
                args.Add("cacheFlags", "" + encodeCacheFlags(cacheFlags));


            return MakeJettyRequest<T>(destNodeId, args, converter);
        }

        /**
         * <summary>
         * Creates new future and passes it to the MakeJettyRequest.</summary>
         *
         * <param name="args">Request parameters.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <param name="converter">Response converter to pass into generated future.</param>
         * <returns>Future.</returns>
         * <exception cref="GridClientClosedException">If client was manually closed.</exception>
         */
        private IGridClientFuture<T> MakeJettyRequest<T>(Guid destNodeId, IDictionary<String, Object> args, Func<Object, T> converter) {
            Dbg.Assert(args != null);
            Dbg.Assert(args.ContainsKey("cmd"));

            if (destNodeId != null && destNodeId != Guid.Empty)
                args.Add("destId", destNodeId.ToString());

            var fut = new GridClientFuture<T>();

            fut.DoneConverter = converter;

            busyLock.AcquireReaderLock(Timeout.Infinite);

            try {
                if (closed)
                    throw new GridClientConnectionResetException("Failed to perform request (connection was closed" +
                        " before request is sent): " + ServerAddress);

                lock (pendingReqs) {
                    pendingReqs.Add(fut);
                }

                U.Async(() => {
                    try {
                        OnResponse(args, fut, LoadString(args, sessionToken));
                    }
                    catch (Exception e) {
                        fut.Fail(() => {
                            throw new GridClientException(e.Message, e);
                        });
                    }
                });

                return fut;
            }
            finally {
                busyLock.ReleaseReaderLock();
            }
        }

        /**
         * <summary>
         * Perform server request with specified request parameters.</summary>
         *
         * <param name="args">Request parameters.</param>
         * <param name="sesTok">Session token or null to send credentials.</param>
         * <returns>Server response in a string format.</returns>
         */
        private String LoadString(IDictionary<String, Object> args, String sesTok) {
            IDictionary<String, Object> hdr = new Dictionary<String, Object>();
            IDictionary<String, Object> body = new Dictionary<String, Object>(args);

            hdr["clientId"] = ClientId.ToString();
            hdr["reqId"] = Interlocked.Increment(ref reqIdCntr) + "";

            if (!String.IsNullOrEmpty(sesTok))
                hdr["sessionToken"] = sesTok;
            else if (Credentials != null)
                body["cred"] = Credentials;

            // Build request uri.
            Uri uri = new Uri((SslCtx == null ? "http://" : "https://") + 
                ServerAddress + "/gridgain?" + EncodeParams(hdr));

            var client = new WebClient();

            client.Headers.Add("Accept-Charset", "UTF-8");
            client.Headers.Add("Content-Type", "application/x-www-form-urlencoded");

            try {
                return client.UploadString(uri, "POST", EncodeParams(body).ToString());
            }
            catch (WebException e) {
                throw new GridClientException("Failed to perforn HTTP request.", e);
            }
        }

        /**
         * <summary>
         * Encode parameters map into "application/x-www-form-urlencoded" format.</summary>
         *
         * <param name="args">Parameters map to encode.</param>
         * <returns>Form-encoded parameters.</returns>
         */
        private StringBuilder EncodeParams(IDictionary<String, Object> args) {
            StringBuilder data = new StringBuilder();

            foreach (var e in args) {
                String val = e.Value as String;

                if (val == null)
                    throw new ArgumentException("Http connection supports only string arguments in requests" +
                        ", while received [key=" + e.Key + ", value=" + e.Value + "]");

                if (data.Length > 0)
                    data.Append('&');
                
                data.Append(HttpUtility.UrlEncode(e.Key, Encoding.UTF8)).Append('=')
                    .Append(HttpUtility.UrlEncode(val, Encoding.UTF8));
            }

            return data;
        }

        /**
         * <summary>
         * Handle server response.</summary>
         *
         * <param name="args">Parameters map.</param>
         * <param name="fut">Future to use.</param>
         * <param name="str">Downloaded string response.</param>
         */
        private void OnResponse(IDictionary<String, Object> args, GridClientFuture fut, String str) {
            try {
                JavaScriptSerializer s = new JavaScriptSerializer();

                // Parse json response.
                var json = (IDictionary<String, Object>)s.Deserialize(str, typeof(Object));

                // Recover status.
                GridClientResponseStatus statusCode = GridClientResponse.FindByCode((int)json["successStatus"]);

                // Retry with credentials, if authentication failed.
                if (statusCode == GridClientResponseStatus.AuthFailure) {
                    // Reset session token.
                    sessionToken = null;

                    // Re-send request with credentials and without session token.
                    str = LoadString(args, null);

                    // Parse json response.
                    json = (IDictionary<String, Object>)s.Deserialize(str, typeof(IDictionary<String, Object>));

                    // Recover status.
                    statusCode = GridClientResponse.FindByCode((int) json["successStatus"]);
                }

                Object o;
                String errorMsg = null;

                if (json.TryGetValue("error", out o))
                    errorMsg = (String)o;

                if (String.IsNullOrEmpty(errorMsg))
                    errorMsg = "Unknown server error";

                if (statusCode == GridClientResponseStatus.AuthFailure) {
                    // Close this connection.
                    Close(false);

                    throw new GridClientAuthenticationException("Client authentication failed " +
                        "[clientId=" + ClientId + ", srvAddr=" + ServerAddress + ", errMsg=" + errorMsg + ']');
                }

                if (statusCode == GridClientResponseStatus.Failed)
                    throw new GridClientException(errorMsg);

                if (statusCode != GridClientResponseStatus.Success)
                    throw new GridClientException("Unsupported response status code: " + statusCode);

                // Update session token only on success and auth-failed responses.
                if (json.TryGetValue("sessionToken", out o))
                    sessionToken = o == null ? null : o.ToString();

                fut.Done(json["response"]);
            }
            catch (Exception e) {
                fut.Fail(() => {
                    throw new GridClientException(e.Message, e);
                });
            }
            finally {
                lock (pendingReqs) {
                    pendingReqs.Remove(fut);
                }
            }
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CachePutAll<TKey, TVal>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, IDictionary<TKey, TVal> entries, Guid destNodeId) {
            Dbg.Assert(entries != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "putall");

            int i = 1;

            foreach (KeyValuePair<TKey, TVal> e in entries) {
                args.Add("k" + i, e.Key);
                args.Add("v" + i, e.Value);

                i++;
            }

            return MakeJettyRequest<Boolean>(destNodeId, cacheName, cacheFlags, args, o => (bool)o);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<TVal> CacheGet<TKey, TVal>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, TKey key, Guid destNodeId) {
            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "get");
            args.Add("key", key);

            return MakeJettyRequest<TVal>(destNodeId, cacheName, cacheFlags, args, o => (TVal)o);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CachePut<TKey, TVal>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, TKey key, TVal val, Guid destNodeId) {
            return MakeCacheRequest<TKey, TVal>(destNodeId, cacheName, cacheFlags, "put", key, val);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<IDictionary<K, V>> CacheGetAll<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, IEnumerable<K> keys, Guid destNodeId) {
            Dbg.Assert(keys != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "getall");

            int i = 1;

            foreach (K key in keys)
                args.Add("k" + i++, key);

            return MakeJettyRequest<IDictionary<K, V>>(destNodeId, cacheName, cacheFlags, args, o => AsMap<K, V>(o));
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CacheRemove<K>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, Guid destNodeId) {
            return MakeCacheRequest<K, Object>(destNodeId, cacheName, cacheFlags, "rmv", key, null);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CacheRemoveAll<K>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, IEnumerable<K> keys, Guid destNodeId) {
            Dbg.Assert(keys != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "rmvall");

            int i = 1;

            foreach (K key in keys)
                args.Add("k" + i++, key);

            return MakeJettyRequest<Boolean>(destNodeId, cacheName, cacheFlags, args, o => false);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CacheReplace<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, V val, Guid destNodeId) {
            return MakeCacheRequest(destNodeId, cacheName, cacheFlags, "rep", key, val);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CacheAppend<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, V val, Guid destNodeId) {
            return MakeCacheRequest(destNodeId, cacheName, cacheFlags, "append", key, val);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CachePrepend<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, V val, Guid destNodeId) {
            return MakeCacheRequest(destNodeId, cacheName, cacheFlags, "prepend", key, val);
        }

        /** <summary>Make cache request.</summary> */
        private IGridClientFuture<Boolean> MakeCacheRequest<TKey, TVal>(Guid destNodeId, String cacheName, ISet<GridClientCacheFlag> cacheFlags, String op, TKey key, TVal val) {
            Dbg.Assert(op != null);
            Dbg.Assert(key != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", op);
            args.Add("key", key);

            if (val != null)
                args.Add("val", val);

            return MakeJettyRequest<Boolean>(destNodeId, cacheName, cacheFlags, args, o => (bool)o);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<Boolean> CacheCompareAndSet<K, V>(String cacheName, ISet<GridClientCacheFlag> cacheFlags, K key, V val1, V val2, Guid destNodeId) {
            Dbg.Assert(key != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "cas");
            args.Add("key", key);

            if (val1 != null)
                args.Add("val1", val1);

            if (val2 != null)
                args.Add("val2", val2);

            return MakeJettyRequest<Boolean>(destNodeId, cacheName, cacheFlags, args, o => (bool)o);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<IGridClientDataMetrics> CacheMetrics(String cacheName, ISet<GridClientCacheFlag> cacheFlags, Guid destNodeId) {
            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "cache");

            return MakeJettyRequest<IGridClientDataMetrics>(destNodeId, cacheName, cacheFlags, args, o => parseCacheMetrics((IDictionary<String, Object>)o));
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<T> Execute<T>(String taskName, Object arg, Guid destNodeId) {
            Dbg.Assert(taskName != null);

            IDictionary<String, Object> paramsMap = new Dictionary<String, Object>();

            paramsMap.Add("cmd", "exe");
            paramsMap.Add("name", taskName);

            if (arg != null)
                paramsMap.Add("p1", arg);

            return MakeJettyRequest<T>(destNodeId, paramsMap, o => {
                var json = (IDictionary<String, Object>)o;

                return (T)json["result"];
            });
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<IGridClientNode> Node(Guid id, bool includeAttrs, bool includeMetrics, Guid destNodeId) {
            Dbg.Assert(id != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("id", id.ToString());

            return Node(args, includeAttrs, includeMetrics, destNodeId);
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<IGridClientNode> Node(String ip, bool includeAttrs, bool includeMetrics, Guid destNodeId) {
            Dbg.Assert(ip != null);

            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("ip", ip);

            return Node(args, includeAttrs, includeMetrics, destNodeId);
        }

        /**
         * <summary>
         * Requests for the node.</summary>
         *
         * <param name="args">Request parameters.</param>
         * <param name="includeAttrs">Whether to include attributes.</param>
         * <param name="includeMetrics">Whether to include metrics.</param>
         * <param name="destNodeId">Node ID to route request to.</param>
         * <returns>Node.</returns>
         */
        private IGridClientFuture<IGridClientNode> Node(IDictionary<String, Object> args, bool includeAttrs, bool includeMetrics, Guid destNodeId) {
            Dbg.Assert(args != null);

            args.Add("cmd", "node");
            args.Add("attr", includeAttrs.ToString());
            args.Add("mtr", includeMetrics.ToString());

            return MakeJettyRequest<IGridClientNode>(destNodeId, args, o => {
                GridClientNodeImpl node = JsonBeanToNode((IDictionary<String, Object>)o);

                if (node != null)
                    Top.UpdateNode(node);

                return node;
            });
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<IList<IGridClientNode>> Topology(bool includeAttrs, bool includeMetrics, Guid destNodeId) {
            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "top");
            args.Add("attr", includeAttrs.ToString());
            args.Add("mtr", includeMetrics.ToString());

            return MakeJettyRequest<IList<IGridClientNode>>(destNodeId, args, o => {
                var json = o as Object[];

                if (json == null)
                    throw new ArgumentException("Expected a JSON array [cls=" + o.GetType() + ", res=" + o + ']');

                IList<IGridClientNode> nodeList = new List<IGridClientNode>(json.Length);

                foreach (var item in json)
                    nodeList.Add(JsonBeanToNode((IDictionary<String, Object>)item));

                Top.UpdateTopology(nodeList);

                return nodeList;
            });
        }

        /** <inheritdoc /> */
        override public IGridClientFuture<IList<String>> Log(String path, int fromLine, int toLine, Guid destNodeId) {
            IDictionary<String, Object> args = new Dictionary<String, Object>();

            args.Add("cmd", "log");

            if (path != null)
                args.Add("path", path);

            args.Add("from", fromLine + "");
            args.Add("to", toLine + "");

            return MakeJettyRequest<IList<String>>(destNodeId, args, o => {
                var log = o as Object[];

                if (log == null)
                    return null;

                var res = new List<String>(log.Length);

                foreach (var i in log)
                    res.Add(i as String);

                return res;
            });
        }

        /**
         * <summary>
         * Creates client node impl from json object representation.</summary>
         *
         * <param name="json">JSONObject (possibly JSONNull).</param>
         * <returns>Converted client node.</returns>
         */
        private GridClientNodeImpl JsonBeanToNode(IDictionary<String, Object> json) {
            Guid nodeId = Guid.Parse(json["nodeId"].ToString());

            GridClientNodeImpl node = new GridClientNodeImpl(nodeId);

            node.TcpAddresses.AddAll<String>(AsList<String>(json["tcpAddresses"]));
            node.TcpHostNames.AddAll<String>(AsList<String>(json["tcpHostNames"]));
            node.TcpPort = (int)json["tcpPort"];
            node.ConsistentId = json["consistentId"];
            node.ReplicaCount = (int)json["replicaCount"];

            IDictionary<String, GridClientCacheMode> caches = new GridClientNullDictionary<String, GridClientCacheMode>();

            if (json.ContainsKey("caches")) {
                IDictionary<String, String> rawCaches = AsMap<String, String>(json["caches"]);

                Object dfltCacheMode;

                if (json.TryGetValue("defaultCacheMode", out dfltCacheMode)) {
                    String mode = dfltCacheMode as String;

                    if (!String.IsNullOrEmpty(mode)) {
                        rawCaches = rawCaches.ToNullable();

                        rawCaches.Add(null, mode);
                    }
                }

                caches = parseCacheModes(rawCaches);
            }

            if (caches.Count > 0)
                node.Caches.AddAll<KeyValuePair<String, GridClientCacheMode>>(caches);

            Object o;
            if (json.TryGetValue("attributes", out o) && o != null) {
                var map = AsMap<String, Object>(o);

                if (map.Count > 0)
                    node.Attributes.AddAll<KeyValuePair<String, Object>>(map);
            }

            if (json.TryGetValue("metrics", out o) && o != null) {
                var map = AsMap<String, Object>(o);

                if (map.Count > 0)
                    node.Metrics = parseNodeMetrics(AsMap<String, Object>(o));
            }

            return node;
        }

        /**
         * <summary>
         * Convert json object to list.</summary>
         *
         * <param name="json">Json object to convert.</param>
         * <returns>Resulting list.</returns>
         */
        private static IList<T> AsList<T>(Object json) {
            if (json == null)
                return null;

            IList<T> list = new List<T>();

            foreach (var o in (sc.IEnumerable)json)
                list.Add((T)o);

            return list;
        }

        /**
         * <summary>
         * Convert json object to map.</summary>
         *
         * <param name="json">Json object to convert.</param>
         * <returns>Resulting map.</returns>
         */
        private static IDictionary<K, V> AsMap<K, V>(Object json) {
            return AsMap<K, V>(json, v => (V)v);
        }

        /**
         * <summary>
         * Convert json object to list.</summary>
         *
         * <param name="json">Json object to convert.</param>
         * <param name="cast">Casting callback for each value in the resulting map.</param>
         * <returns>Resulting map.</returns>
         */
        private static IDictionary<K, V> AsMap<K, V>(Object json, Func<Object, V> cast) {
            if (json == null)
                return null;

            IDictionary<K, V> map = new Dictionary<K, V>();

            foreach (var o in (IDictionary<String, Object>)json)
                try {
                    map.Add((K)(Object)o.Key, cast(o.Value));
                }
                catch (Exception e) {
                    Console.Out.WriteLine("e=" + e);

                    throw;
                }

            return map;
        }
    }
}
