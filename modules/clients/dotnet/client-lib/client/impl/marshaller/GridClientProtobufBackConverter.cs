/* @csharp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Marshaller {
    using System;
    using System.Linq;
    using sc = System.Collections;
    using System.Collections.Generic;
    using GridGain.Client.Impl.Message;
    using GridGain.Client.Impl.Protobuf;
    using Google.ProtocolBuffers;
    using GridGain.Client.Util;

    using A = GridGain.Client.Util.GridClientArgumentCheck;
    using U = GridGain.Client.Util.GridClientUtils;
    using Dbg = System.Diagnostics.Debug;

    using ProtoCacheOp = GridGain.Client.Impl.Protobuf.ProtoCacheRequest.Types.GridCacheOperation;

    /**
     * <summary>
     * Client messages converter from protobuf format to grid client's one.</summary>
     */
    internal static class GridClientProtobufBackConverter {
        /**
         * <summary>
         * Converts byte array into Guid.</summary>
         *
         * <param name="id">Guid to convert.</param>
         * <returns>Converted Guid.</returns>
         */
        private static Guid WrapGuid(ByteString id) {
            Dbg.Assert(id != null, "id != null");
            Dbg.Assert(id.Length == 16, "id.Length == " + id.Length);

            return U.BytesToGuid(id.ToByteArray(), 0);
        }

        /**
         * <summary>
         * Converts protocol message into map.</summary>
         *
         * <param name="map">Protocol message map to convert.</param>
         * <returns>Converted map.</returns>
         */
        private static sc::IDictionary WrapMap(Map map) {
            sc::IDictionary res = new sc::Hashtable();

            foreach (KeyValue entry in map.EntryList)
                res.Add(WrapObject(entry.Key), WrapObject(entry.Value));

            return res;
        }

        /**
         * <summary>
         * Converts protocol message into map.</summary>
         *
         * <param name="map">Protocol message map to convert.</param>
         * <returns>Converted map.</returns>
         */
        private static IDictionary<TKey, TVal> WrapMap<TKey, TVal>(Map map) {
            IDictionary<TKey, TVal> res = new Dictionary<TKey, TVal>();

            foreach (KeyValue entry in map.EntryList) {
                var key = WrapObject<TKey>(entry.Key);
                var val = WrapObject<TVal>(entry.Value);

                // C# hack for null keys.
                if (key == null)
                    res = res.ToNullable();

                res.Add(key, val);
            }

            return res;
        }

        /**
         * <summary>
         * Converts collection to the list of values.</summary>
         *
         * <param name="col">Collection to convert.</param>
         * <returns>Collection of the values.</returns>
         */
        private static sc::ArrayList WrapCollection(Collection col) {
            sc::ArrayList res = new sc::ArrayList(col.ItemCount);

            foreach (ObjectWrapper o in col.ItemList)
                res.Add(WrapObject(o));

            return res;
        }

        /**
         * <summary>
         * Converts protocol object into java object.</summary>
         *
         * <param name="obj">Protocol message object to convert.</param>
         * <returns>Recovered object.</returns>
         */
        public static T WrapObject<T>(ObjectWrapper obj) {
            return (T)WrapObject(obj);
        }

        /**
         * <summary>
         * Converts protocol object into object.</summary>
         *
         * <param name="val">Protocol message object to convert into value.</param>
         * <returns>Recovered object.</returns>
         */
        public static Object WrapObject(ObjectWrapper val) {
            byte[] bin = val.Binary.ToByteArray();

            // Primitives.

            switch (val.Type) {
                case ObjectWrapperType.NONE:
                    return null;

                case ObjectWrapperType.BOOL:
                    Dbg.Assert(bin.Length == 1, "bin.Length == 1");

                    return bin[0] != 0;

                case ObjectWrapperType.BYTE:
                    Dbg.Assert(bin.Length == 1, "bin.Length == 1");

                    return bin[0];

                case ObjectWrapperType.SHORT:
                    Dbg.Assert(bin.Length == 2, "bin.Length == 2");

                    return U.BytesToInt16(bin, 0);

                case ObjectWrapperType.INT32:
                    Dbg.Assert(bin.Length == 4, "bin.Length == 4");

                    return U.BytesToInt32(bin, 0);

                case ObjectWrapperType.INT64:
                    Dbg.Assert(bin.Length == 8, "bin.Length == 8");

                    return U.BytesToInt64(bin, 0);

                case ObjectWrapperType.FLOAT:
                    Dbg.Assert(bin.Length == 4, "bin.Length == 4");

                    return U.BytesToSingle(bin, 0);

                case ObjectWrapperType.DOUBLE:
                    Dbg.Assert(bin.Length == 8, "bin.Length == 8");

                    return U.BytesToDouble(bin, 0);

                case ObjectWrapperType.BYTES:
                    return bin;

                case ObjectWrapperType.UUID:
                    return WrapGuid(val.Binary);

                case ObjectWrapperType.STRING:
                    return val.Binary.ToStringUtf8();

                case ObjectWrapperType.COLLECTION:
                    return WrapCollection(Collection.ParseFrom(bin));

                case ObjectWrapperType.MAP:
                    return WrapMap(Map.ParseFrom(bin));

                case ObjectWrapperType.AUTH_REQUEST:
                    return WrapAuthRequest(ProtoRequest.ParseFrom(bin));

                case ObjectWrapperType.CACHE_REQUEST:
                    return WrapCacheRequest(ProtoRequest.ParseFrom(bin));

                case ObjectWrapperType.TASK_REQUEST:
                    return WrapTaskRequest(ProtoRequest.ParseFrom(bin));

                case ObjectWrapperType.LOG_REQUEST:
                    return WrapLogRequest(ProtoRequest.ParseFrom(bin));

                case ObjectWrapperType.TOPOLOGY_REQUEST:
                    return WrapTopologyRequest(ProtoRequest.ParseFrom(bin));

                case ObjectWrapperType.RESPONSE:
                    return WrapResponse(ProtoResponse.ParseFrom(bin));

                case ObjectWrapperType.NODE_BEAN:
                    return WrapNode(ProtoNodeBean.ParseFrom(bin));

                case ObjectWrapperType.TASK_BEAN:
                    return WrapTaskResult(ProtoTaskBean.ParseFrom(bin));

                default:
                    throw new ArgumentException("Failed to deserialize object (object deserialization" +
                        " of given type is not supported): " + val.Type);
            }
        }

        /**
         * <summary>
         * Converts node bean to protocol message.</summary>
         *
         * <param name="node">Node bean to convert.</param>
         * <returns>Converted message.</returns>
         */
        private static GridClientNodeBean WrapNode(ProtoNodeBean node) {
            GridClientNodeBean bean = new GridClientNodeBean();

            bean.NodeId = WrapGuid(node.NodeId);

            bean.TcpPort = node.TcpPort;
            bean.JettyPort = node.JettyPort;
            bean.ConsistentId = WrapObject(node.ConsistentId);
            bean.ReplicaCount = node.ReplicaCount;

            bean.TcpAddresses.AddAll(node.TcpAddressList);
            bean.TcpHostNames.AddAll(node.TcpHostNameList);
            bean.JettyAddresses.AddAll(node.JettyAddressList);
            bean.JettyHostNames.AddAll(node.JettyHostNameList);

            if (node.HasCaches)
                bean.Caches.AddAll<KeyValuePair<String, String>>(WrapMap<String, String>(node.Caches));

            if (node.HasAttributes)
                bean.Attributes.AddAll<KeyValuePair<String, Object>>(WrapMap<String, Object>(node.Attributes));

            // if (node.HasMetrics)
               //  bean.Metrics.AddAll<KeyValuePair<String, Object>>(WrapMetrics(node.Metrics));

            return bean;
        }

        /**
         * <summary>
         * Converts metrics bean to protocol message.</summary>
         *
         * <param name="m">Protocol message metrics to convert.</param>
         * <returns>Converted metrics bean.</returns>
         */
        private static IDictionary<String, Object> WrapMetrics(ProtoNodeMetricsBean m) {
            IDictionary<String, Object> metrics = new Dictionary<String, Object>();

            metrics.Add("startTime", m.StartTime);
            metrics.Add("averageActiveJobs", m.AverageActiveJobs);
            metrics.Add("averageCancelledJobs", m.AverageCancelledJobs);
            metrics.Add("averageCpuLoad", m.AverageCpuLoad);
            metrics.Add("averageJobExecuteTime", m.AverageJobExecuteTime);
            metrics.Add("averageJobWaitTime", m.AverageJobWaitTime);
            metrics.Add("averageRejectedJobs", m.AverageRejectedJobs);
            metrics.Add("averageWaitingJobs", m.AverageWaitingJobs);
            metrics.Add("currentActiveJobs", m.CurrentActiveJobs);
            metrics.Add("currentCancelledJobs", m.CurrentCancelledJobs);
            metrics.Add("currentCpuLoad", m.CurrentCpuLoad);
            metrics.Add("currentDaemonThreadCount", m.CurrentDaemonThreadCount);
            metrics.Add("currentIdleTime", m.CurrentIdleTime);
            metrics.Add("currentJobExecuteTime", m.CurrentJobExecuteTime);
            metrics.Add("currentJobWaitTime", m.CurrentJobWaitTime);
            metrics.Add("currentRejectedJobs", m.CurrentRejectedJobs);
            metrics.Add("currentThreadCount", m.CurrentThreadCount);
            metrics.Add("currentWaitingJobs", m.CurrentWaitingJobs);
            metrics.Add("fileSystemFreeSpace", m.FileSystemFreeSpace);
            metrics.Add("fileSystemTotalSpace", m.FileSystemTotalSpace);
            metrics.Add("fileSystemUsableSpace", m.FileSystemUsableSpace);
            metrics.Add("heapMemoryCommitted", m.HeapMemoryCommitted);
            metrics.Add("heapMemoryInitialized", m.HeapMemoryInitialized);
            metrics.Add("heapMemoryMaximum", m.HeapMemoryMaximum);
            metrics.Add("heapMemoryUsed", m.HeapMemoryUsed);
            metrics.Add("lastDataVersion", m.LastDataVersion);
            metrics.Add("lastUpdateTime", m.LastUpdateTime);
            metrics.Add("maximumActiveJobs", m.MaximumActiveJobs);
            metrics.Add("maximumCancelledJobs", m.MaximumCancelledJobs);
            metrics.Add("maximumJobExecuteTime", m.MaximumJobExecuteTime);
            metrics.Add("maximumJobWaitTime", m.MaximumJobWaitTime);
            metrics.Add("maximumRejectedJobs", m.MaximumRejectedJobs);
            metrics.Add("maximumThreadCount", m.MaximumThreadCount);
            metrics.Add("maximumWaitingJobs", m.MaximumWaitingJobs);
            metrics.Add("nodeStartTime", m.NodeStartTime);
            metrics.Add("nonHeapMemoryCommitted", m.NonHeapMemoryCommitted);
            metrics.Add("nonHeapMemoryInitialized", m.NonHeapMemoryInitialized);
            metrics.Add("nonHeapMemoryMaximum", m.NonHeapMemoryMaximum);
            metrics.Add("nonHeapMemoryUsed", m.NonHeapMemoryUsed);
            metrics.Add("totalCancelledJobs", m.TotalCancelledJobs);
            metrics.Add("totalCpus", m.TotalCpus);
            metrics.Add("totalExecutedJobs", m.TotalExecutedJobs);
            metrics.Add("totalIdleTime", m.TotalIdleTime);
            metrics.Add("totalRejectedJobs", m.TotalRejectedJobs);
            metrics.Add("totalStartedThreadCount", m.TotalStartedThreadCount);
            metrics.Add("upTime", m.UpTime);

            return metrics;
        }

        /**
         * <summary>
         * Unwraps protocol message to a task result bean.</summary>
         *
         * <param name="bean">Protocol message to unwrap.</param>
         * <returns>Unwrapped message.</returns>
         */
        private static GridClientTaskResultBean WrapTaskResult(ProtoTaskBean bean) {
            GridClientTaskResultBean res = new GridClientTaskResultBean();

            res.TaskId = bean.TaskId;
            res.IsFinished = bean.Finished;

            if (bean.HasError)
                res.Error = bean.Error;

            if (bean.HasResultBean)
                res.Result = WrapObject(bean.ResultBean);

            return res;
        }

        /**
         * <summary>
         * Wraps any request into a protocol message.</summary>
         *
         * <param name="req">Request service information (headers).</param>
         * <param name="bean">Data bean to send.</param>
         * <returns>Wrapped message.</returns>
         */
        private static T WrapRequest<T>(T bean, ProtoRequest req) where T : GridClientRequest {
            //bean.RequestId = req.RequestId;
            //bean.ClientId = WrapGuid(req.ClientId);
            
            if (req.HasSessionToken)
                bean.SessionToken = req.SessionToken.ToByteArray();

            return bean;
        }

        /**
         * <summary>
         * Wraps authentication request into a protocol message.</summary>
         *
         * <param name="req">Authentication request that need to be wrapped.</param>
         * <returns>Wrapped message.</returns>
         */
        private static GridClientAuthenticationRequest WrapAuthRequest(ProtoRequest req) {
            var data = ProtoAuthenticationRequest.ParseFrom(req.Body);
            var bean = new GridClientAuthenticationRequest(Guid.Empty);

            bean.Credentials = WrapObject(data.Credentials);

            return WrapRequest(bean, req);
        }

        /**
         * <summary>
         * Wraps cache request into a protocol message.</summary>
         *
         * <param name="req">Cache request that need to be wrapped.</param>
         * <returns>Wrapped message.</returns>
         */
        private static GridClientCacheRequest WrapCacheRequest(ProtoRequest req) {
            ProtoCacheRequest data = ProtoCacheRequest.ParseFrom(req.Body);

            GridClientCacheRequest bean = new GridClientCacheRequest((GridClientCacheRequestOperation)data.Operation, Guid.Empty);

            if (data.HasCacheName)
                bean.CacheName = data.CacheName;

            if (data.HasKey)
                bean.Key = WrapObject(data.Key);

            if (data.HasValue)
                bean.Value = WrapObject(data.Value);

            if (data.HasValue2)
                bean.Value2 = WrapObject(data.Value2);

            //if (data.HasValues)
                //bean.Values = WrapMap(data.Values);

            return WrapRequest(bean, req);
        }

        /**
         * <summary>
         * Wraps log request into a protocol message.</summary>
         *
         * <param name="req">Log request that need to be wrapped.</param>
         * <returns>Wrapped message.</returns>
         */
        private static GridClientLogRequest WrapLogRequest(ProtoRequest req) {
            ProtoLogRequest data = ProtoLogRequest.ParseFrom(req.Body);

            GridClientLogRequest bean = new GridClientLogRequest(Guid.Empty);

            if (data.HasPath)
                bean.Path = data.Path;

            if (data.HasFrom)
                bean.From = data.From;

            if (data.HasTo)
                bean.To = data.To;

            return WrapRequest(bean, req);
        }

        /**
         * <summary>
         * Wraps task request into a protocol message.</summary>
         *
         * <param name="req">Task request that need to be wrapped.</param>
         * <returns>Wrapped message.</returns>
         */
        private static GridClientTaskRequest WrapTaskRequest(ProtoRequest req) {
            ProtoTaskRequest data = ProtoTaskRequest.ParseFrom(req.Body);

            GridClientTaskRequest bean = new GridClientTaskRequest(Guid.Empty);

            bean.TaskName = data.TaskName;
            bean.Argument = WrapObject(data.Argument);

            return WrapRequest(bean, req);
        }

        /**
         * <summary>
         * Wraps topology request into a protocol message.</summary>
         *
         * <param name="req">Topology request that need to be wrapped.</param>
         * <returns>Wrapped message.</returns>
         */
        private static GridClientTopologyRequest WrapTopologyRequest(ProtoRequest req) {
            ProtoTopologyRequest data = ProtoTopologyRequest.ParseFrom(req.Body);

            GridClientTopologyRequest bean = new GridClientTopologyRequest(Guid.Empty);
            
            bean.IncludeAttributes = data.IncludeAttributes;
            bean.IncludeMetrics = data.IncludeMetrics;

            if (data.HasNodeId)
                bean.NodeId = Guid.Parse(data.NodeId);

            if (data.HasNodeIp)
                bean.NodeIP = data.NodeIp;

            return WrapRequest(bean, req);
        }

        /**
         * <summary>
         * Wraps protocol message into a results bean.</summary>
         *
         * <param name="data">Protocol message that need to be wrapped.</param>
         * <returns>Wrapped message.</returns>
         */
        private static GridClientResponse WrapResponse(ProtoResponse data) {
            GridClientResponse bean = new GridClientResponse();

            //bean.RequestId = data.RequestId;
            //bean.ClientId = WrapGuid(data.ClientId);
            bean.Status = GridClientResponse.FindByCode(data.Status);

            if (data.HasErrorMessage)
                bean.ErrorMessage = data.ErrorMessage;

            if (data.HasResultBean)
                bean.Result = WrapObject(data.ResultBean);

            if (data.HasSessionToken)
                bean.SessionToken = data.SessionToken.ToByteArray();

            return bean;
        }
    }
}
