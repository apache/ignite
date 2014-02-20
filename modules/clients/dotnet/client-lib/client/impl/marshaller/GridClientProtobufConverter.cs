// @csharp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Client.Impl.Marshaller {
    using System;
    using sc = System.Collections;
    using System.Collections.Generic;
    using GridGain.Client.Impl.Message;
    using GridGain.Client.Impl.Protobuf;
    using Google.ProtocolBuffers;
    using GridGain.Client.Util;

    using U = GridGain.Client.Util.GridClientUtils;

    /** <summary>Client messages converter from grid client's format to protobuf one.</summary> */
    internal static class GridClientProtobufConverter {
        /** <summary>Empty (default) guid.</summary> */
        private static readonly Guid ZERO_GUID = new Guid(/* Zero UUID */);

        /**
         * <summary>
         * Converts Guid to a byte array.</summary>
         *
         * <param name="id">Guid to convert.</param>
         * <returns>Converted bytes.</returns>
         */
        private static ByteString WrapGuid(Guid id) {
            return ByteString.CopyFrom(U.ToBytes(id));
        }

        /**
         * <summary>
         * Converts map entry to a key-value pair.</summary>
         *
         * <param name="key">Map entry key.</param>
         * <param name="val">Map entry value.</param>
         * <returns>Key-value pair.</returns>
         */
        private static KeyValue WrapEntry(Object key, Object val) {
            return KeyValue.CreateBuilder()
                .SetKey(WrapObject(key))
                .SetValue(WrapObject(val))
                .Build();
        }

        /**
         * <summary>
         * Converts map into protocol message.</summary>
         *
         * <param name="map">Map to convert.</param>
         * <returns>Protocol message map.</returns>
         */
        private static Map WrapMap(sc::IDictionary map) {
            Map.Builder builder = Map.CreateBuilder();

            foreach (sc::DictionaryEntry pair in map)
                builder.AddEntry(WrapEntry(pair.Key, pair.Value));

            return builder.Build();
        }

        /**
         * <summary>
         * Converts map into protocol message.</summary>
         *
         * <param name="map">Map to convert.</param>
         * <returns>Protocol message map.</returns>
         */
        private static Map WrapMap<TKey, TVal>(IDictionary<TKey, TVal> map) {
            Map.Builder builder = Map.CreateBuilder();

            foreach (KeyValuePair<TKey, TVal> pair in map)
                builder.AddEntry(WrapEntry(pair.Key, pair.Value));

            return builder.Build();
        }

        /**
         * <summary>
         * Converts collection to the list of values.</summary>
         *
         * <param name="col">Collection to convert.</param>
         * <returns>Collection of the values.</returns>
         */
        private static Collection WrapCollection(sc::IEnumerable col) {
            Collection.Builder builder = Collection.CreateBuilder();

            foreach (Object o in col)
                builder.AddItem(WrapObject(o));

            return builder.Build();
        }

        /**
         * <summary>
         * Converts java object to a protocol-understandable format.</summary>
         *
         * <param name="val">Value to convert.</param>
         * <returns>Wrapped protocol message object.</returns>
         */
        public static ObjectWrapper WrapObject(Object val) {
            ObjectWrapper.Builder builder = ObjectWrapper.CreateBuilder();

            // Primitives.

            if (val == null)
                builder
                    .SetType(ObjectWrapperType.NONE)
                    .SetBinary(ByteString.Empty);
            else if (val is bool)
                builder
                    .SetType(ObjectWrapperType.BOOL)
                    .SetBinary(ByteString.CopyFrom(new byte[] { (byte)((bool)val == true ? 1 : 0) }));
            else if (val is byte)
                builder
                    .SetType(ObjectWrapperType.BYTE)
                    .SetBinary(ByteString.CopyFrom(new byte[] { (byte)val }));
            else if (val is short)
                builder
                    .SetType(ObjectWrapperType.SHORT)
                    .SetBinary(ByteString.CopyFrom(U.ToBytes((short)val)));
            else if (val is int)
                builder
                    .SetType(ObjectWrapperType.INT32)
                    .SetBinary(ByteString.CopyFrom(U.ToBytes((int)val)));
            else if (val is long)
                builder
                    .SetType(ObjectWrapperType.INT64)
                    .SetBinary(ByteString.CopyFrom(U.ToBytes((long)val)));
            else if (val is float)
                builder
                    .SetType(ObjectWrapperType.FLOAT)
                    .SetBinary(ByteString.CopyFrom(U.ToBytes((float)val)));
            else if (val is double)
                builder
                    .SetType(ObjectWrapperType.DOUBLE)
                    .SetBinary(ByteString.CopyFrom(U.ToBytes((double)val)));
            else if (val is string)
                builder
                    .SetType(ObjectWrapperType.STRING)
                    .SetBinary(ByteString.CopyFromUtf8((string)val));
            else if (val is Guid)
                builder
                    .SetType(ObjectWrapperType.UUID)
                    .SetBinary(WrapGuid((Guid)val));
            else if (val is byte[])
                builder
                    .SetType(ObjectWrapperType.BYTES)
                    .SetBinary(ByteString.CopyFrom((byte[])val));

            // Common objects.

            else if (val is sc::IDictionary)
                // Note! Map's check should goes BEFORE collections check due to IDictionary is inherited from ICollection.
                builder
                    .SetType(ObjectWrapperType.MAP)
                    .SetBinary(WrapMap((sc::IDictionary)val).ToByteString());
            else if (val is sc::ICollection)
                builder
                    .SetType(ObjectWrapperType.COLLECTION)
                    .SetBinary(WrapCollection((sc::ICollection)val).ToByteString());

            // Requests.

            else if (val is GridClientAuthenticationRequest)
                builder
                    .SetType(ObjectWrapperType.AUTH_REQUEST)
                    .SetBinary(WrapAuthRequest((GridClientAuthenticationRequest)val).ToByteString());
            else if (val is GridClientCacheRequest)
                builder
                    .SetType(ObjectWrapperType.CACHE_REQUEST)
                    .SetBinary(WrapCacheRequest((GridClientCacheRequest)val).ToByteString());
            else if (val is GridClientTaskRequest)
                builder
                    .SetType(ObjectWrapperType.TASK_REQUEST)
                    .SetBinary(WrapTaskRequest((GridClientTaskRequest)val).ToByteString());
            else if (val is GridClientLogRequest)
                builder
                    .SetType(ObjectWrapperType.LOG_REQUEST)
                    .SetBinary(WrapLogRequest((GridClientLogRequest)val).ToByteString());
            else if (val is GridClientTopologyRequest)
                builder
                    .SetType(ObjectWrapperType.TOPOLOGY_REQUEST)
                    .SetBinary(WrapTopologyRequest((GridClientTopologyRequest)val).ToByteString());

            // Responses.

            else if (val is GridClientResponse)
                builder
                    .SetType(ObjectWrapperType.RESPONSE)
                    .SetBinary(WrapResultBean((GridClientResponse)val).ToByteString());
            else if (val is GridClientNodeBean)
                builder
                    .SetType(ObjectWrapperType.NODE_BEAN)
                    .SetBinary(WrapNode((GridClientNodeBean)val).ToByteString());
            else if (val is GridClientTaskResultBean)
                builder
                    .SetType(ObjectWrapperType.TASK_BEAN)
                    .SetBinary(WrapTaskResult((GridClientTaskResultBean)val).ToByteString());

            // To-string conversion for special cases.

            else if (val is Enum)
                builder
                    .SetType(ObjectWrapperType.STRING)
                    .SetBinary(ByteString.CopyFromUtf8(Enum.GetName(val.GetType(), val)));
            else if (val is System.Net.IPAddress)
                builder
                    .SetType(ObjectWrapperType.STRING)
                    .SetBinary(ByteString.CopyFromUtf8(val.ToString()));

            // In all other cases.

            else
                throw new ArgumentException("Failed to serialize object (object serialization" +
                    " of given type is not supported): " + val.GetType());

            return builder.Build();
        }

        /**
         * <summary>
         * Converts node bean to protocol message.</summary>
         *
         * <param name="node">Node bean to convert.</param>
         * <returns>Converted message.</returns>
         */
        private static ProtoNodeBean WrapNode(GridClientNodeBean node) {
            ProtoNodeBean.Builder builder = ProtoNodeBean.CreateBuilder()
                .SetNodeId(WrapGuid(node.NodeId))
                .SetTcpPort(node.TcpPort)
                .SetJettyPort(node.JettyPort)
                .SetConsistentId(WrapObject(node.ConsistentId))
                .SetReplicaCount(node.ReplicaCount);

            if (node.TcpAddresses != null)
                foreach (String addr in node.TcpAddresses)
                    builder.AddTcpAddress(addr);

            if (node.TcpHostNames != null)
                foreach (String addr in node.TcpHostNames)
                    builder.AddTcpHostName(addr);

            if (node.JettyAddresses != null)
                foreach (String addr in node.JettyAddresses)
                    builder.AddJettyAddress(addr);

            if (node.JettyHostNames != null)
                foreach (String addr in node.JettyHostNames)
                    builder.AddJettyHostName(addr);

            if (node.Caches != null)
                builder.SetCaches(WrapMap(node.Caches));

            if (node.Attributes != null)
                builder.SetAttributes(WrapMap(node.Attributes));

            if (node.Metrics != null)
                builder.SetMetrics(WrapMetrics(node.Metrics));

            return builder.Build();
        }

        /**
         * <summary>
         * Converts metrics bean to protocol message.</summary>
         *
         * <param name="m">Metrics bean to convert.</param>
         * <returns>Converted message.</returns>
         */
        private static ProtoNodeMetricsBean WrapMetrics(IDictionary<String, Object> m) {
            throw new InvalidOperationException("Operation not supported for metrics:" + m);
        }

        /**
         * <summary>
         * Wraps task result bean into a protocol message.</summary>
         *
         * <param name="bean">Task result that need to be wrapped.</param>
         * <returns>Wrapped message.</returns>
         */
        private static ProtoTaskBean WrapTaskResult(GridClientTaskResultBean bean) {
            ProtoTaskBean.Builder builder = ProtoTaskBean.CreateBuilder()
                .SetTaskId(bean.TaskId)
                .SetFinished(bean.IsFinished);

            if (bean.Error != null)
                builder.SetError(bean.Error);

            if (bean.Result != null)
                builder.SetResultBean(WrapObject(bean.Result));

            return builder.Build();
        }

        /**
         * <summary>
         * Wraps any request into a protocol message.</summary>
         *
         * <param name="req">Request service information (headers).</param>
         * <param name="bean">Data bean to send.</param>
         * <returns>Wrapped message.</returns>
         */
        private static ProtoRequest WrapRequest(GridClientRequest req, IMessage bean) {
            var builder = ProtoRequest.CreateBuilder()
                //.SetRequestId(req.RequestId)
                //.SetClientId(WrapGuid(req.ClientId))
                .SetBody(bean.ToByteString());

            if (req.SessionToken != null)
                builder.SetSessionToken(ByteString.CopyFrom(req.SessionToken));

            return builder.Build();
        }

        /**
         * <summary>
         * Wraps authentication request into a protocol message.</summary>
         *
         * <param name="req">Authentication request that need to be wrapped.</param>
         * <returns>Wrapped message.</returns>
         */
        private static ProtoRequest WrapAuthRequest(GridClientAuthenticationRequest req) {
            ProtoAuthenticationRequest.Builder builder = ProtoAuthenticationRequest.CreateBuilder()
                .SetCredentials(WrapObject(req.Credentials));

            return WrapRequest(req, builder.Build());
        }

        /**
         * <summary>
         * Wraps cache request into a protocol message.</summary>
         *
         * <param name="req">Cache request that need to be wrapped.</param>
         * <returns>Wrapped message.</returns>
         */
        private static ProtoRequest WrapCacheRequest(GridClientCacheRequest req) {
            ProtoCacheRequest.Builder builder = ProtoCacheRequest.CreateBuilder()
                .SetOperation((ProtoCacheRequest.Types.GridCacheOperation)req.Operation);

            if (req.CacheName != null)
                builder.SetCacheName(req.CacheName);

            if (req.CacheFlags != 0)
                builder.SetCacheFlagsOn(req.CacheFlags);

            if (req.Key != null)
                builder.SetKey(WrapObject(req.Key));

            if (req.Value != null)
                builder.SetValue(WrapObject(req.Value));

            if (req.Value2 != null)
                builder.SetValue2(WrapObject(req.Value2));

            if (req.Values != null)
                builder.SetValues(WrapMap(req.Values));

            return WrapRequest(req, builder.Build());
        }

        /**
         * <summary>
         * Wraps log request into a protocol message.</summary>
         *
         * <param name="req">Log request that need to be wrapped.</param>
         * <returns>Wrapped message.</returns>
         */
        private static ProtoRequest WrapLogRequest(GridClientLogRequest req) {
            ProtoLogRequest.Builder builder = ProtoLogRequest.CreateBuilder()
                .SetFrom(req.From)
                .SetTo(req.To);

            if (req.Path != null)
                builder.SetPath(req.Path);

            return WrapRequest(req, builder.Build());
        }

        /**
         * <summary>
         * Wraps task request into a protocol message.</summary>
         *
         * <param name="req">Task request that need to be wrapped.</param>
         * <returns>Wrapped message.</returns>
         */
        private static ProtoRequest WrapTaskRequest(GridClientTaskRequest req) {
            Object args = req.Argument;

            ProtoTaskRequest.Builder builder = ProtoTaskRequest.CreateBuilder()
                .SetTaskName(req.TaskName)
                .SetArgument(WrapObject(args));

            return WrapRequest(req, builder.Build());
        }

        /**
         * <summary>
         * Wraps topology request into a protocol message.</summary>
         *
         * <param name="req">Topology request that need to be wrapped.</param>
         * <returns>Wrapped message.</returns>
         */
        private static ProtoRequest WrapTopologyRequest(GridClientTopologyRequest req) {
            ProtoTopologyRequest.Builder builder = ProtoTopologyRequest.CreateBuilder()
                .SetIncludeAttributes(req.IncludeAttributes)
                .SetIncludeMetrics(req.IncludeMetrics);

            if (req.NodeId != null && !req.NodeId.Equals(ZERO_GUID))
                builder.SetNodeId(req.NodeId.ToString());

            if (req.NodeIP != null)
                builder.SetNodeIp(req.NodeIP);

            return WrapRequest(req, builder.Build());
        }

        /**
         * <summary>
         * Wraps results bean into a protocol message.</summary>
         *
         * <param name="bean">Results bean that need to be wrapped.</param>
         * <returns>Wrapped message.</returns>
         */
        private static ProtoResponse WrapResultBean(GridClientResponse bean) {
            ProtoResponse.Builder builder = ProtoResponse.CreateBuilder()
                //.SetRequestId(bean.RequestId)
                //.SetClientId(WrapGuid(bean.ClientId))
                .SetStatus((int) bean.Status);

            if (bean.ErrorMessage != null)
                builder.SetErrorMessage(bean.ErrorMessage);

            if (bean.Result != null)
                builder.SetResultBean(WrapObject(bean.Result));

            if (bean.SessionToken != null)
                builder.SetSessionToken(ByteString.CopyFrom(bean.SessionToken));

            return builder.Build();
        }
    }
}
