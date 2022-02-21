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

namespace Apache.Ignite.Core.Impl.Client
{
    using Apache.Ignite.Core.Client;

    /// <summary>
    /// Extensions for <see cref="ClientOp"/>.
    /// </summary>
    internal static class ClientOpExtensions
    {
        /// <summary>
        /// Converts the internal op code to a public operation type.
        /// </summary>
        /// <param name="op">Operation code.</param>
        /// <returns>Operation type.</returns>
        public static ClientOperationType? ToPublicOperationsType(this ClientOp op)
        {
            switch (op)
            {
                case ClientOp.CacheGetOrCreateWithName:
                case ClientOp.CacheGetOrCreateWithConfiguration:
                    return ClientOperationType.CacheGetOrCreate;

                case ClientOp.CacheCreateWithConfiguration:
                case ClientOp.CacheCreateWithName:
                    return ClientOperationType.CacheCreate;

                case ClientOp.CachePut:
                    return ClientOperationType.CachePut;

                case ClientOp.CacheGet:
                    return ClientOperationType.CacheGet;

                case ClientOp.CacheGetNames:
                    return ClientOperationType.CacheGetNames;

                case ClientOp.CacheDestroy:
                    return ClientOperationType.CacheDestroy;

                case ClientOp.CacheContainsKey:
                    return ClientOperationType.CacheContainsKey;

                case ClientOp.CacheContainsKeys:
                    return ClientOperationType.CacheContainsKeys;

                case ClientOp.CacheGetConfiguration:
                    return ClientOperationType.CacheGetConfiguration;

                case ClientOp.CacheGetSize:
                    return ClientOperationType.CacheGetSize;

                case ClientOp.CachePutAll:
                    return ClientOperationType.CachePutAll;

                case ClientOp.CacheGetAll:
                    return ClientOperationType.CacheGetAll;

                case ClientOp.CacheReplaceIfEquals:
                case ClientOp.CacheReplace:
                    return ClientOperationType.CacheReplace;

                case ClientOp.CacheRemoveKey:
                case ClientOp.CacheRemoveIfEquals:
                    return ClientOperationType.CacheRemoveOne;

                case ClientOp.CacheRemoveKeys:
                    return ClientOperationType.CacheRemoveMultiple;

                case ClientOp.CacheRemoveAll:
                    return ClientOperationType.CacheRemoveEverything;

                case ClientOp.CacheGetAndPut:
                    return ClientOperationType.CacheGetAndPut;

                case ClientOp.CacheGetAndRemove:
                    return ClientOperationType.CacheGetAndRemove;

                case ClientOp.CacheGetAndReplace:
                    return ClientOperationType.CacheGetAndReplace;

                case ClientOp.CachePutIfAbsent:
                    return ClientOperationType.CachePutIfAbsent;

                case ClientOp.CacheGetAndPutIfAbsent:
                    return ClientOperationType.CacheGetAndPutIfAbsent;

                case ClientOp.CacheClear:
                    return ClientOperationType.CacheClearEverything;

                case ClientOp.CacheClearKey:
                    return ClientOperationType.CacheClearOne;

                case ClientOp.CacheClearKeys:
                    return ClientOperationType.CacheClearMultiple;

                case ClientOp.QueryScan:
                    return ClientOperationType.QueryScan;

                case ClientOp.QuerySql:
                case ClientOp.QuerySqlFields:
                    return ClientOperationType.QuerySql;

                case ClientOp.QueryContinuous:
                    return ClientOperationType.QueryContinuous;

                case ClientOp.TxStart:
                    return ClientOperationType.TransactionStart;

                case ClientOp.ClusterIsActive:
                    return ClientOperationType.ClusterGetState;

                case ClientOp.ClusterChangeState:
                    return ClientOperationType.ClusterChangeState;

                case ClientOp.ClusterGetWalState:
                    return ClientOperationType.ClusterGetWalState;

                case ClientOp.ClusterChangeWalState:
                    return ClientOperationType.ClusterChangeWalState;

                case ClientOp.ClusterGroupGetNodeIds:
                case ClientOp.ClusterGroupGetNodesInfo:
                    return ClientOperationType.ClusterGroupGetNodes;

                case ClientOp.ComputeTaskExecute:
                    return ClientOperationType.ComputeTaskExecute;

                case ClientOp.ServiceInvoke:
                    return ClientOperationType.ServiceInvoke;

                case ClientOp.ServiceGetDescriptors:
                    return ClientOperationType.ServiceGetDescriptors;

                case ClientOp.ServiceGetDescriptor:
                    return ClientOperationType.ServiceGetDescriptor;

                default:
                    return null;
            }
        }
    }
}
