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
        public static ClientOperationType? ToPublicOperationsType(this ClientOp op)
        {
            switch (op)
            {
                case ClientOp.CacheGetOrCreateWithName:
                case ClientOp.CacheGetOrCreateWithConfiguration:
                    return ClientOperationType.CacheGetOrCreate;

                case ClientOp.CACHE_CREATE_WITH_CONFIGURATION:
                case ClientOp.CACHE_CREATE_WITH_NAME:
                    return ClientOperationType.CACHE_CREATE;

                case ClientOp.CACHE_PUT:
                    return ClientOperationType.CACHE_PUT;

                case ClientOp.CACHE_GET:
                    return ClientOperationType.CACHE_GET;

                case ClientOp.CACHE_GET_NAMES:
                    return ClientOperationType.CACHE_GET_NAMES;

                case ClientOp.CACHE_DESTROY:
                    return ClientOperationType.CACHE_DESTROY;

                case ClientOp.CACHE_CONTAINS_KEY:
                    return ClientOperationType.CACHE_CONTAINS_KEY;

                case ClientOp.CACHE_CONTAINS_KEYS:
                    return ClientOperationType.CACHE_CONTAINS_KEYS;

                case ClientOp.CACHE_GET_CONFIGURATION:
                    return ClientOperationType.CACHE_GET_CONFIGURATION;

                case ClientOp.CACHE_GET_SIZE:
                    return ClientOperationType.CACHE_GET_SIZE;

                case ClientOp.CACHE_PUT_ALL:
                    return ClientOperationType.CACHE_PUT_ALL;

                case ClientOp.CACHE_GET_ALL:
                    return ClientOperationType.CACHE_GET_ALL;

                case ClientOp.CACHE_REPLACE_IF_EQUALS:
                case ClientOp.CACHE_REPLACE:
                    return ClientOperationType.CACHE_REPLACE;

                case ClientOp.CACHE_REMOVE_KEY:
                case ClientOp.CACHE_REMOVE_IF_EQUALS:
                    return ClientOperationType.CACHE_REMOVE_ONE;

                case ClientOp.CACHE_REMOVE_KEYS:
                    return ClientOperationType.CACHE_REMOVE_MULTIPLE;

                case ClientOp.CACHE_REMOVE_ALL:
                    return ClientOperationType.CACHE_REMOVE_EVERYTHING;

                case ClientOp.CACHE_GET_AND_PUT:
                    return ClientOperationType.CACHE_GET_AND_PUT;

                case ClientOp.CACHE_GET_AND_REMOVE:
                    return ClientOperationType.CACHE_GET_AND_REMOVE;

                case ClientOp.CACHE_GET_AND_REPLACE:
                    return ClientOperationType.CACHE_GET_AND_REPLACE;

                case ClientOp.CACHE_PUT_IF_ABSENT:
                    return ClientOperationType.CACHE_PUT_IF_ABSENT;

                case ClientOp.CACHE_GET_AND_PUT_IF_ABSENT:
                    return ClientOperationType.CACHE_GET_AND_PUT_IF_ABSENT;

                case ClientOp.CACHE_CLEAR:
                    return ClientOperationType.CACHE_CLEAR_EVERYTHING;

                case ClientOp.CACHE_CLEAR_KEY:
                    return ClientOperationType.CACHE_CLEAR_ONE;

                case ClientOp.CACHE_CLEAR_KEYS:
                    return ClientOperationType.CACHE_CLEAR_MULTIPLE;

                case ClientOp.QUERY_SCAN:
                    return ClientOperationType.QUERY_SCAN;

                case ClientOp.QUERY_SQL:
                case ClientOp.QUERY_SQL_FIELDS:
                    return ClientOperationType.QUERY_SQL;

                case ClientOp.QUERY_CONTINUOUS:
                    return ClientOperationType.QUERY_CONTINUOUS;

                case ClientOp.TX_START:
                    return ClientOperationType.TRANSACTION_START;

                case ClientOp.CLUSTER_GET_STATE:
                    return ClientOperationType.CLUSTER_GET_STATE;

                case ClientOp.CLUSTER_CHANGE_STATE:
                    return ClientOperationType.CLUSTER_CHANGE_STATE;

                case ClientOp.CLUSTER_GET_WAL_STATE:
                    return ClientOperationType.CLUSTER_GET_WAL_STATE;

                case ClientOp.CLUSTER_CHANGE_WAL_STATE:
                    return ClientOperationType.CLUSTER_CHANGE_WAL_STATE;

                case ClientOp.CLUSTER_GROUP_GET_NODE_IDS:
                case ClientOp.CLUSTER_GROUP_GET_NODE_INFO:
                    return ClientOperationType.CLUSTER_GROUP_GET_NODES;

                case ClientOp.COMPUTE_TASK_EXECUTE:
                    return ClientOperationType.COMPUTE_TASK_EXECUTE;

                case ClientOp.SERVICE_INVOKE:
                    return ClientOperationType.SERVICE_INVOKE;

                case ClientOp.SERVICE_GET_DESCRIPTORS:
                    return ClientOperationType.SERVICE_GET_DESCRIPTORS;

                case ClientOp.SERVICE_GET_DESCRIPTOR:
                    return ClientOperationType.SERVICE_GET_DESCRIPTOR;

                default:
                    return null;
            }
        }
    }
}
