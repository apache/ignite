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

namespace Apache.Ignite.Core.Client
{
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Retry policy that returns true for all read-only operations that do not modify data.
    /// </summary>
    public sealed class ClientRetryReadPolicy : IClientRetryPolicy
    {
        /** <inheritDoc /> */
        public bool ShouldRetry(IClientRetryPolicyContext context)
        {
            IgniteArgumentCheck.NotNull(context, nameof(context));

            switch (context.Operation)
            {
                case ClientOperationType.CacheGetNames:
                case ClientOperationType.CacheGet:
                case ClientOperationType.CacheContainsKey:
                case ClientOperationType.CacheContainsKeys:
                case ClientOperationType.CacheGetConfiguration:
                case ClientOperationType.CacheGetSize:
                case ClientOperationType.CacheGetAll:
                case ClientOperationType.QueryScan:
                case ClientOperationType.QueryContinuous:
                case ClientOperationType.ClusterGetState:
                case ClientOperationType.ClusterGetWalState:
                case ClientOperationType.ClusterGroupGetNodes:
                case ClientOperationType.ServiceGetDescriptors:
                case ClientOperationType.ServiceGetDescriptor:
                    return true;

                default:
                    return false;
            }
        }
    }
}
