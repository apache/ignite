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

namespace Apache.Ignite.Core.Cache.Affinity
{
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Base class for predefined affinity functions.
    /// </summary>
    public abstract class AffinityFunctionBase : IAffinityFunction
    {
        // TODO: Partitions, ExcludeNeighbors
        internal AffinityFunctionBase()
        {
            // TODO: Defaults
        }

        internal static IAffinityFunction Read(IBinaryRawReader reader)
        {
            throw new System.NotImplementedException();
        }

        internal static void Write(IBinaryRawWriter writer, IAffinityFunction affinityFunction)
        {
            throw new System.NotImplementedException();
        }
    }
}
