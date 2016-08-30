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

namespace Apache.Ignite.Core.Cache.Eviction
{
    /// <summary>
    /// Eviction policy based on Least Recently Used (LRU) algorithm with batch eviction support.
    /// <para />
    /// The eviction starts in the following cases: 
    /// The cache size becomes { @code batchSize }
    /// elements greater than the maximum size;
    /// The size of cache entries in bytes becomes greater than the maximum memory size;
    /// The size of cache entry calculates as sum of key size and value size.
    /// <para />
    /// Note: Batch eviction is enabled only if maximum memory limit isn't set.
    /// <para />
    /// This implementation is very efficient since it does not create any additional
    /// table-like data structures. The LRU ordering information is
    /// maintained by attaching ordering metadata to cache entries.
    /// </summary>
    public class LruEvictionPolicy : EvictionPolicyBase
    {
        // No-op.
    }
}