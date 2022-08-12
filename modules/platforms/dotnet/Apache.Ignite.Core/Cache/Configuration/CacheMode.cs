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

namespace Apache.Ignite.Core.Cache.Configuration
{
    /// <summary>
    /// Caching modes.
    /// </summary>
    public enum CacheMode
    {
        /// <summary>
        /// Specifies fully replicated cache behavior. In this mode all the keys are distributed
        /// to all participating nodes. 
        /// </summary>
        Replicated = 1,

        /// <summary>
        /// Specifies partitioned cache behaviour. In this mode the overall
        /// key set will be divided into partitions and all partitions will be split
        /// equally between participating nodes. 
        /// <para />
        /// Note that partitioned cache is always fronted by local 'near' cache which stores most recent data. 
        /// </summary>
        Partitioned = 2
    }
}