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
    using System;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Base class for predefined eviction policies.
    /// </summary>
    public abstract class EvictionPolicyBase : IEvictionPolicy
    {
        /// <summary>
        /// Gets or sets the size of the eviction batch.
        /// Batch eviction is enabled only if maximum memory limit isn't set (<see cref="MaxMemorySize"/> == 0).
        /// </summary>
        public int BatchSize { get; set; }

        /// <summary>
        /// Gets or sets the maximum allowed cache size (entry count).
        /// </summary>
        public int MaxSize { get; set; }

        /// <summary>
        /// Gets or sets the maximum allowed cache size in bytes.
        /// </summary>
        public long MaxMemorySize { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="EvictionPolicyBase"/> class.
        /// </summary>
        internal EvictionPolicyBase()
        {
            // No-op.
        }
        
        /// <summary>
        /// Writes to the specified writer.
        /// </summary>
        internal static void Write(IBinaryRawWriter writer, IEvictionPolicy policy)
        {
            if (policy == null)
            {
                writer.WriteByte(0);
                return;
            }

            var p = policy as EvictionPolicyBase;

            if (p == null)
            {
                throw new NotSupportedException(
                    string.Format("Unsupported Eviction Policy: {0}. Only predefined eviction policy types " +
                                  "are supported: {1}, {2}", policy.GetType(), typeof (LruEvictionPolicy),
                        typeof (FifoEvictionPolicy)));
            }

            writer.WriteByte(p is FifoEvictionPolicy ? (byte) 1 : (byte) 2);
        }

        /// <summary>
        /// Reads an instance.
        /// </summary>
        internal static EvictionPolicyBase Read(IBinaryRawReader reader)
        {
            switch (reader.ReadByte())
            {
                case 0: return null;
                case 1: return new FifoEvictionPolicy();
                case 2: return new LruEvictionPolicy();
                default: throw new InvalidOperationException("Unsupported eviction policy.");
            }
        }
    }
}