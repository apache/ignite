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
    using System.ComponentModel;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Configuration;

    /// <summary>
    /// Base class for predefined eviction policies.
    /// </summary>
    public abstract class EvictionPolicyBase : IEvictionPolicy
    {
        /// <summary> Default batch cache size. </summary>
        public const int DefaultBatchSize = 1;
        
        /// <summary> Default max cache size. </summary>
        public const int DefaultMaxSize = CacheConfiguration.DefaultCacheSize;

        /// <summary> Default max cache size in bytes. </summary>
        public const long DefaultMaxMemorySize = 0;

        /// <summary>
        /// Gets or sets the size of the eviction batch.
        /// Batch eviction is enabled only if maximum memory limit isn't set (<see cref="MaxMemorySize"/> == 0).
        /// </summary>
        [DefaultValue(DefaultBatchSize)]
        public int BatchSize { get; set; }

        /// <summary>
        /// Gets or sets the maximum allowed cache size (entry count).
        /// 0 for unlimited.
        /// </summary>
        [DefaultValue(DefaultMaxSize)]
        public int MaxSize { get; set; }

        /// <summary>
        /// Gets or sets the maximum allowed cache size in bytes.
        /// 0 for unlimited.
        /// </summary>
        [DefaultValue(DefaultMaxMemorySize)]
        public long MaxMemorySize { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="EvictionPolicyBase"/> class.
        /// </summary>
        internal EvictionPolicyBase()
        {
            BatchSize = DefaultBatchSize;
            MaxSize = DefaultMaxSize;
            MaxMemorySize = DefaultMaxMemorySize;
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

            writer.WriteInt(p.BatchSize);
            writer.WriteInt(p.MaxSize);
            writer.WriteLong(p.MaxMemorySize);
        }

        /// <summary>
        /// Reads an instance.
        /// </summary>
        internal static EvictionPolicyBase Read(IBinaryRawReader reader)
        {
            EvictionPolicyBase p;

            switch (reader.ReadByte())
            {
                case 0:
                    return null;
                case 1:
                    p = new FifoEvictionPolicy();
                    break;
                case 2:
                    p = new LruEvictionPolicy();
                    break;
                default:
                    throw new InvalidOperationException("Unsupported eviction policy.");
            }

            p.BatchSize = reader.ReadInt();
            p.MaxSize = reader.ReadInt();
            p.MaxMemorySize = reader.ReadLong();

            return p;
        }
    }
}