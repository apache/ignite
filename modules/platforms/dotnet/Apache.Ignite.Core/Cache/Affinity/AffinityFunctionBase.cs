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
    using System;
    using System.ComponentModel;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity.Fair;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Impl.Handle;

    /// <summary>
    /// Base class for predefined affinity functions.
    /// </summary>
    public abstract class AffinityFunctionBase : IAffinityFunction
    {
        /** */
        private const byte TypeCodeNull = 0;

        /** */
        private const byte TypeCodeFair = 1;

        /** */
        private const byte TypeCodeRendezvous = 2;

        /** */
        private const byte TypeCodeUser = 3;

        /// <summary> The default value for <see cref="PartitionCount"/> property. </summary>
        public const int DefaultPartitionCount = 1024;

        /// <summary>
        /// Gets or sets the total number of partitions.
        /// </summary>
        [DefaultValue(DefaultPartitionCount)]
        public int PartitionCount { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether to exclude same-host-neighbors from being backups of each other.
        /// </summary>
        public bool ExcludeNeighbors { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="AffinityFunctionBase"/> class.
        /// </summary>
        internal AffinityFunctionBase()
        {
            PartitionCount = DefaultPartitionCount;
        }

        /// <summary>
        /// Reads the instance.
        /// </summary>
        internal static IAffinityFunction Read(IBinaryRawReader reader)
        {
            AffinityFunctionBase fun;

            var typeCode = reader.ReadByte();
            switch (typeCode)
            {
                case TypeCodeNull:
                    return null;
                case TypeCodeFair:
                    fun = new FairAffinityFunction();
                    break;
                case TypeCodeRendezvous:
                    fun = new RendezvousAffinityFunction();
                    break;
                default:
                    throw new InvalidOperationException("Invalid AffinityFunction type code: " + typeCode);
            }

            fun.PartitionCount = reader.ReadInt();
            fun.ExcludeNeighbors = reader.ReadBoolean();

            return fun;
        }

        /// <summary>
        /// Writes the instance.
        /// </summary>
        internal static void Write(IBinaryRawWriter writer, IAffinityFunction fun, HandleRegistry handleRegistry)
        {
            if (fun == null)
            {
                writer.WriteByte(TypeCodeNull);
                return;
            }

            var p = fun as AffinityFunctionBase;

            if (p != null)
            {
                writer.WriteByte(p is FairAffinityFunction ? TypeCodeFair : TypeCodeRendezvous);
                writer.WriteInt(p.PartitionCount);
                writer.WriteBoolean(p.ExcludeNeighbors);
            }
            else
            {
                // User-defined function
                writer.WriteByte(TypeCodeUser);

                // TODO: Deal with handle. It is possible that handle already exists.
                // Wrap user func in something else to pair it with handle id.
            }
        }
    }
}
