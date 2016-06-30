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

namespace Apache.Ignite.Core.Impl.Cache.Affinity
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Affinity.Fair;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Memory;

    /// <summary>
    /// Affinity function read/write methods.
    /// </summary>
    internal static class AffinityFunctionSerializer
    {
        /** */
        private const byte TypeCodeNull = 0;

        /** */
        private const byte TypeCodeFair = 1;

        /** */
        private const byte TypeCodeRendezvous = 2;

        /** */
        private const byte TypeCodeUser = 3;

        /// <summary>
        /// Writes the instance.
        /// </summary>
        internal static void Write(IBinaryRawWriter writer, IAffinityFunction fun)
        {
            if (fun == null)
            {
                writer.WriteByte(TypeCodeNull);
                return;
            }

            // 1) Type code
            // 2) Partitions
            // 3) ExcludeNeighbors
            // 4) Override flags
            // 5) User object

            var p = fun as AffinityFunctionBase;

            if (p != null)
            {
                writer.WriteByte(p is FairAffinityFunction ? TypeCodeFair : TypeCodeRendezvous);
                writer.WriteInt(p.Partitions);
                writer.WriteBoolean(p.ExcludeNeighbors);
                writer.WriteByte((byte)GetOverrideFlags(p.GetType())); // Override flags
                // TODO: User func only if there are override flags
                WriteUserFunc(writer, fun); // User func
            }
            else
            {
                writer.WriteByte(TypeCodeUser);
                writer.WriteInt(fun.Partitions); // partition count is written once and can not be changed.
                writer.WriteBoolean(false); // Exclude neighbors
                writer.WriteByte((byte)UserOverrides.None); // Override flags
                WriteUserFunc(writer, fun); // User func
            }
        }

        /// <summary>
        /// Reads the instance.
        /// </summary>
        internal static IAffinityFunction Read(IBinaryRawReader reader)
        {
            // TODO
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
                case TypeCodeUser:
                    var f = reader.ReadObject<IAffinityFunction>();
                    reader.ReadInt(); // skip partition count

                    return f;
                default:
                    throw new InvalidOperationException("Invalid AffinityFunction type code: " + typeCode);
            }

            fun.Partitions = reader.ReadInt();
            fun.ExcludeNeighbors = reader.ReadBoolean();

            return fun;
        }


        /// <summary>
        /// Writes the partitions assignment to a stream.
        /// </summary>
        /// <param name="parts">The parts.</param>
        /// <param name="outStream">The out stream.</param>
        /// <param name="marsh">The marshaller.</param>
        internal static void WritePartitions(IEnumerable<IEnumerable<IClusterNode>> parts,
            PlatformMemoryStream outStream, Marshaller marsh)
        {
            var writer = marsh.StartMarshal(outStream);

            var partCnt = 0;
            writer.WriteInt(partCnt); // reserve size

            foreach (var part in parts)
            {
                if (part == null)
                    throw new IgniteException("IAffinityFunction.AssignPartitions() returned invalid partition: null");

                partCnt++;

                var nodeCnt = 0;
                var cntPos = outStream.Position;
                writer.WriteInt(nodeCnt); // reserve size

                foreach (var node in part)
                {
                    nodeCnt++;
                    writer.WriteGuid(node.Id);
                }

                var endPos = outStream.Position;
                outStream.Seek(cntPos, SeekOrigin.Begin);
                outStream.WriteInt(nodeCnt);
                outStream.Seek(endPos, SeekOrigin.Begin);
            }

            outStream.SynchronizeOutput();
            outStream.Seek(0, SeekOrigin.Begin);
            writer.WriteInt(partCnt);
        }

        /// <summary>
        /// Gets the override flags.
        /// </summary>
        private static UserOverrides GetOverrideFlags(Type funcType)
        {
            // TODO
            return UserOverrides.None;
        }

        /// <summary>
        /// Writes the user function.
        /// </summary>
        private static void WriteUserFunc(IBinaryRawWriter writer, IAffinityFunction fun)
        {
            if (!fun.GetType().IsSerializable)
                throw new IgniteException("AffinityFunction should be serializable.");

            writer.WriteObject(fun);
        }

        [Flags]
        private enum UserOverrides : byte
        {
            None = 0,
            GetPartition = 1,
            RemoveNode = 1 << 1,
            AssignPartitions = 1 << 2
        }
    }
}
