﻿/*
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
    using System.Diagnostics;
    using System.IO;
    using System.Linq;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Cache.Affinity;
    using Apache.Ignite.Core.Cache.Affinity.Rendezvous;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Memory;

    /// <summary>
    /// Affinity function read/write methods.
    /// </summary>
    internal static class AffinityFunctionSerializer
    {
        /** */
        private const byte TypeCodeNull = 0;

        /** */
        private const byte TypeCodeRendezvous = 2;

        /** */
        private const byte TypeCodeUser = 3;

        /// <summary>
        /// Writes the instance.
        /// </summary>
        internal static void Write(IBinaryRawWriter writer, IAffinityFunction fun, object userFuncOverride = null)
        {
            Debug.Assert(writer != null);

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

            var p = fun as RendezvousAffinityFunction;

            if (p != null)
            {
                writer.WriteByte(TypeCodeRendezvous);
                writer.WriteInt(p.Partitions);
                writer.WriteBoolean(p.ExcludeNeighbors);

                var overrideFlags = GetOverrideFlags(p.GetType());
                writer.WriteByte((byte) overrideFlags);

                // Do not write user func if there is nothing overridden
                WriteUserFunc(writer, overrideFlags != UserOverrides.None ? fun : null, userFuncOverride);

                WriteBackupFilter(writer, p);
            }
            else
            {
                writer.WriteByte(TypeCodeUser);
                writer.WriteInt(fun.Partitions);
                writer.WriteBoolean(false); // Exclude neighbors
                writer.WriteByte((byte) UserOverrides.All);
                WriteUserFunc(writer, fun, userFuncOverride);
            }
        }

        /// <summary>
        /// Reads the instance.
        /// </summary>
        internal static IAffinityFunction Read(IBinaryRawReader reader)
        {
            Debug.Assert(reader != null);

            var typeCode = reader.ReadByte();

            if (typeCode == TypeCodeNull)
                return null;

            var partitions = reader.ReadInt();
            var exclNeighbors = reader.ReadBoolean();
            var overrideFlags = (UserOverrides)reader.ReadByte();
            var userFunc = reader.ReadObjectEx<IAffinityFunction>();

            if (userFunc != null)
            {
                Debug.Assert(overrideFlags != UserOverrides.None);

                var rendezvous = userFunc as RendezvousAffinityFunction;
                if (rendezvous != null)
                {
                    rendezvous.Partitions = partitions;
                    rendezvous.ExcludeNeighbors = exclNeighbors;
                    rendezvous.AffinityBackupFilter = ReadBackupFilter(reader);
                }

                return userFunc;
            }

            Debug.Assert(overrideFlags == UserOverrides.None);

            if (typeCode != TypeCodeRendezvous)
                throw new InvalidOperationException("Invalid AffinityFunction type code: " + typeCode);

            return new RendezvousAffinityFunction
            {
                Partitions = partitions,
                ExcludeNeighbors = exclNeighbors,
                AffinityBackupFilter = ReadBackupFilter(reader)
            };
        }

        /// <summary>
        /// Writes the partitions assignment to a stream.
        /// </summary>
        /// <param name="parts">The parts.</param>
        /// <param name="stream">The stream.</param>
        /// <param name="marsh">The marshaller.</param>
        internal static void WritePartitions(IEnumerable<IEnumerable<IClusterNode>> parts,
            PlatformMemoryStream stream, Marshaller marsh)
        {
            Debug.Assert(parts != null);
            Debug.Assert(stream != null);
            Debug.Assert(marsh != null);

            IBinaryRawWriter writer = marsh.StartMarshal(stream);

            var partCnt = 0;
            writer.WriteInt(partCnt); // reserve size

            foreach (var part in parts)
            {
                if (part == null)
                    throw new IgniteException("IAffinityFunction.AssignPartitions() returned invalid partition: null");

                partCnt++;

                var nodeCnt = 0;
                var cntPos = stream.Position;
                writer.WriteInt(nodeCnt); // reserve size

                foreach (var node in part)
                {
                    nodeCnt++;
                    writer.WriteGuid(node.Id);
                }

                var endPos = stream.Position;
                stream.Seek(cntPos, SeekOrigin.Begin);
                stream.WriteInt(nodeCnt);
                stream.Seek(endPos, SeekOrigin.Begin);
            }

            stream.SynchronizeOutput();
            stream.Seek(0, SeekOrigin.Begin);
            writer.WriteInt(partCnt);
        }

        /// <summary>
        /// Reads the partitions assignment from a stream.
        /// </summary>
        /// <param name="stream">The stream.</param>
        /// <param name="marsh">The marshaller.</param>
        /// <returns>Partitions assignment.</returns>
        internal static IEnumerable<IEnumerable<IClusterNode>> ReadPartitions(IBinaryStream stream, Marshaller marsh)
        {
            Debug.Assert(stream != null);
            Debug.Assert(marsh != null);

            var reader = marsh.StartUnmarshal(stream);

            var partCnt = reader.ReadInt();

            var res = new List<IEnumerable<IClusterNode>>(partCnt);

            for (var i = 0; i < partCnt; i++)
                res.Add(IgniteUtils.ReadNodes(reader));

            return res;
        }

        /// <summary>
        /// Gets the override flags.
        /// </summary>
        private static UserOverrides GetOverrideFlags(Type funcType)
        {
            var res = UserOverrides.None;

            var methods = new[] {UserOverrides.GetPartition, UserOverrides.AssignPartitions, UserOverrides.RemoveNode};

            var map = funcType.GetInterfaceMap(typeof(IAffinityFunction));

            foreach (var method in methods)
            {
                // Find whether user type overrides IAffinityFunction method from AffinityFunctionBase.
                var methodName = method.ToString();

                if (map.TargetMethods.Single(x => x.Name == methodName).DeclaringType != typeof(AffinityFunctionBase))
                    res |= method;
            }

            return res;
        }

        /// <summary>
        /// Writes the user function.
        /// </summary>
        private static void WriteUserFunc(IBinaryRawWriter writer, IAffinityFunction func, object funcOverride)
        {
            if (funcOverride != null)
            {
                writer.WriteObject(funcOverride);
                return;
            }

            writer.WriteObject(func);
        }

        /// <summary>
        /// Reads the backup filter.
        /// </summary>
        private static ClusterNodeAttributeAffinityBackupFilter ReadBackupFilter(IBinaryRawReader reader)
        {
            var attrCount = reader.ReadInt();

            if (attrCount <= 0)
            {
                return null;
            }

            var attrs = new string[attrCount];

            for (var i = 0; i < attrCount; i++)
            {
                attrs[i] = reader.ReadString();
            }

            return new ClusterNodeAttributeAffinityBackupFilter{AttributeNames = attrs};
        }

        /// <summary>
        /// Writes the backup filter.
        /// </summary>
        private static void WriteBackupFilter(IBinaryRawWriter writer, RendezvousAffinityFunction func)
        {
            if (func.AffinityBackupFilter == null)
            {
                writer.WriteInt(-1);
                return;
            }

            var filter = func.AffinityBackupFilter as ClusterNodeAttributeAffinityBackupFilter;

            if (filter == null)
            {
                throw new NotSupportedException(string.Format(
                    "Unsupported RendezvousAffinityFunction.AffinityBackupFilter: '{0}'. " +
                    "Only predefined implementations are supported: '{1}'",
                    func.AffinityBackupFilter.GetType().FullName,
                    typeof(ClusterNodeAttributeAffinityBackupFilter).Name));
            }

            IgniteArgumentCheck.NotNullOrEmpty(filter.AttributeNames,
                "ClusterNodeAttributeAffinityBackupFilter.AttributeNames");

            writer.WriteInt(filter.AttributeNames.Count);

            foreach (var attr in filter.AttributeNames)
            {
                writer.WriteString(attr);
            }
        }

        /// <summary>
        /// Overridden function flags.
        /// </summary>
        [Flags]
        private enum UserOverrides : byte
        {
            None = 0,
            GetPartition = 1,
            RemoveNode = 1 << 1,
            AssignPartitions = 1 << 2,
            All = GetPartition | RemoveNode | AssignPartitions
        }
    }
}
