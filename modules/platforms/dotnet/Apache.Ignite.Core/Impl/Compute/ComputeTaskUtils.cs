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

namespace Apache.Ignite.Core.Impl.Compute
{
    using System;
    using System.Collections.Generic;
    using System.Diagnostics;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Binary;

    /// <summary>
    /// Shared compute task utilities.
    /// </summary>
    internal static class ComputeTaskUtils
    {
        /// <summary>
        /// Writes job collection.
        /// </summary>
        /// <returns>Job handle list</returns>
        internal static List<long> WriteJobs<T>(BinaryWriter writer, ICollection<IComputeJob<T>> list)
        {
            Debug.Assert(writer != null && list != null);

            writer.WriteInt(list.Count);

            var jobHandles = new List<long>(list.Count);
            var ignite = writer.Marshaller.Ignite;

            try
            {
                foreach (var entry in list)
                {
                    var job = new ComputeJobHolder(ignite, entry.ToNonGeneric());

                    var jobHandle = ignite.HandleRegistry.Allocate(job);

                    jobHandles.Add(jobHandle);

                    writer.WriteLong(jobHandle);
                    writer.WriteObject(job);
                }
            }
            catch (Exception)
            {
                foreach (var handle in jobHandles)
                {
                    ignite.HandleRegistry.Release(handle);
                }

                throw;
            }

            return jobHandles;
        }

        /// <summary>
        /// Writes job map.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="map">Map</param>
        /// <returns>Job handle list.</returns>
        internal static List<long> WriteJobs<T>(BinaryWriter writer, IDictionary<IComputeJob<T>, IClusterNode> map)
        {
            Debug.Assert(writer != null && map != null);

            writer.WriteInt(map.Count); // Amount of mapped jobs.

            var jobHandles = new List<long>(map.Count);
            var ignite = writer.Marshaller.Ignite;

            try
            {
                foreach (KeyValuePair<IComputeJob<T>, IClusterNode> mapEntry in map)
                {
                    var job = new ComputeJobHolder(ignite, mapEntry.Key.ToNonGeneric());

                    IClusterNode node = mapEntry.Value;

                    var jobHandle = ignite.HandleRegistry.Allocate(job);

                    jobHandles.Add(jobHandle);

                    writer.WriteLong(jobHandle);

                    if (node.IsLocal)
                        writer.WriteBoolean(false); // Job is not serialized.
                    else
                    {
                        writer.WriteBoolean(true); // Job is serialized.
                        writer.WriteObject(job);
                    }

                    writer.WriteGuid(node.Id);
                }
            }
            catch (Exception)
            {
                foreach (var handle in jobHandles)
                    ignite.HandleRegistry.Release(handle);

                throw;
            }

            return jobHandles;
        }
    }
}