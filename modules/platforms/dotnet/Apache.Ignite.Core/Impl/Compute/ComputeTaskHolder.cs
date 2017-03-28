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
    using System.Collections.ObjectModel;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Compute.Closure;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Resource;

    /// <summary>
    /// Compute task holder interface used to avoid generics.
    /// </summary>
    internal interface IComputeTaskHolder
    {
        /// <summary>
        /// Perform map step.
        /// </summary>
        /// <param name="stream">Stream with IN data (topology info) and for OUT data (map result).</param>
        /// <returns>Map with produced jobs.</returns>
        void Map(PlatformMemoryStream stream);

        /// <summary>
        /// Process local job result.
        /// </summary>
        /// <param name="jobId">Job pointer.</param>
        /// <returns>Policy.</returns>
        int JobResultLocal(ComputeJobHolder jobId);

        /// <summary>
        /// Process remote job result.
        /// </summary>
        /// <param name="jobId">Job pointer.</param>
        /// <param name="stream">Stream.</param>
        /// <returns>Policy.</returns>
        int JobResultRemote(ComputeJobHolder jobId, PlatformMemoryStream stream);
        
        /// <summary>
        /// Perform task reduce.
        /// </summary>
        void Reduce();

        /// <summary>
        /// Complete task.
        /// </summary>
        /// <param name="taskHandle">Task handle.</param>
        void Complete(long taskHandle);
        
        /// <summary>
        /// Complete task with error.
        /// </summary>
        /// <param name="taskHandle">Task handle.</param>
        /// <param name="stream">Stream with serialized exception.</param>
        void CompleteWithError(long taskHandle, PlatformMemoryStream stream);
    }

    /// <summary>
    /// Compute task holder.
    /// </summary>
    internal class ComputeTaskHolder<TA, T, TR> : IComputeTaskHolder
    {
        /** Empty results. */
        private static readonly IList<IComputeJobResult<T>> EmptyRes =     
            new ReadOnlyCollection<IComputeJobResult<T>>(new List<IComputeJobResult<T>>());

        /** Compute instance. */
        private readonly ComputeImpl _compute;

        /** Actual task. */
        private readonly IComputeTask<TA, T, TR> _task;

        /** Task argument. */
        private readonly TA _arg;

        /** Results cache flag. */
        private readonly bool _resCache;

        /** Task future. */
        private readonly Future<TR> _fut = new Future<TR>();
                
        /** Jobs whose results are cached. */
        private ISet<object> _resJobs;

        /** Cached results. */
        private IList<IComputeJobResult<T>> _ress;

        /** Handles for jobs which are not serialized right away. */
        private volatile List<long> _jobHandles;
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="compute">Compute.</param>
        /// <param name="task">Task.</param>
        /// <param name="arg">Argument.</param>
        public ComputeTaskHolder(Ignite grid, ComputeImpl compute, IComputeTask<TA, T, TR> task, TA arg)
        {
            _compute = compute;
            _arg = arg;
            _task = task;

            ResourceTypeDescriptor resDesc = ResourceProcessor.Descriptor(task.GetType());

            IComputeResourceInjector injector = task as IComputeResourceInjector;

            if (injector != null)
                injector.Inject(grid);
            else
                resDesc.InjectIgnite(task, grid);

            _resCache = !resDesc.TaskNoResultCache;
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "User code can throw any exception")]
        public void Map(PlatformMemoryStream stream)
        {
            IList<IClusterNode> subgrid;

            ClusterGroupImpl prj = (ClusterGroupImpl)_compute.ClusterGroup;

            var ignite = (Ignite) prj.Ignite;

            // 1. Unmarshal topology info if topology changed.
            var reader = prj.Marshaller.StartUnmarshal(stream);

            if (reader.ReadBoolean())
            {
                long topVer = reader.ReadLong();

                List<IClusterNode> nodes = new List<IClusterNode>(reader.ReadInt());

                int nodesCnt = reader.ReadInt();

                subgrid = new List<IClusterNode>(nodesCnt);

                for (int i = 0; i < nodesCnt; i++)
                {
                    IClusterNode node = ignite.GetNode(reader.ReadGuid());

                    nodes.Add(node);

                    if (reader.ReadBoolean())
                        subgrid.Add(node);
                }

                // Update parent projection to help other task callers avoid this overhead.
                // Note that there is a chance that topology changed even further and this update fails.
                // It means that some of subgrid nodes could have left the Grid. This is not critical
                // for us, because Java will handle it gracefully.
                prj.UpdateTopology(topVer, nodes);
            }
            else
            {
                IList<IClusterNode> nodes = prj.NodesNoRefresh();

                Debug.Assert(nodes != null, "At least one topology update should have occurred.");

                subgrid = IgniteUtils.Shuffle(nodes);
            }

            // 2. Perform map.
            IDictionary<IComputeJob<T>, IClusterNode> map;
            Exception err;

            try
            {
                map = _task.Map(subgrid, _arg);

                err = null;
            }
            catch (Exception e)
            {
                map = null;

                err = e;

                // Java can receive another exception in case of marshalling failure but it is not important.
                Finish(default(TR), e);
            }

            // 3. Write map result to the output stream.
            stream.Reset();
            BinaryWriter writer = prj.Marshaller.StartMarshal(stream);

            try
            {
                if (err == null)
                {
                    writer.WriteBoolean(true); // Success flag.

                    if (map == null)
                        writer.WriteBoolean(false); // Map produced no result.
                    else
                    {
                        writer.WriteBoolean(true); // Map produced result.

                        _jobHandles = WriteJobs(writer, map);
                    }
                }
                else
                {
                    writer.WriteBoolean(false); // Map failed.

                    // Write error as string because it is not important for Java, we need only to print
                    // a message in the log.
                    writer.WriteString("Map step failed [errType=" + err.GetType().Name +
                        ", errMsg=" + err.Message + ']');
                }
            }
            catch (Exception e)
            {
                // Something went wrong during marshaling.
                Finish(default(TR), e);

                stream.Reset();
                
                writer.WriteBoolean(false); // Map failed.
                writer.WriteString(e.Message); // Write error message.
            }
            finally
            {
                prj.Marshaller.FinishMarshal(writer);
            }
        }

        /// <summary>
        /// Writes job map.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="map">Map</param>
        /// <returns>Job handle list.</returns>
        private static List<long> WriteJobs(BinaryWriter writer, IDictionary<IComputeJob<T>, IClusterNode> map)
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

        /** <inheritDoc /> */
        public int JobResultLocal(ComputeJobHolder job)
        {
            return (int)JobResult0(job.JobResult);
        }

        /** <inheritDoc /> */
        [SuppressMessage("ReSharper", "PossibleInvalidOperationException")]
        public int JobResultRemote(ComputeJobHolder job, PlatformMemoryStream stream)
        {
            // 1. Unmarshal result.
            BinaryReader reader = _compute.Marshaller.StartUnmarshal(stream);

            var nodeId = reader.ReadGuid();
            Debug.Assert(nodeId.HasValue);

            bool cancelled = reader.ReadBoolean();

            try
            {
                object err;

                var data = BinaryUtils.ReadInvocationResult(reader, out err);

                // 2. Process the result.
                return (int) JobResult0(new ComputeJobResultImpl(data, (Exception) err, job.Job, nodeId.Value, cancelled));
            }
            catch (Exception e)
            {
                Finish(default(TR), e);

                if (!(e is IgniteException))
                    throw new IgniteException("Failed to process job result: " + e.Message, e);

                throw;
            }
        }
        
        /** <inheritDoc /> */
        public void Reduce()
        {
            try
            {
                TR taskRes = _task.Reduce(_resCache ? _ress : EmptyRes);

                Finish(taskRes, null);
            }
            catch (Exception e)
            {
                Finish(default(TR), e);

                if (!(e is IgniteException))
                    throw new IgniteException("Failed to reduce task: " + e.Message, e);

                throw;
            }
        }

        /** <inheritDoc /> */
        public void Complete(long taskHandle)
        {
            Clean(taskHandle);
        }

        /// <summary>
        /// Complete task with error.
        /// </summary>
        /// <param name="taskHandle">Task handle.</param>
        /// <param name="e">Error.</param>
        public void CompleteWithError(long taskHandle, Exception e)
        {
            Finish(default(TR), e);

            Clean(taskHandle);
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "User object deserialization can throw any exception")]
        public void CompleteWithError(long taskHandle, PlatformMemoryStream stream)
        {
            BinaryReader reader = _compute.Marshaller.StartUnmarshal(stream);

            Exception err;

            try
            {
                err = reader.ReadBoolean()
                    ? reader.ReadObject<BinaryObject>().Deserialize<Exception>()
                    : ExceptionUtils.GetException(_compute.Marshaller.Ignite, reader.ReadString(), reader.ReadString(),
                                                  reader.ReadString());
            }
            catch (Exception e)
            {
                err = new IgniteException("Task completed with error, but it cannot be unmarshalled: " + e.Message, e);
            }

            CompleteWithError(taskHandle, err);
        }

        /// <summary>
        /// Task completion future.
        /// </summary>
        internal Future<TR> Future
        {
            get { return _fut; }
        }

        /// <summary>
        /// Manually set job handles. Used by closures because they have separate flow for map step.
        /// </summary>
        /// <param name="jobHandles">Job handles.</param>
        internal void JobHandles(List<long> jobHandles)
        {
            _jobHandles = jobHandles;
        }

        /// <summary>
        /// Process job result.
        /// </summary>
        /// <param name="res">Result.</param>
        private ComputeJobResultPolicy JobResult0(IComputeJobResult<object> res)
        {
            try
            {
                IList<IComputeJobResult<T>> ress0;

                // 1. Prepare old results.
                if (_resCache)
                {
                    if (_resJobs == null)
                    {
                        _resJobs = new HashSet<object>();

                        _ress = new List<IComputeJobResult<T>>();
                    }

                    ress0 = _ress;
                }
                else
                    ress0 = EmptyRes;

                // 2. Invoke user code.
                var policy = _task.OnResult(new ComputeJobResultGenericWrapper<T>(res), ress0);

                // 3. Add result to the list only in case of success.
                if (_resCache)
                {
                    var job = res.Job.Unwrap();

                    if (!_resJobs.Add(job))
                    {
                        // Duplicate result => find and replace it with the new one.
                        var oldRes = _ress.Single(item => item.Job == job);

                        _ress.Remove(oldRes);
                    }

                    _ress.Add(new ComputeJobResultGenericWrapper<T>(res));
                }

                return policy;
            }
            catch (Exception e)
            {
                Finish(default(TR), e);

                if (!(e is IgniteException))
                    throw new IgniteException("Failed to process job result: " + e.Message, e);

                throw;
            }
        }

        /// <summary>
        /// Finish task.
        /// </summary>
        /// <param name="res">Result.</param>
        /// <param name="err">Error.</param>
        private void Finish(TR res, Exception err)
        {
            _fut.OnDone(res, err);
        }

        /// <summary>
        /// Clean-up task resources.
        /// </summary>
        /// <param name="taskHandle"></param>
        private void Clean(long taskHandle)
        {
            var handles = _jobHandles;

            var handleRegistry = _compute.Marshaller.Ignite.HandleRegistry;

            if (handles != null)
                foreach (var handle in handles) 
                    handleRegistry.Release(handle, true);

            handleRegistry.Release(taskHandle, true);
        }
    }
}
