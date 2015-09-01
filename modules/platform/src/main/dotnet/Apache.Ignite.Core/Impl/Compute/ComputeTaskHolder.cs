/*
 *  Copyright (C) GridGain Systems. All Rights Reserved.
 *  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

namespace GridGain.Impl.Compute
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.Diagnostics;
    using System.Diagnostics.CodeAnalysis;
    using System.Linq;
    using Apache.Ignite.Core.Impl.Memory;
    using GridGain.Cluster;
    using GridGain.Common;
    using GridGain.Compute;
    using GridGain.Impl;
    using GridGain.Impl.Cluster;
    using GridGain.Impl.Common;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Resource;

    using U = GridGain.Impl.GridUtils;

    /// <summary>
    /// Compute task holder interface used to avoid generics.
    /// </summary>
    internal interface IComputeTaskHolder
    {
        /// <summary>
        /// Perform map step.
        /// </summary>
        /// <param name="inStream">Stream with IN data (topology info).</param>
        /// <param name="outStream">Stream for OUT data (map result).</param>
        /// <returns>Map with produced jobs.</returns>
        void Map(PlatformMemoryStream inStream, PlatformMemoryStream outStream);

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
    internal class ComputeTaskHolder<A, T, R> : IComputeTaskHolder
    {
        /** Empty results. */
        private static readonly IList<IComputeJobResult<T>> EMPTY_RES =     
            new ReadOnlyCollection<IComputeJobResult<T>>(new List<IComputeJobResult<T>>());

        /** Compute instance. */
        private readonly ComputeImpl compute;

        /** Actual task. */
        private readonly IComputeTask<A, T, R> task;

        /** Task argument. */
        private readonly A arg;

        /** Results cache flag. */
        private readonly bool resCache;

        /** Task future. */
        private readonly IgniteFutureProxy<R> fut = new IgniteFutureProxy<R>();
                
        /** Jobs whose results are cached. */
        private ISet<object> resJobs;

        /** Cached results. */
        private IList<IComputeJobResult<T>> ress;

        /** Handles for jobs which are not serialized right away. */
        private volatile List<long> jobHandles;
        
        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="compute">Compute.</param>
        /// <param name="task">Task.</param>
        /// <param name="arg">Argument.</param>
        public ComputeTaskHolder(GridImpl grid, ComputeImpl compute, IComputeTask<A, T, R> task, A arg)
        {
            this.compute = compute;
            this.arg = arg;
            this.task = task;

            ResourceTypeDescriptor resDesc = ResourceProcessor.Descriptor(task.GetType());

            IComputeResourceInjector injector = task as IComputeResourceInjector;

            if (injector != null)
                injector.Inject(grid);
            else
                resDesc.InjectGrid(task, grid);

            resCache = !resDesc.TaskNoResultCache;
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "User code can throw any exception")]
        public void Map(PlatformMemoryStream inStream, PlatformMemoryStream outStream)
        {
            IList<IClusterNode> subgrid;

            ClusterGroupImpl prj = (ClusterGroupImpl)compute.ClusterGroup;

            var grid = (GridImpl) prj.Grid;

            // 1. Unmarshal topology info if topology changed.
            var reader = prj.Marshaller.StartUnmarshal(inStream);

            if (reader.ReadBoolean())
            {
                long topVer = reader.ReadLong();

                List<IClusterNode> nodes = new List<IClusterNode>(reader.ReadInt());

                int nodesCnt = reader.ReadInt();

                subgrid = new List<IClusterNode>(nodesCnt);

                for (int i = 0; i < nodesCnt; i++)
                {
                    IClusterNode node = grid.GetNode(reader.ReadGuid());

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

                subgrid = GridUtils.Shuffle(nodes);
            }

            // 2. Perform map.
            IDictionary<IComputeJob<T>, IClusterNode> map;
            Exception err;

            try
            {
                map = task.Map(subgrid, arg);

                err = null;
            }
            catch (Exception e)
            {
                map = null;

                err = e;

                // Java can receive another exception in case of marshalling failure but it is not important.
                Finish(default(R), e);
            }

            // 3. Write map result to the output stream.
            PortableWriterImpl writer = prj.Marshaller.StartMarshal(outStream);

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
                        writer.WriteInt(map.Count); // Amount of mapped jobs.

                        var jobHandles = new List<long>(map.Count);

                        foreach (KeyValuePair<IComputeJob<T>, IClusterNode> mapEntry in map)
                        {
                            var job = new ComputeJobHolder(compute.ClusterGroup.Grid as GridImpl, mapEntry.Key.ToNonGeneric());

                            IClusterNode node = mapEntry.Value;

                            var jobHandle = grid.HandleRegistry.Allocate(job);

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

                        this.jobHandles = jobHandles;
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
                Finish(default(R), e);

                outStream.Reset();
                
                writer.WriteBoolean(false); // Map failed.
                writer.WriteString(e.Message); // Write error message.
            }
            finally
            {
                prj.Marshaller.FinishMarshal(writer);
            }
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
            PortableReaderImpl reader = compute.Marshaller.StartUnmarshal(stream);

            Guid nodeId = reader.ReadGuid().Value;
            bool cancelled = reader.ReadBoolean();

            try
            {
                object err;

                var data = PortableUtils.ReadWrappedInvocationResult(reader, out err);

                // 2. Process the result.
                return (int) JobResult0(new ComputeJobResultImpl(data, (Exception) err, job.Job, nodeId, cancelled));
            }
            catch (Exception e)
            {
                Finish(default(R), e);

                if (!(e is GridException))
                    throw new GridException("Failed to process job result: " + e.Message, e);

                throw;
            }
        }
        
        /** <inheritDoc /> */
        public void Reduce()
        {
            try
            {
                R taskRes = task.Reduce(resCache ? ress : EMPTY_RES);

                Finish(taskRes, null);
            }
            catch (Exception e)
            {
                Finish(default(R), e);

                if (!(e is GridException))
                    throw new GridException("Failed to reduce task: " + e.Message, e);

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
            Finish(default(R), e);

            Clean(taskHandle);
        }

        /** <inheritDoc /> */
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "User object deserialization can throw any exception")]
        public void CompleteWithError(long taskHandle, PlatformMemoryStream stream)
        {
            PortableReaderImpl reader = compute.Marshaller.StartUnmarshal(stream);

            Exception err;

            try
            {
                if (reader.ReadBoolean())
                {
                    PortableResultWrapper res = reader.ReadObject<PortableUserObject>()
                        .Deserialize<PortableResultWrapper>();

                    err = (Exception) res.Result;
                }
                else
                    err = ExceptionUtils.GetException(reader.ReadString(), reader.ReadString());
            }
            catch (Exception e)
            {
                err = new GridException("Task completed with error, but it cannot be unmarshalled: " + e.Message, e);
            }

            CompleteWithError(taskHandle, err);
        }

        /// <summary>
        /// Task completion future.
        /// </summary>
        internal IFuture<R> Future
        {
            get { return fut; }
        }

        /// <summary>
        /// Manually set job handles. Used by closures because they have separate flow for map step.
        /// </summary>
        /// <param name="jobHandles">Job handles.</param>
        internal void JobHandles(List<long> jobHandles)
        {
            this.jobHandles = jobHandles;
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
                if (resCache)
                {
                    if (resJobs == null)
                    {
                        resJobs = new HashSet<object>();

                        ress = new List<IComputeJobResult<T>>();
                    }

                    ress0 = ress;
                }
                else
                    ress0 = EMPTY_RES;

                // 2. Invoke user code.
                var policy = task.Result(new ComputeJobResultGenericWrapper<T>(res), ress0);

                // 3. Add result to the list only in case of success.
                if (resCache)
                {
                    var job = res.Job().Unwrap();

                    if (!resJobs.Add(job))
                    {
                        // Duplicate result => find and replace it with the new one.
                        var oldRes = ress.Single(item => item.Job() == job);

                        ress.Remove(oldRes);
                    }

                    ress.Add(new ComputeJobResultGenericWrapper<T>(res));
                }

                return policy;
            }
            catch (Exception e)
            {
                Finish(default(R), e);

                if (!(e is GridException))
                    throw new GridException("Failed to process job result: " + e.Message, e);

                throw;
            }
        }

        /// <summary>
        /// Finish task.
        /// </summary>
        /// <param name="res">Result.</param>
        /// <param name="err">Error.</param>
        private void Finish(R res, Exception err)
        {
            fut.OnDone(res, err);
        }

        /// <summary>
        /// Clean-up task resources.
        /// </summary>
        /// <param name="taskHandle"></param>
        private void Clean(long taskHandle)
        {
            var handles = jobHandles;

            var handleRegistry = compute.Marshaller.Grid.HandleRegistry;

            if (handles != null)
                foreach (var handle in handles) 
                    handleRegistry.Release(handle, true);

            handleRegistry.Release(taskHandle, true);
        }
    }
}
