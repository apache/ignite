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
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using GridGain.Common;
    using GridGain.Compute;
    using GridGain.Impl.Cluster;
    using GridGain.Impl.Portable;
    using GridGain.Impl.Resource;
    using GridGain.Portable;

    /// <summary>
    /// Holder for user-provided compute job.
    /// </summary>
    internal class ComputeJobHolder : IPortableWriteAware
    {
        /** Actual job. */
        private readonly IComputeJob job;
        
        /** Owning grid. */
        private readonly GridImpl grid;

        /** Result (set for local jobs only). */
        private volatile ComputeJobResultImpl jobRes;

        /// <summary>
        /// Default ctor for marshalling.
        /// </summary>
        /// <param name="reader"></param>
        public ComputeJobHolder(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl) reader.RawReader();

            grid = reader0.Marshaller.Grid;

            job = PortableUtils.ReadPortableOrSerializable<IComputeJob>(reader0);
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="job">Job.</param>
        public ComputeJobHolder(GridImpl grid, IComputeJob job)
        {
            this.grid = grid;
            this.job = job;
        }

        /// <summary>
        /// Executes local job.
        /// </summary>
        /// <param name="cancel">Cancel flag.</param>
        public void ExecuteLocal(bool cancel)
        {
            object res;
            bool success;

            Execute0(cancel, out res, out success);

            jobRes = new ComputeJobResultImpl(
                success ? res : null, 
                success ? null : res as Exception, 
                job, 
                grid.LocalNode.Id, 
                cancel
            );
        }

        /// <summary>
        /// Execute job serializing result to the stream.
        /// </summary>
        /// <param name="cancel">Whether the job must be cancelled.</param>
        /// <param name="stream">Stream.</param>
        public void ExecuteRemote(PlatformMemoryStream stream, bool cancel)
        {
            // 1. Execute job.
            object res;
            bool success;

            Execute0(cancel, out res, out success);

            // 2. Try writing result to the stream.
            ClusterGroupImpl prj = grid.ClusterGroup;

            PortableWriterImpl writer = prj.Marshaller.StartMarshal(stream);

            try
            {
                // 3. Marshal results.
                PortableUtils.WriteWrappedInvocationResult(writer, success, res);
            }
            finally
            {
                // 4. Process metadata.
                prj.FinishMarshal(writer);
            }
        }

        /// <summary>
        /// Cancel the job.
        /// </summary>
        public void Cancel()
        {
            job.Cancel();
        }

        /// <summary>
        /// Serialize the job to the stream.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <returns>True if successfull.</returns>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "User job can throw any exception")]
        internal bool Serialize(IPortableStream stream)
        {
            ClusterGroupImpl prj = grid.ClusterGroup;

            PortableWriterImpl writer = prj.Marshaller.StartMarshal(stream);

            try
            {
                writer.Write(this);

                return true;
            }
            catch (Exception e)
            {
                writer.WriteString("Failed to marshal job [job=" + job + ", errType=" + e.GetType().Name +
                    ", errMsg=" + e.Message + ']');

                return false;
            }
            finally
            {
                // 4. Process metadata.
                prj.FinishMarshal(writer);
            }
        }

        /// <summary>
        /// Job.
        /// </summary>
        internal IComputeJob Job
        {
            get { return job; }
        }

        /// <summary>
        /// Job result.
        /// </summary>
        internal ComputeJobResultImpl JobResult
        {
            get { return jobRes; }
        }

        /// <summary>
        /// Internal job execution routine.
        /// </summary>
        /// <param name="cancel">Cancel flag.</param>
        /// <param name="res">Result.</param>
        /// <param name="success">Success flag.</param>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes",
            Justification = "User job can throw any exception")]
        private void Execute0(bool cancel, out object res, out bool success)
        {
            // 1. Inject resources.
            IComputeResourceInjector injector = job as IComputeResourceInjector;

            if (injector != null)
                injector.Inject(grid);
            else
                ResourceProcessor.Inject(job, grid);

            // 2. Execute.
            try
            {
                if (cancel)
                    job.Cancel();

                res = job.Execute();

                success = true;
            }
            catch (Exception e)
            {
                res = e;

                success = false;
            }
        }

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            PortableWriterImpl writer0 = (PortableWriterImpl) writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, job);
        }

        /// <summary>
        /// Create job instance.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="stream">Stream.</param>
        /// <returns></returns>
        internal static ComputeJobHolder CreateJob(GridImpl grid, IPortableStream stream)
        {
            try
            {
                return grid.Marshaller.StartUnmarshal(stream).ReadObject<ComputeJobHolder>();
            }
            catch (Exception e)
            {
                throw new IgniteException("Failed to deserialize the job [errType=" + e.GetType().Name +
                    ", errMsg=" + e.Message + ']');
            }
        }
    }
}
