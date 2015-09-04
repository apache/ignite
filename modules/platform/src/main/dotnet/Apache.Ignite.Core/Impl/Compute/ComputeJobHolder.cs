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
    using System.Diagnostics.CodeAnalysis;
    using Apache.Ignite.Core.Common;
    using Apache.Ignite.Core.Impl.Cluster;
    using Apache.Ignite.Core.Impl.Compute.Closure;
    using Apache.Ignite.Core.Impl.Memory;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// Holder for user-provided compute job.
    /// </summary>
    internal class ComputeJobHolder : IPortableWriteAware
    {
        /** Actual job. */
        private readonly IComputeJob _job;
        
        /** Owning grid. */
        private readonly Ignite _ignite;

        /** Result (set for local jobs only). */
        private volatile ComputeJobResultImpl _jobRes;

        /// <summary>
        /// Default ctor for marshalling.
        /// </summary>
        /// <param name="reader"></param>
        public ComputeJobHolder(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl) reader.RawReader();

            _ignite = reader0.Marshaller.Ignite;

            _job = PortableUtils.ReadPortableOrSerializable<IComputeJob>(reader0);
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="job">Job.</param>
        public ComputeJobHolder(Ignite grid, IComputeJob job)
        {
            _ignite = grid;
            _job = job;
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

            _jobRes = new ComputeJobResultImpl(
                success ? res : null, 
                success ? null : res as Exception, 
                _job, 
                _ignite.LocalNode.Id, 
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
            ClusterGroupImpl prj = _ignite.ClusterGroup;

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
            _job.Cancel();
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
            ClusterGroupImpl prj = _ignite.ClusterGroup;

            PortableWriterImpl writer = prj.Marshaller.StartMarshal(stream);

            try
            {
                writer.Write(this);

                return true;
            }
            catch (Exception e)
            {
                writer.WriteString("Failed to marshal job [job=" + _job + ", errType=" + e.GetType().Name +
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
            get { return _job; }
        }

        /// <summary>
        /// Job result.
        /// </summary>
        internal ComputeJobResultImpl JobResult
        {
            get { return _jobRes; }
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
            IComputeResourceInjector injector = _job as IComputeResourceInjector;

            if (injector != null)
                injector.Inject(_ignite);
            else
                ResourceProcessor.Inject(_job, _ignite);

            // 2. Execute.
            try
            {
                if (cancel)
                    _job.Cancel();

                res = _job.Execute();

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
            PortableUtils.WritePortableOrSerializable(writer0, _job);
        }

        /// <summary>
        /// Create job instance.
        /// </summary>
        /// <param name="grid">Grid.</param>
        /// <param name="stream">Stream.</param>
        /// <returns></returns>
        internal static ComputeJobHolder CreateJob(Ignite grid, IPortableStream stream)
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
