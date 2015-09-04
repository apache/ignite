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
    using System.Reflection;
    using Apache.Ignite.Core.Compute;
    using Apache.Ignite.Core.Impl.Common;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Portable;
    using Apache.Ignite.Core.Resource;

    /// <summary>
    /// Non-generic version of IComputeJob{T}.
    /// </summary>
    internal interface IComputeJob : IComputeJob<object>
    {
        // No-op.
    }

    /// <summary>
    /// Wraps generic func into a non-generic for internal usage.
    /// </summary>
    internal class ComputeJobWrapper : IComputeJob, IPortableWriteAware
    {
        /** */
        private readonly Func<object, object> _execute;

        /** */
        private readonly Action<object> _cancel;

        /** */
        private readonly object _job;

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeJobWrapper"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ComputeJobWrapper(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl)reader.RawReader();

            _job = PortableUtils.ReadPortableOrSerializable<object>(reader0);

            DelegateTypeDescriptor.GetComputeJob(_job.GetType(), out _execute, out _cancel);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeFuncWrapper" /> class.
        /// </summary>
        public ComputeJobWrapper(object job, Func<object, object> execute, Action<object> cancel)
        {
            _job = job;

            _execute = execute;

            _cancel = cancel;
        }

        /** <inheritDoc /> */
        public object Execute()
        {
            try
            {
                return _execute(_job);
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException;
            }
        }

        /** <inheritDoc /> */
        public void Cancel()
        {
            try
            {
                _cancel(_job);
            }
            catch (TargetInvocationException ex)
            {
                throw ex.InnerException;
            }
        }

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            var writer0 = (PortableWriterImpl)writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, Job);
        }

        /// <summary>
        /// Injects Ignite instance into wrapped object.
        /// </summary>
        [InstanceResource]
        public void InjectIgnite(IIgnite ignite)
        {
            // Propagate injection
            ResourceProcessor.Inject(Job, (IgniteProxy)ignite);
        }

        /// <summary>
        /// Gets the inner job.
        /// </summary>
        public object Job
        {
            get { return _job; }
        }
    }

    /// <summary>
    /// Extension methods for IComputeJob{T}.
    /// </summary>
    internal static class ComputeJobExtensions
    {
        /// <summary>
        /// Convert to non-generic wrapper.
        /// </summary>
        public static IComputeJob ToNonGeneric<T>(this IComputeJob<T> job)
        {
            return new ComputeJobWrapper(job, x => job.Execute(), x => job.Cancel());
        }

        /// <summary>
        /// Unwraps job of one type into job of another type.
        /// </summary>
        public static IComputeJob<TR> Unwrap<T, TR>(this IComputeJob<T> job)
        {
            var wrapper = job as ComputeJobWrapper;

            return wrapper != null ? (IComputeJob<TR>) wrapper.Job : (IComputeJob<TR>) job;
        }
        
        /// <summary>
        /// Unwraps job of one type into job of another type.
        /// </summary>
        public static object Unwrap(this IComputeJob<object> job)
        {
            var wrapper = job as ComputeJobWrapper;

            return wrapper != null ? wrapper.Job : job;
        }
    }
}
