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

namespace Apache.Ignite.Core.Impl.Compute.Closure
{
    using System;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Resource;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// System job which wraps over <c>Func</c>.
    /// </summary>
    internal class ComputeFuncJob : IComputeJob, IComputeResourceInjector, IPortableWriteAware
    {
        /** Closure. */
        private readonly IComputeFunc _clo;

        /** Argument. */
        private readonly object _arg;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="clo">Closure.</param>
        /// <param name="arg">Argument.</param>
        public ComputeFuncJob(IComputeFunc clo, object arg)
        {
            _clo = clo;
            _arg = arg;
        }

        /** <inheritDoc /> */
        public object Execute()
        {
            return _clo.Invoke(_arg);
        }

        /** <inheritDoc /> */
        public void Cancel()
        {
            throw new NotSupportedException("Func job cannot be cancelled.");
        }

        /** <inheritDoc /> */
        public void Inject(Ignite grid)
        {
            ResourceProcessor.Inject(_clo, grid);
        }

        /** <inheritDoc /> */
        public void WritePortable(IPortableWriter writer)
        {
            PortableWriterImpl writer0 = (PortableWriterImpl) writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, _clo);

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, _arg);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ComputeFuncJob"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public ComputeFuncJob(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl) reader.RawReader();
            
            _clo = PortableUtils.ReadPortableOrSerializable<IComputeFunc>(reader0);
            _arg = PortableUtils.ReadPortableOrSerializable<object>(reader0);
        }
    }
}
