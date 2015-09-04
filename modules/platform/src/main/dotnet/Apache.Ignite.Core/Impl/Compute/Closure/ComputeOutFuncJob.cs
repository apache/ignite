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
    internal class ComputeOutFuncJob : IComputeJob, IComputeResourceInjector, IPortableWriteAware
    {
        /** Closure. */
        private readonly IComputeOutFunc _clo;

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="clo">Closure.</param>
        public ComputeOutFuncJob(IComputeOutFunc clo)
        {
            _clo = clo;
        }

        /** <inheritDoc /> */
        public object Execute()
        {
            return _clo.Invoke();
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
            var writer0 = (PortableWriterImpl) writer.RawWriter();

            writer0.DetachNext();
            PortableUtils.WritePortableOrSerializable(writer0, _clo);
        }

        public ComputeOutFuncJob(IPortableReader reader)
        {
            var reader0 = (PortableReaderImpl) reader.RawReader();

            _clo = PortableUtils.ReadPortableOrSerializable<IComputeOutFunc>(reader0);
        }
    }
}
