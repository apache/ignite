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

namespace Apache.Ignite.Core.Impl.Interop
{
    using System.Collections.Generic;
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Portable;

    /// <summary>
    /// .Net configuration as defined in Java configuration file.
    /// </summary>
    internal class InteropDotNetConfiguration : IPortableWriteAware
    {
        /// <summary>
        /// Portable configuration.
        /// </summary>
        public InteropDotNetPortableConfiguration PortableCfg { get; set; }

        /// <summary>
        /// Assemblies to load.
        /// </summary>
        public IList<string> Assemblies { get; set; }

        /** {@inheritDoc} */
        public void WritePortable(IPortableWriter writer)
        {
            IPortableRawWriter rawWriter = writer.RawWriter();

            rawWriter.WriteObject(PortableCfg);

            rawWriter.WriteGenericCollection(Assemblies);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="InteropDotNetConfiguration"/> class.
        /// </summary>
        /// <param name="reader">The reader.</param>
        public InteropDotNetConfiguration(IPortableReader reader)
        {
            IPortableRawReader rawReader = reader.RawReader();

            PortableCfg = rawReader.ReadObject<InteropDotNetPortableConfiguration>();

            Assemblies = (List<string>) rawReader.ReadGenericCollection<string>();
        }
    }
}
