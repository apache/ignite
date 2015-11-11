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

namespace Apache.Ignite.Core.Impl
{
    using System;
    using System.Runtime.Serialization.Formatters.Binary;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;

    /// <summary>
    /// Holder of exception which must be serialized to Java and then backwards to the native platform.
    /// </summary>
    internal class InteropExceptionHolder : IBinarizable
    {
        /** Initial exception. */
        private Exception _err;

        /// <summary>
        /// Constructor.
        /// </summary>
        public InteropExceptionHolder()
        {
            // No-op.
        }

        /// <summary>
        /// Constructor.
        /// </summary>
        /// <param name="err">Error.</param>
        public InteropExceptionHolder(Exception err)
        {
            _err = err;
        }

        /// <summary>
        /// Underlying exception.
        /// </summary>
        public Exception Error
        {
            get { return _err; }
        }

        /** <inheritDoc /> */
        public void WriteBinary(IBinaryWriter writer)
        {
            var writer0 = (BinaryWriter) writer.GetRawWriter();

            if (writer0.IsBinarizable(_err))
            {
                writer0.WriteBoolean(true);
                writer0.WriteObject(_err);
            }
            else
            {
                writer0.WriteBoolean(false);

                BinaryFormatter bf = new BinaryFormatter();

                bf.Serialize(new BinaryStreamAdapter(writer0.Stream), _err);
            }
        }

        /** <inheritDoc /> */
        public void ReadBinary(IBinaryReader reader)
        {
            throw new NotImplementedException();
        }
    }
}
