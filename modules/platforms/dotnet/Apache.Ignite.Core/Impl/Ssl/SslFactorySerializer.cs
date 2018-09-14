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

namespace Apache.Ignite.Core.Impl.Ssl
{
    using System;
    using System.Diagnostics;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Ssl;

    /// <summary>
    /// Serializer/deserializer for <see cref="ISslContextFactory"/>.
    /// </summary>
    internal static class SslFactorySerializer
    {
        /// <summary>
        /// Writes the SSL context factory.
        /// </summary>
        internal static void Write(IBinaryRawWriter writer, ISslContextFactory factory)
        {
            Debug.Assert(writer != null);

            var contextFactory = factory as SslContextFactory;
            
            if (contextFactory != null)
            {
                writer.WriteBoolean(true);

                contextFactory.Write(writer);
            }
            else if (factory != null)
            {
                throw new NotSupportedException(
                    string.Format("Unsupported {0}: {1}. Only predefined implementations are supported: {2}",
                        typeof(ISslContextFactory).Name, factory.GetType(), typeof(SslContextFactory).Name));
            }
            else
            {
                writer.WriteBoolean(false);
            }
        }

        /// <summary>
        /// Reads the SSL context factory.
        /// </summary>
        internal static ISslContextFactory Read(IBinaryRawReader reader)
        {
            return reader.ReadBoolean() ? new SslContextFactory(reader) : null;
        }
    }
}
