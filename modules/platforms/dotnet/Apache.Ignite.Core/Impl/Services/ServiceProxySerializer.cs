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

namespace Apache.Ignite.Core.Impl.Services
{
    using System;
    using System.Diagnostics;
    using System.Reflection;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Services;

    /// <summary>
    /// Static proxy methods.
    /// </summary>
    internal static class ServiceProxySerializer
    {
        /// <summary>
        /// Writes proxy method invocation data to the specified writer.
        /// </summary>
        /// <param name="writer">Writer.</param>
        /// <param name="method">Method.</param>
        /// <param name="arguments">Arguments.</param>
        public static void WriteProxyMethod(BinaryWriter writer, MethodBase method, object[] arguments)
        {
            Debug.Assert(writer != null);
            Debug.Assert(method != null);

            writer.WriteString(method.Name);

            if (arguments != null)
            {
                writer.WriteBoolean(true);
                writer.WriteInt(arguments.Length);

                foreach (var arg in arguments)
                    writer.WriteObject(arg);
            }
            else
                writer.WriteBoolean(false);
        }

        /// <summary>
        /// Reads proxy method invocation data from the specified reader.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="mthdName">Method name.</param>
        /// <param name="mthdArgs">Method arguments.</param>
        public static void ReadProxyMethod(IBinaryStream stream, Marshaller marsh, 
            out string mthdName, out object[] mthdArgs)
        {
            var reader = marsh.StartUnmarshal(stream);

            var srvKeepBinary = reader.ReadBoolean();

            mthdName = reader.ReadString();

            if (reader.ReadBoolean())
            {
                mthdArgs = new object[reader.ReadInt()];

                if (srvKeepBinary)
                    reader = marsh.StartUnmarshal(stream, true);

                for (var i = 0; i < mthdArgs.Length; i++)
                    mthdArgs[i] = reader.ReadObject<object>();
            }
            else
                mthdArgs = null;
        }

        /// <summary>
        /// Writes method invocation result.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="methodResult">Method result.</param>
        /// <param name="invocationError">Method invocation error.</param>
        public static void WriteInvocationResult(IBinaryStream stream, Marshaller marsh, object methodResult,
            Exception invocationError)
        {
            Debug.Assert(stream != null);
            Debug.Assert(marsh != null);

            var writer = marsh.StartMarshal(stream);

            BinaryUtils.WriteInvocationResult(writer, invocationError == null, invocationError ?? methodResult);
        }

        /// <summary>
        /// Reads method invocation result.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepBinary">Binary flag.</param>
        /// <returns>
        /// Method invocation result, or exception in case of error.
        /// </returns>
        public static object ReadInvocationResult(IBinaryStream stream, Marshaller marsh, bool keepBinary)
        {
            Debug.Assert(stream != null);
            Debug.Assert(marsh != null);

            var mode = keepBinary ? BinaryMode.ForceBinary : BinaryMode.Deserialize;

            var reader = marsh.StartUnmarshal(stream, mode);

            object err;

            var res = BinaryUtils.ReadInvocationResult(reader, out err);

            if (err == null)
                return res;

            var binErr = err as IBinaryObject;

            throw binErr != null
                ? new ServiceInvocationException("Proxy method invocation failed with a binary error. " +
                                                 "Examine BinaryCause for details.", binErr)
                : new ServiceInvocationException("Proxy method invocation failed with an exception. " +
                                                 "Examine InnerException for details.", (Exception) err);
        }
    }
}