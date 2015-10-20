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
    using Apache.Ignite.Core.Impl.Portable;
    using Apache.Ignite.Core.Impl.Portable.IO;
    using Apache.Ignite.Core.Portable;
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
        public static void WriteProxyMethod(PortableWriterImpl writer, MethodBase method, object[] arguments)
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
        public static void ReadProxyMethod(IPortableStream stream, PortableMarshaller marsh, 
            out string mthdName, out object[] mthdArgs)
        {
            var reader = marsh.StartUnmarshal(stream);

            var srvKeepPortable = reader.ReadBoolean();

            mthdName = reader.ReadString();

            if (reader.ReadBoolean())
            {
                mthdArgs = new object[reader.ReadInt()];

                if (srvKeepPortable)
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
        public static void WriteInvocationResult(IPortableStream stream, PortableMarshaller marsh, object methodResult,
            Exception invocationError)
        {
            Debug.Assert(stream != null);
            Debug.Assert(marsh != null);

            var writer = marsh.StartMarshal(stream);

            PortableUtils.WriteInvocationResult(writer, invocationError == null, invocationError ?? methodResult);
        }

        /// <summary>
        /// Reads method invocation result.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepPortable">Portable flag.</param>
        /// <returns>
        /// Method invocation result, or exception in case of error.
        /// </returns>
        public static object ReadInvocationResult(IPortableStream stream, PortableMarshaller marsh, bool keepPortable)
        {
            Debug.Assert(stream != null);
            Debug.Assert(marsh != null);

            var mode = keepPortable ? PortableMode.ForcePortable : PortableMode.Deserialize;

            var reader = marsh.StartUnmarshal(stream, mode);

            object err;

            var res = PortableUtils.ReadInvocationResult(reader, out err);

            if (err == null)
                return res;

            var portErr = err as IPortableObject;

            throw portErr != null
                ? new ServiceInvocationException("Proxy method invocation failed with a portable error. " +
                                                 "Examine PortableCause for details.", portErr)
                : new ServiceInvocationException("Proxy method invocation failed with an exception. " +
                                                 "Examine InnerException for details.", (Exception) err);
        }
    }
}