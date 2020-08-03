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
    using System.Collections;
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Reflection;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Common;
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
        /// <param name="methodName">Name of the method.</param>
        /// <param name="method">Method (optional, can be null).</param>
        /// <param name="arguments">Arguments.</param>
        /// <param name="platformType">The platform.</param>
        public static void WriteProxyMethod(BinaryWriter writer, string methodName, MethodBase method,
            object[] arguments, PlatformType platformType)
        {
            Debug.Assert(writer != null);

            writer.WriteString(methodName);

            if (arguments != null)
            {
                writer.WriteBoolean(true);
                writer.WriteInt(arguments.Length);

                if (platformType == PlatformType.DotNet)
                {
                    // Write as is for .NET.
                    foreach (var arg in arguments)
                    {
                        writer.WriteObjectDetached(arg);
                    }
                }
                else
                {
                    // Other platforms do not support Serializable, need to convert arrays and collections
                    var mParams = method != null ? method.GetParameters() : null;
                    Debug.Assert(mParams == null || mParams.Length == arguments.Length);

                    for (var i = 0; i < arguments.Length; i++)
                    {
                        WriteArgForPlatforms(writer, mParams != null ? mParams[i].ParameterType : null, arguments[i]);
                    }
                }
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

            marsh.FinishMarshal(writer);
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

        /// <summary>
        /// Reads service deployment result.
        /// </summary>
        /// <param name="stream">Stream.</param>
        /// <param name="marsh">Marshaller.</param>
        /// <param name="keepBinary">Binary flag.</param>
        /// <returns>
        /// Method invocation result, or exception in case of error.
        /// </returns>
        public static void ReadDeploymentResult(IBinaryStream stream, Marshaller marsh, bool keepBinary)
        {
            Debug.Assert(stream != null);
            Debug.Assert(marsh != null);

            var mode = keepBinary ? BinaryMode.ForceBinary : BinaryMode.Deserialize;

            var reader = marsh.StartUnmarshal(stream, mode);

            object err;

            BinaryUtils.ReadInvocationResult(reader, out err);

            if (err == null)
            {
                return;
            }

            // read failed configurations
            ICollection<ServiceConfiguration> failedCfgs;

            try
            {
                // switch to BinaryMode.Deserialize mode to avoid IService casting exception
                reader = marsh.StartUnmarshal(stream);
                failedCfgs = reader.ReadNullableCollectionRaw(f => new ServiceConfiguration(f));
            }
            catch (Exception e)
            {
                throw new ServiceDeploymentException("Service deployment failed with an exception. " +
                                                     "Examine InnerException for details.", e);
            }

            var binErr = err as IBinaryObject;

            throw binErr != null
                ? new ServiceDeploymentException("Service deployment failed with a binary error. " +
                                                 "Examine BinaryCause for details.", binErr, failedCfgs)
                : new ServiceDeploymentException("Service deployment failed with an exception. " +
                                                 "Examine InnerException for details.", (Exception) err, failedCfgs);
        }

        /// <summary>
        /// Writes the argument in platform-compatible format.
        /// </summary>
        private static void WriteArgForPlatforms(BinaryWriter writer, Type paramType, object arg)
        {
            var hnd = GetPlatformArgWriter(paramType, arg);

            if (hnd != null)
            {
                hnd(writer, arg);
            }
            else
            {
                writer.WriteObjectDetached(arg);
            }
        }

        /// <summary>
        /// Gets arg writer for platform-compatible service calls.
        /// </summary>
        private static Action<BinaryWriter, object> GetPlatformArgWriter(Type paramType, object arg)
        {
            if (arg == null)
            {
                return null;
            }

            var type = paramType ?? arg.GetType();

            // Unwrap nullable
            type = Nullable.GetUnderlyingType(type) ?? type;

            if (type.IsPrimitive)
                return null;

            if (type.IsArray)
            {
                Type elemType = type.GetElementType();

                if (elemType == typeof(Guid?))
                    return (writer, o) => writer.WriteGuidArray((Guid?[]) o);
                else if (elemType == typeof(DateTime?))
                    return (writer, o) => writer.WriteTimestampArray((DateTime?[]) o);
            }

            var handler = BinarySystemHandlers.GetWriteHandler(type);

            if (handler != null)
                return null;

            if (type.IsArray)
                return (writer, o) => writer.WriteArrayInternal((Array) o);

            if (arg is ICollection)
                return (writer, o) => writer.WriteCollection((ICollection) o);

            if (arg is DateTime)
                return (writer, o) => writer.WriteTimestamp((DateTime) o);

            return null;
        }
    }
}