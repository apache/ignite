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
    using System.Collections.Generic;
    using System.Diagnostics;
    using System.Linq;
    using System.Reflection;

    /// <summary>
    /// Invokes service proxy methods.
    /// </summary>
    internal static class ServiceProxyInvoker 
    {
        /// <summary>
        /// Invokes the service method according to data from a stream,
        /// and writes invocation result to the output stream.
        /// </summary>
        /// <param name="svc">Service instance.</param>
        /// <param name="methodName">Name of the method.</param>
        /// <param name="arguments">Arguments.</param>
        /// <returns>Pair of method return value and invocation exception.</returns>
        public static KeyValuePair<object, Exception> InvokeServiceMethod(object svc, string methodName, 
            object[] arguments)
        {
            Debug.Assert(svc != null);
            Debug.Assert(!string.IsNullOrWhiteSpace(methodName));

            var method = GetMethodOrThrow(svc.GetType(), methodName, arguments);

            try
            {
                return new KeyValuePair<object, Exception>(method.Invoke(svc, arguments), null);
            }
            catch (TargetInvocationException invokeErr)
            {
                return new KeyValuePair<object, Exception>(null, invokeErr.InnerException);
            }
            catch (Exception err)
            {
                return new KeyValuePair<object, Exception>(null, err);
            }
        }

        /// <summary>
        /// Finds suitable method in the specified type, or throws an exception.
        /// </summary>
        private static MethodBase GetMethodOrThrow(Type svcType, string methodName, object[] arguments)
        {
            Debug.Assert(svcType != null);
            Debug.Assert(!string.IsNullOrWhiteSpace(methodName));

            // 1) Find methods by name
            var methods = svcType.GetMethods(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance)
                .Where(m => CleanupMethodName(m) == methodName).ToArray();

            if (methods.Length == 1)
                return methods[0];

            if (methods.Length == 0)
                throw new InvalidOperationException(
                    string.Format("Failed to invoke proxy: there is no method '{0}' in type '{1}'", 
                    methodName, svcType));

            // 2) There is more than 1 method with specified name - resolve with argument types.
            methods = methods.Where(m => AreMethodArgsCompatible(arguments, m.GetParameters())).ToArray();

            if (methods.Length == 1)
                return methods[0];

            // 3) 0 or more than 1 matching method - throw.
            var argsString = arguments == null || arguments.Length == 0
                ? "0"
                : "(" +
                  arguments.Select(x => x == null ? "null" : x.GetType().Name).Aggregate((x, y) => x + ", " + y)
                  + ")";

            if (methods.Length == 0)
                throw new InvalidOperationException(
                    string.Format("Failed to invoke proxy: there is no method '{0}' in type '{1}' with {2} arguments",
                    methodName, svcType, argsString));

            throw new InvalidOperationException(
                string.Format("Failed to invoke proxy: there are {2} methods '{0}' in type '{1}' with {3} " +
                              "arguments, can't resolve ambiguity.", methodName, svcType, methods.Length, argsString));
        }
        
        /// <summary>
        /// Cleans up a method name by removing interface part, 
        /// which occurs when explicit interface implementation is used.
        /// </summary>
        private static string CleanupMethodName(MethodBase method)
        {
            var name = method.Name;

            var dotIdx = name.LastIndexOf(Type.Delimiter);

            return dotIdx < 0 ? name : name.Substring(dotIdx + 1);
        }

        /// <summary>
        /// Determines whether specified method arguments are comatible with given method parameter definitions.
        /// </summary>
        /// <param name="methodArgs">Method argument types.</param>
        /// <param name="targetParameters">Target method parameter definitions.</param>
        /// <returns>True if a target method can be called with specified set of arguments; otherwise, false.</returns>
        private static bool AreMethodArgsCompatible(object[] methodArgs, ParameterInfo[] targetParameters)
        {
            if (methodArgs == null || methodArgs.Length == 0)
                return targetParameters.Length == 0;

            if (methodArgs.Length != targetParameters.Length)
                return false;

            return methodArgs
                .Zip(targetParameters, (arg, param) => arg == null || param.ParameterType.IsInstanceOfType(arg))
                .All(x => x);
        }
    }
}