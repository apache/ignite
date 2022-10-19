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
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Linq;
    using System.Reflection;
    using Apache.Ignite.Core.Impl.Common;

    /// <summary>
    /// Invokes service proxy methods.
    /// </summary>
    internal static class ServiceProxyInvoker
    {
        /** Cached method info. */
        private static readonly CopyOnWriteConcurrentDictionary<Tuple<Type, string, int>, Func<object, object[], object>> Methods =
            new CopyOnWriteConcurrentDictionary<Tuple<Type, string, int>, Func<object, object[], object>>();

        /// <summary>
        /// Invokes the service method according to data from a stream,
        /// and writes invocation result to the output stream.
        /// </summary>
        /// <param name="svcCtx">Service context.</param>
        /// <param name="methodName">Name of the method.</param>
        /// <param name="arguments">Arguments.</param>
        /// <returns>Pair of method return value and invocation exception.</returns>
        [SuppressMessage("Microsoft.Design", "CA1031:DoNotCatchGeneralExceptionTypes")]
        public static KeyValuePair<object, Exception> InvokeServiceMethod(ServiceContext svcCtx, string methodName, 
            object[] arguments)
        {
            Debug.Assert(svcCtx != null);
            Debug.Assert(svcCtx.Service != null);
            Debug.Assert(!string.IsNullOrWhiteSpace(methodName));

            var method = GetMethodOrThrow(svcCtx.Service.GetType(), methodName, arguments);

            try
            {
                var res = svcCtx.Interceptor == null
                    ? method.Invoke(svcCtx.Service, arguments)
                    : svcCtx.Interceptor.Invoke(methodName, arguments, svcCtx,
                        () => method.Invoke(svcCtx.Service, arguments));

                return new KeyValuePair<object, Exception>(res, null);
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
        private static Func<object, object[], object> GetMethodOrThrow(Type svcType, string methodName,
            object[] arguments)
        {
            Debug.Assert(svcType != null);
            Debug.Assert(!string.IsNullOrWhiteSpace(methodName));
            
            var argsLength = arguments == null ? 0 : arguments.Length;

            // 0) Check cached methods
            var cacheKey = Tuple.Create(svcType, methodName, argsLength);
            Func<object, object[], object> res;

            if (Methods.TryGetValue(cacheKey, out res))
                return res;

            // 1) Find methods by name
            // Handle default interface implementations by including non-abstract methods from all interfaces.
            var bindingFlags = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;

            var methods = svcType.GetMethods(bindingFlags)
                .Concat(svcType.GetInterfaces().SelectMany(x => x.GetMethods(bindingFlags)).Where(x => !x.IsAbstract))
                .Where(m => CleanupMethodName(m) == methodName && m.GetParameters().Length == argsLength)
                .ToArray();

            if (methods.Length == 1)
            {
                // Update cache only when there is a single method with a given name and arg count.
                return Methods.GetOrAdd(cacheKey, x => DelegateConverter.CompileFuncFromArray(methods[0]));
            }

            if (methods.Length == 0)
                throw new InvalidOperationException(
                    string.Format(CultureInfo.InvariantCulture,
                        "Failed to invoke proxy: there is no method '{0}' in type '{1}' with {2} arguments",
                        methodName, svcType, argsLength));

            // 2) There is more than 1 method with specified name - resolve with argument types.
            methods = methods.Where(m => AreMethodArgsCompatible(arguments, m.GetParameters())).ToArray();

            if (methods.Length == 1)
                return (obj, args) => methods[0].Invoke(obj, args);

            if (methods.Length > 1)
                // Try to search applicable without equality of all arguments.
                methods = methods.Where(m => AreMethodArgsCompatible(arguments, m.GetParameters(), true)).ToArray();

            if (methods.Length == 1)
                return (obj, args) => methods[0].Invoke(obj, args);

            // 3) 0 or more than 1 matching method - throw.
            var argsString = argsLength == 0
                ? "0"
                : "(" +
                  // ReSharper disable once ConditionIsAlwaysTrueOrFalse
                  // ReSharper disable once AssignNullToNotNullAttribute
                  arguments.Select(x => x == null ? "null" : x.GetType().Name).Aggregate((x, y) => x + ", " + y)
                  + ")";

            if (methods.Length == 0)
                throw new InvalidOperationException(
                    string.Format(CultureInfo.InvariantCulture,
                        "Failed to invoke proxy: there is no method '{0}' in type '{1}' with {2} arguments",
                        methodName, svcType, argsString));

            throw new InvalidOperationException(
                string.Format(CultureInfo.InvariantCulture,
                    "Failed to invoke proxy: there are {2} methods '{0}' in type '{1}' with {3} " +
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
        /// <param name="strictTypeCheck">If true check argument type for equality.</param>
        /// <returns>True if a target method can be called with specified set of arguments; otherwise, false.</returns>
        private static bool AreMethodArgsCompatible(object[] methodArgs, ParameterInfo[] targetParameters,
            bool strictTypeCheck = false)
        {
            if (methodArgs == null || methodArgs.Length == 0)
                return targetParameters.Length == 0;

            if (methodArgs.Length != targetParameters.Length)
                return false;

            Func<object, ParameterInfo, bool> checker;
            if (strictTypeCheck)
            {
                checker = (arg, param) => arg == null || param.ParameterType == arg.GetType();
            }
            else
            {
                checker = (arg, param) => arg == null || param.ParameterType.IsInstanceOfType(arg);
            }

            return methodArgs
                .Zip(targetParameters, checker)
                .All(x => x);
        }
    }
}
