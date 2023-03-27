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

namespace Apache.Ignite.Core.Impl.Unmanaged.Jni
{
    using System;
    using System.Diagnostics.CodeAnalysis;
    using System.Globalization;
    using System.Reflection;

    /// <summary>
    /// Cross-AppDomain Jvm parts, valid only on .NET Classic.
    /// </summary>
    internal sealed partial class Jvm
    {
        /// <summary>
        /// Gets the callbacks.
        /// </summary>
        private static Callbacks GetCallbacksFromDefaultDomainImpl()
        {
            // JVM exists once per process, and JVM callbacks exist once per process.
            // We should register callbacks ONLY from the default AppDomain (which can't be unloaded).
            // Non-default appDomains should delegate this logic to the default one.
            var defDomain = AppDomains.GetDefaultAppDomain();

            // In some cases default AppDomain is not able to locate Apache.Ignite.Core assembly.
            // First, use CreateInstanceFrom to set up the AssemblyResolve handler.
            var resHelpType = typeof(AssemblyResolver);

            var handle = InvokeMethod(defDomain, "CreateInstanceFrom", resHelpType.Assembly.Location, resHelpType.FullName);
            var resHelp = (AssemblyResolver)InvokeMethod(handle, "Unwrap");

            resHelp.TrackResolve(resHelpType.Assembly.FullName, resHelpType.Assembly.Location);

            // Now use CreateInstance to get the domain helper of a properly loaded class.
            var type = typeof(CallbackAccessor);
            var helperHandle = InvokeMethod(defDomain, "CreateInstance", type.Assembly.FullName, type.FullName);
            var helper = (CallbackAccessor)InvokeMethod(helperHandle, "Unwrap");

            return helper.GetCallbacks();
        }

        /// <summary>
        /// Invokes a method with reflection.
        /// </summary>
        private static object InvokeMethod(object o, string name, params object[] args)
        {
            return o.GetType().InvokeMember(
                name,
                BindingFlags.Instance | BindingFlags.Public | BindingFlags.InvokeMethod,
                null,
                o,
                args,
                CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Provides access to <see cref="Callbacks"/> instance in the default AppDomain.
        /// </summary>
        private class CallbackAccessor : MarshalByRefObject
        {
            /// <summary>
            /// Gets the callbacks.
            /// </summary>
            [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic",
                Justification = "Only instance methods can be called across AppDomain boundaries.")]
            public Callbacks GetCallbacks()
            {
                return GetOrCreate(null)._callbacks;
            }
        }

        /// <summary>
        /// Resolves Apache.Ignite.Core assembly in the default AppDomain when needed.
        /// </summary>
        private class AssemblyResolver : MarshalByRefObject
        {
            /// <summary>
            /// Tracks the AssemblyResolve event.
            /// </summary>
            [SuppressMessage("Microsoft.Performance", "CA1822:MarkMembersAsStatic",
                Justification = "Only instance methods can be called across AppDomain boundaries.")]
            public void TrackResolve(string name, string path)
            {
                AppDomain.CurrentDomain.AssemblyResolve += (sender, args) =>
                {
                    if (args.Name == name)
                    {
                        return Assembly.LoadFrom(path);
                    }

                    return null;
                };
            }
        }
    }
}
