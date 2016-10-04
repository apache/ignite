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

namespace Apache.Ignite.Core.Impl.Plugin
{
    using System;
    using Apache.Ignite.Core.Binary;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Impl.Unmanaged;
    using Apache.Ignite.Core.Plugin;

    /// <summary>
    /// Plugin target.
    /// </summary>
    internal class PluginTarget : PlatformDisposableTarget, IPluginTarget
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PluginTarget" /> class.
        /// </summary>
        /// <param name="target">The target.</param>
        /// <param name="marsh">Marshaller.</param>
        public PluginTarget(IUnmanagedTarget target, Marshaller marsh) : base(target, marsh)
        {
            // No-op.
        }

        /** <inheritdoc /> */
        public unsafe T InvokeOperation<T>(int opCode, Action<IBinaryRawWriter> writeAction, 
            Func<IBinaryRawReader, IPluginTarget, T> readFunc, IPluginTarget arg)
        {
            ThrowIfDisposed();

            void* argPtr = null;

            var arg0 = arg as PluginTarget;

            if (arg0 != null)
                argPtr = arg0.Target.Target;
            else if (arg != null)
                throw new ArgumentException("Unsupported IPluginTarget implementation: " + arg.GetType(), "arg");

            var inAction = readFunc != null
                ? (stream, res) => readFunc(Marshaller.StartUnmarshal(stream),
                    res == null ? null : new PluginTarget(res, Marshaller))
                : (Func<IBinaryStream, IUnmanagedTarget, T>) null;

            return DoOutInOp(opCode, writeAction, inAction, argPtr);
        }

        /** <inheritdoc /> */
        public event EventHandler<PluginCallbackEventArgs> Callback;

        /// <summary>
        /// Called when callback occurs from the Java side.
        /// </summary>
        /// <param name="reader">The reader.</param>
        /// <param name="writer">The writer.</param>
        public void OnCallback(IBinaryRawReader reader, IBinaryRawWriter writer)
        {
            var handler = Callback;

            if (handler != null)
                handler(this, new PluginCallbackEventArgs(reader, writer));
        }
    }
}
