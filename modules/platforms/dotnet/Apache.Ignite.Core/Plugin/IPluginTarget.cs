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

namespace Apache.Ignite.Core.Plugin
{
    using System;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Unmanaged plugin target.
    /// </summary>
    public interface IPluginTarget
    {
        /// <summary>
        /// Invokes a cache extension.
        /// </summary>
        /// <typeparam name="T">The type of the result.</typeparam>
        /// <param name="opCode">The operation code.</param>
        /// <param name="writeAction">The write action.</param>
        /// <param name="readFunc">The read action.</param>
        /// <param name="arg">The optional argument.</param>
        /// <returns>
        /// Result of the processing.
        /// </returns>
        T InvokeOperation<T>(int opCode, Action<IBinaryRawWriter> writeAction,
            Func<IBinaryRawReader, IPluginTarget, T> readFunc, IPluginTarget arg);

        /// <summary>
        /// Occurs when Java part of the plugin invokes a callback.
        /// </summary>
        event EventHandler<PluginCallbackEventArgs> Callback;
    }
}
