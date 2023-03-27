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

namespace Apache.Ignite.Core.Interop
{
    using System;
    using System.Threading;
    using System.Threading.Tasks;
    using Apache.Ignite.Core.Binary;

    /// <summary>
    /// Interface to interoperate with
    /// org.apache.ignite.internal.processors.platform.PlatformTarget on Java side.
    /// </summary>
    public interface IPlatformTarget
    {
        /// <summary>
        /// Performs InLongOutLong operation.
        /// </summary>
        /// <param name="type">Operation type code.</param>
        /// <param name="val">Value.</param>
        /// <returns>Result.</returns>
        long InLongOutLong(int type, long val);

        /// <summary>
        /// Performs InStreamOutLong operation.
        /// </summary>
        /// <param name="type">Operation type code.</param>
        /// <param name="writeAction">Write action.</param>
        /// <returns>Result.</returns>
        long InStreamOutLong(int type, Action<IBinaryRawWriter> writeAction);

        /// <summary>
        /// Performs InStreamOutStream operation.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="type">Operation type code.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        /// <returns>Result.</returns>
        T InStreamOutStream<T>(int type, Action<IBinaryRawWriter> writeAction, Func<IBinaryRawReader, T> readAction);

        /// <summary>
        /// Performs InStreamOutObject operation.
        /// </summary>
        /// <param name="type">Operation type code.</param>
        /// <param name="writeAction">Write action.</param>
        /// <returns>Result.</returns>
        IPlatformTarget InStreamOutObject(int type, Action<IBinaryRawWriter> writeAction);

        /// <summary>
        /// Performs InObjectStreamOutObjectStream operation.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="type">Operation type code.</param>
        /// <param name="arg">Target argument.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        /// <returns>Result.</returns>
        T InObjectStreamOutObjectStream<T>(int type, IPlatformTarget arg, Action<IBinaryRawWriter> writeAction,
            Func<IBinaryRawReader, IPlatformTarget, T> readAction);

        /// <summary>
        /// Performs OutStream operation.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="type">Operation type code.</param>
        /// <param name="readAction">Read action.</param>
        /// <returns>Result.</returns>
        T OutStream<T>(int type, Func<IBinaryRawReader, T> readAction);

        /// <summary>
        /// Performs the OutObject operation.
        /// </summary>
        /// <param name="type">Operation type code.</param>
        /// <returns>Result.</returns>
        IPlatformTarget OutObject(int type);

        /// <summary>
        /// Performs asynchronous operation.
        /// </summary>
        /// <typeparam name="T">Result type</typeparam>
        /// <param name="type">Operation type code.</param>
        /// <param name="writeAction">Write action (can be null).</param>
        /// <param name="readAction">Read function (can be null).</param>
        /// <returns>Task.</returns>
        Task<T> DoOutOpAsync<T>(int type, Action<IBinaryRawWriter> writeAction,
            Func<IBinaryRawReader, T> readAction);

        /// <summary>
        /// Performs asynchronous operation.
        /// </summary>
        /// <typeparam name="T">Result type</typeparam>
        /// <param name="type">Operation type code.</param>
        /// <param name="writeAction">Write action (can be null).</param>
        /// <param name="readAction">Read function (can be null).</param>
        /// <param name="cancellationToken">The cancellation token.</param>
        /// <returns>
        /// Task.
        /// </returns>
        Task<T> DoOutOpAsync<T>(int type, Action<IBinaryRawWriter> writeAction,
            Func<IBinaryRawReader, T> readAction, CancellationToken cancellationToken);
    }
}
