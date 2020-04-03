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

namespace Apache.Ignite.Core.Impl
{
    using System;
    using Apache.Ignite.Core.Impl.Binary;
    using Apache.Ignite.Core.Impl.Binary.IO;
    using Apache.Ignite.Core.Interop;

    /// <summary>
    /// Extended platform target interface with methods that operate on internal entities (streams and targets).
    /// </summary>
    internal interface IPlatformTargetInternal : IPlatformTarget, IDisposable
    {
        /// <summary>
        /// Gets the marshaller.
        /// </summary>
        Marshaller Marshaller { get; }

        /// <summary>
        /// Performs InStreamOutLong operation.
        /// </summary>
        /// <param name="type">Operation type code.</param>
        /// <param name="writeAction">Write action.</param>
        /// <returns>Result.</returns>
        long InStreamOutLong(int type, Action<IBinaryStream> writeAction);

        /// <summary>
        /// Performs InStreamOutLong operation with stream reuse.
        /// </summary>
        /// <param name="type">Operation type code.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        /// <param name="readErrorAction">Error action.</param>
        /// <returns>
        /// Result.
        /// </returns>
        T InStreamOutLong<T>(int type, Func<IBinaryStream, bool> writeAction, Func<IBinaryStream, long, T> readAction,
            Func<IBinaryStream, Exception> readErrorAction);

        /// <summary>
        /// Performs InStreamOutStream operation.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="type">Operation type code.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        /// <returns>Result.</returns>
        T InStreamOutStream<T>(int type, Action<IBinaryStream> writeAction, Func<IBinaryStream, T> readAction);

        /// <summary>
        /// Performs InStreamOutObject operation.
        /// </summary>
        /// <param name="type">Operation type code.</param>
        /// <param name="writeAction">Write action.</param>
        /// <returns>Result.</returns>
        IPlatformTargetInternal InStreamOutObject(int type, Action<IBinaryStream> writeAction);

        /// <summary>
        /// Performs InObjectStreamOutObjectStream operation.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="type">Operation type code.</param>
        /// <param name="arg">Target argument.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        /// <returns>Result.</returns>
        T InObjectStreamOutObjectStream<T>(int type, Action<IBinaryStream> writeAction,
            Func<IBinaryStream, IPlatformTargetInternal, T> readAction, IPlatformTargetInternal arg);

        /// <summary>
        /// Performs OutStream operation.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="type">Operation type code.</param>
        /// <param name="readAction">Read action.</param>
        /// <returns>Result.</returns>
        T OutStream<T>(int type, Func<IBinaryStream, T> readAction);

        /// <summary>
        /// Performs the OutObject operation.
        /// </summary>
        /// <param name="type">Operation type code.</param>
        /// <returns>Result.</returns>
        IPlatformTargetInternal OutObjectInternal(int type);
    }
}