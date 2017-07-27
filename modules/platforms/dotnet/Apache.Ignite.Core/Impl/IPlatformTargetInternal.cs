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
    /// Extended platform target interface.
    /// </summary>
    internal interface IPlatformTargetInternal : IPlatformTarget, IDisposable  // TODO: Verify consistent naming.
    {
        /// <summary>
        /// Gets the marshaller.
        /// </summary>
        Marshaller Marshaller { get; }

        T OutStream<T>(int type, Func<IBinaryStream, T> action);

        /// <summary>
        /// Performs InStreamOutLong operation.
        /// </summary>
        /// <param name="type">Operation type code.</param>
        /// <param name="writeAction">Write action.</param>
        /// <returns>Result.</returns>
        long InStreamOutLong(int type, Action<IBinaryStream> writeAction);

        /// <summary>
        /// Performs the OutObject operation.
        /// </summary>
        /// <param name="type">Operation type code.</param>
        /// <returns>Result.</returns>
        IPlatformTargetInternal OutOpObject(int type);

        /// <summary>
        /// Performs InStreamOutObject operation.
        /// </summary>
        /// <param name="type">Operation type code.</param>
        /// <param name="writeAction">Write action.</param>
        /// <returns>Result.</returns>
        IPlatformTargetInternal InStreamOutObject(int type, Action<IBinaryStream> writeAction);

        /// <summary>
        /// Performs InStreamOutStream operation.
        /// </summary>
        /// <typeparam name="T">Result type.</typeparam>
        /// <param name="type">Operation type code.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        /// <returns>Result.</returns>
        T InStreamOutStream<T>(int type, Action<BinaryWriter> writeAction, Func<IBinaryStream, T> readAction);

        /// <summary>
        /// Performs InStreamOutLong operation with stream reuse.
        /// </summary>
        /// <param name="type">Operation type code.</param>
        /// <param name="writeAction">Write action.</param>
        /// <param name="readAction">Read action.</param>
        /// <param name="inErrorAction">Error action.</param>
        /// <returns>
        /// Result.
        /// </returns>
        T InStreamOutLong<T>(int type, Action<BinaryWriter> writeAction, Func<IBinaryStream, long, T> readAction,
            Func<IBinaryStream, Exception> inErrorAction);

        bool DoOutInOpX(int type, Action<BinaryWriter> outAction, Func<IBinaryStream, Exception> inErrorAction);

        T DoOutInOp<T>(int type, Action<BinaryWriter> outAction,
            Func<IBinaryStream, IPlatformTargetInternal, T> inAction, IPlatformTargetInternal arg);
    }
}