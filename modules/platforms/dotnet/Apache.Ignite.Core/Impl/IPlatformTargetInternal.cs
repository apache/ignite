/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
        T InStreamOutLong<T>(int type, Action<IBinaryStream> writeAction, Func<IBinaryStream, long, T> readAction,
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