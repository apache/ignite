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
