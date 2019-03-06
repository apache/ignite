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

package org.apache.ignite.internal.processors.hadoop.impl.igfs;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.igfs.common.IgfsMessage;
import org.jetbrains.annotations.Nullable;

/**
 * IO abstraction layer for IGFS client. Two kind of messages are expected to be sent: requests with response
 * and request without response.
 */
public interface HadoopIgfsIo {
    /**
     * Sends given IGFS client message and asynchronously awaits for response.
     *
     * @param msg Message to send.
     * @return Future that will be completed.
     * @throws IgniteCheckedException If a message cannot be sent (connection is broken or client was closed).
     */
    public IgniteInternalFuture<IgfsMessage> send(IgfsMessage msg) throws IgniteCheckedException;

    /**
     * Sends given IGFS client message and asynchronously awaits for response. When IO detects response
     * beginning for given message it stops reading data and passes input stream to closure which can read
     * response in a specific way.
     *
     * @param msg Message to send.
     * @param outBuf Output buffer. If {@code null}, the output buffer is not used.
     * @param outOff Output buffer offset.
     * @param outLen Output buffer length.
     * @return Future that will be completed when response is returned from closure.
     * @throws IgniteCheckedException If a message cannot be sent (connection is broken or client was closed).
     */
    public <T> IgniteInternalFuture<T> send(IgfsMessage msg, @Nullable byte[] outBuf, int outOff, int outLen)
        throws IgniteCheckedException;

    /**
     * Sends given message and does not wait for response.
     *
     * @param msg Message to send.
     * @throws IgniteCheckedException If send failed.
     */
    public void sendPlain(IgfsMessage msg) throws IgniteCheckedException;

    /**
     * Adds event listener that will be invoked when connection with server is lost or remote error has occurred.
     * If connection is closed already, callback will be invoked synchronously inside this method.
     *
     * @param lsnr Event listener.
     */
    public void addEventListener(HadoopIgfsIpcIoListener lsnr);

    /**
     * Removes event listener that will be invoked when connection with server is lost or remote error has occurred.
     *
     * @param lsnr Event listener.
     */
    public void removeEventListener(HadoopIgfsIpcIoListener lsnr);
}