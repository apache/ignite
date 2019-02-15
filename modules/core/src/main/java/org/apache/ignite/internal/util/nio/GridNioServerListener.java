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

package org.apache.ignite.internal.util.nio;

import java.util.EventListener;
import org.apache.ignite.failure.FailureType;
import org.jetbrains.annotations.Nullable;

/**
 * Listener passed in to the {@link GridNioServer} that will be notified on client events.
 */
public interface GridNioServerListener<T> extends EventListener {
    /**
     * This method is called whenever a new client is connected and session is created.
     *
     * @param ses Newly created session for remote client.
     */
    public void onConnected(GridNioSession ses);

    /**
     * This method is called whenever client is disconnected due to correct connection close
     * or due to {@code IOException} during network operations.
     *
     * @param ses Closed session.
     * @param e Exception occurred, if any.
     */
    public void onDisconnected(GridNioSession ses, @Nullable Exception e);

    /**
     * Handle message sent.
     *
     * @param ses Session.
     * @param msg Message.
     */
    void onMessageSent(GridNioSession ses, T msg);

    /**
     * This method is called whenever a {@link GridNioParser} returns non-null value.
     *
     * @param ses Session on which message was received.
     * @param msg Parsed message.
     */
    public void onMessage(GridNioSession ses, T msg);

    /**
     * Called when session has non-empty write queue and server did not send any data
     * within timeout interval.
     *
     * @param ses Session that has timed out writes.
     */
    public void onSessionWriteTimeout(GridNioSession ses);

    /**
     * Called when session did not receive any activity within timeout interval.
     *
     * @param ses Session that is idle.
     */
    public void onSessionIdleTimeout(GridNioSession ses);

    /**
     * Called when critical failure occurs in server implementation.
     */
    public void onFailure(FailureType failureType, Throwable failure);
}
