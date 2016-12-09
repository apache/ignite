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

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteInClosure;

/**
 *
 */
public interface SessionWriteRequest {
    /**
     * Sets flag indicating that message send future was created in thread that was processing a message.
     *
     * @param msgThread {@code True} if future was created in thread that is processing message.
     */
    public void messageThread(boolean msgThread);

    /**
     * @return {@code True} if future was created in thread that was processing message.
     */
    public boolean messageThread();

    /**
     * @return {@code True} if skip recovery for this operation.
     */
    public boolean skipRecovery();

    /**
     * Sets ack closure which will be applied when ack received.
     *
     * @param c Ack closure.
     */
    public void ackClosure(IgniteInClosure<IgniteException> c);

    /**
     * The method will be called when ack received.
     */
    public void onAckReceived();

    /**
     * @return Ack closure.
     */
    public IgniteInClosure<IgniteException> ackClosure();

    /**
     * @return Session.
     */
    public GridNioSession session();

    /**
     * @param ses Session.
     */
    public void resetSession(GridNioSession ses);

    /**
     *
     */
    public void onError(Exception e);

    /**
     * @return Message.
     */
    public Object message();

    /**
     *
     */
    public void onMessageWritten();
}
