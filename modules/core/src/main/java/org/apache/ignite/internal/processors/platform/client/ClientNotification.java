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

package org.apache.ignite.internal.processors.platform.client;

import org.apache.ignite.internal.binary.BinaryRawWriterEx;
import org.apache.ignite.internal.processors.odbc.ClientListenerResponse;

/**
 * Server to client notification for some resource.
 */
public class ClientNotification extends ClientListenerResponse implements ClientOutgoingMessage {
    /** Resource id. */
    private final long rsrcId;

    /** Operation code. */
    private final short opCode;

    /**
     * Constructor.
     *
     * @param opCode Operation code.
     * @param rsrcId Resource id.
     */
    public ClientNotification(short opCode, long rsrcId) {
        super(ClientStatus.SUCCESS, null);

        this.rsrcId = rsrcId;
        this.opCode = opCode;
    }

    /**
     * Constructor.
     *
     * @param opCode Operation code.
     * @param rsrcId Resource id.
     * @param err Error message.
     */
    public ClientNotification(short opCode, long rsrcId, String err) {
        super(ClientStatus.FAILED, err);

        this.rsrcId = rsrcId;
        this.opCode = opCode;
    }

    /**
     * Constructor.
     *
     * @param opCode Operation code.
     * @param rsrcId Resource id.
     * @param status Status code.
     * @param err Error message.
     */
    public ClientNotification(short opCode, long rsrcId, int status, String err) {
        super(status, err);

        this.rsrcId = rsrcId;
        this.opCode = opCode;
    }

    /**
     * Encodes the notification data.
     *
     * @param ctx Connection context.
     * @param writer Writer.
     */
    @Override public void encode(ClientConnectionContext ctx, BinaryRawWriterEx writer) {
        writer.writeLong(rsrcId);

        short flags = (short) (ClientFlag.NOTIFICATION | (status() == ClientStatus.SUCCESS ? 0 : ClientFlag.ERROR));

        writer.writeShort(flags);

        writer.writeShort(opCode);

        if (status() != ClientStatus.SUCCESS) {
            writer.writeInt(status());

            writer.writeString(error());
        }
    }

    /**
     * Gets the resource id.
     *
     * @return Resource id.
     */
    public long resourceId() {
        return rsrcId;
    }
}
