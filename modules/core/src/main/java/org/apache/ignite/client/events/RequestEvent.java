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

package org.apache.ignite.client.events;

/** */
public abstract class RequestEvent {
    /** */
    private final ConnectionDescription conn;

    /** */
    private final long requestId;

    /** */
    private final short opCode;

    /** */
    private final String opName;

    /**
     * @param conn Connection description.
     * @param requestId Request id.
     * @param opCode Operation code.
     * @param opName Operation name.
     */
    protected RequestEvent(
        ConnectionDescription conn,
        long requestId,
        short opCode,
        String opName
    ) {
        this.conn = conn;
        this.requestId = requestId;
        this.opCode = opCode;
        this.opName = opName;
    }

    /**
     * @return Connection description.
     */
    public ConnectionDescription connectionDescription() {
        return conn;
    }

    /**
     * @return Request id.
     */
    public long requestId() {
        return requestId;
    }

    /**
     * @return Operation code.
     */
    public short operationCode() {
        return opCode;
    }

    /**
     * @return Operation name.
     */
    public String operationName() {
        return opName;
    }
}
