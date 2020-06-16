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

package org.apache.ignite.internal.processors.query.calcite.metadata;

import java.util.UUID;

/**
 *
 */
public class RemoteException extends RuntimeException {
    /** */
    private final UUID nodeId;

    /** */
    private final UUID queryId;

    /** */
    private final  long fragmentId;

    /** */
    private final long exchangeId;

    /**
     * @param cause Cause.
     * @param nodeId Node ID.
     * @param queryId Query ID.
     * @param fragmentId Fragment ID.
     * @param exchangeId Exchange ID.
     */
    public RemoteException(UUID nodeId, UUID queryId, long fragmentId, long exchangeId, Throwable cause) {
        super("Remote query execution", cause);
        this.nodeId = nodeId;
        this.queryId = queryId;
        this.fragmentId = fragmentId;
        this.exchangeId = exchangeId;
    }

    /**
     * @return Node ID.
     */
    public UUID nodeId() {
        return nodeId;
    }

    /**
     * @return Query ID.
     */
    public UUID queryId() {
        return queryId;
    }

    /**
     * @return Fragment ID.
     */
    public long fragmentId() {
        return fragmentId;
    }

    /**
     * @return Exchange ID.
     */
    public long exchangeId() {
        return exchangeId;
    }
}
