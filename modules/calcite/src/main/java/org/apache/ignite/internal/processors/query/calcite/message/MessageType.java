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

package org.apache.ignite.internal.processors.query.calcite.message;

/** */
public enum MessageType {
    /** */
    QUERY_START_REQUEST(300),

    /** */
    QUERY_START_RESPONSE(301),

    /** */
    QUERY_ERROR_MESSAGE(302),

    /** */
    QUERY_BATCH_MESSAGE(303),

    /** */
    QUERY_ACKNOWLEDGE_MESSAGE(304),

    /** */
    QUERY_INBOX_CANCEL_MESSAGE(305),

    /** */
    QUERY_CLOSE_MESSAGE(306),

    /** */
    GENERIC_VALUE_MESSAGE(307),

    /** */
    FRAGMENT_MAPPING(350),

    /** */
    COLOCATION_GROUP(351),

    /** */
    FRAGMENT_DESCRIPTION(352),

    /** */
    QUERY_TX_ENTRY(353);

    /** */
    private final int directType;

    /**
     * @param directType Direct type.
     */
    MessageType(int directType) {
        this.directType = directType;
    }

    /**
     * @return Message direct type.
     */
    public short directType() {
        return (short)directType;
    }
}
