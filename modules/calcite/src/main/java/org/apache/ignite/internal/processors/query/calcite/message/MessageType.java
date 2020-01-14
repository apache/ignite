/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.message;

/**
 *
 */
public final class MessageType {
    /** */
    public static final short QUERY_START_REQUEST = 300;

    /** */
    public static final short QUERY_START_RESPONSE = 301;

    /** */
    public static final short QUERY_CANCEL_REQUEST = 302;

    /** */
    public static final short QUERY_BATCH_MESSAGE = 303;

    /** */
    public static final short QUERY_ACKNOWLEDGE_MESSAGE = 304;

    /** */
    public static final short QUERY_INBOX_CANCEL_MESSAGE = 305;

    /** */
    public static final short GENERIC_ROW_MESSAGE = 306;

    private MessageType() {}
}
