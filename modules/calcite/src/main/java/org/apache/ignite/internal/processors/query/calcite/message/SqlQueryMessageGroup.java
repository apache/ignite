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

import static org.apache.ignite.internal.processors.query.calcite.message.SqlQueryMessageGroup.GROUP_TYPE;

import org.apache.ignite.network.annotations.MessageGroup;

/**
 * Message types for the sql query processing module.
 */
@MessageGroup(groupType = GROUP_TYPE, groupName = "SqlQueryMessages")
public final class SqlQueryMessageGroup {
    public static final short GROUP_TYPE = 4;

    public static final short QUERY_START_REQUEST = 0;

    public static final short QUERY_START_RESPONSE = 1;

    public static final short ERROR_MESSAGE = 2;

    public static final short QUERY_BATCH_MESSAGE = 3;

    public static final short QUERY_BATCH_ACK = 4;

    public static final short INBOX_CLOSE_MESSAGE = 5;

    public static final short OUTBOX_CLOSE_MESSAGE = 6;
}
