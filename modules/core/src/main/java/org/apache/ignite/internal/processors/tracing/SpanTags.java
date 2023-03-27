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

package org.apache.ignite.internal.processors.tracing;

/**
 * List of tags that can be used to decorate spans.
 */
public class SpanTags {
    /** Tag parts default delimiter. */
    private static final String TAG_PARTS_DELIMITER = ".";

    /**
     * List of basic tags. Can be combined together to get more composite tags.
     * Don't forget to add new tags here and use constant reference instead of raw string creation.
     * Frequently used composite tags can be also declared here.
     */

    /** */
    public static final String NODE = "node";

    /** */
    public static final String ID = "id";

    /** */
    public static final String ORDER = "order";

    /** */
    public static final String EVENT = "event";

    /** */
    public static final String NAME = "name";

    /** */
    public static final String TYPE = "type";

    /** */
    public static final String INITIAL = "initial";

    /** */
    public static final String RESULT = "result";

    /** */
    public static final String ERROR = "error";

    /** */
    public static final String EXCHANGE = "exchange";

    /** */
    public static final String CONSISTENT_ID = "consistent.id";

    /** */
    public static final String TOPOLOGY_VERSION = "topology.version";

    /** */
    public static final String MAJOR = "major";

    /** */
    public static final String MINOR = "minor";

    /** */
    public static final String EVENT_NODE = tag(EVENT, NODE);

    /** */
    public static final String NODE_ID = tag(NODE, ID);

    /** */
    public static final String MESSAGE = "message";

    /** Number of bytes that were written to the socket. */
    public static final String SOCKET_WRITE_BYTES = "socket.write.bytes";

    /** */
    public static final String MESSAGE_CLASS = "message.class";

    /** Text of SQL query. */
    public static final String SQL_QRY_TEXT = "sql.query.text";

    /** SQL schema. */
    public static final String SQL_SCHEMA = "sql.schema";

    /** Number of rows that result page contains. */
    public static final String SQL_PAGE_ROWS = "sql.page.rows";

    /** Number of rows that index range request contains. */
    public static final String SQL_IDX_RANGE_ROWS = "sql.index.range.rows";

    /** Name of SQL table. */
    public static final String SQL_TABLE = "sql.table";

    /** Name of SQL index. */
    public static final String SQL_IDX = "sql.index";

    /** Number of cache entries to be updated as a result of DML query. */
    public static final String SQL_CACHE_UPDATES = "sql.cache.updates";

    /** Whether parsing of the SQL query was skipped due to the cached result. */
    public static final String SQL_PARSER_CACHE_HIT = "sql.parser.cache.hit";

    /** Query identifier. */
    public static final String SQL_QRY_ID = "sql.query.id";

    /** */
    private SpanTags() {}

    /**
     * @param tagParts String parts of composite tag.
     * @return Composite tag with given parts joined using delimiter.
     */
    public static String tag(String... tagParts) {
        return String.join(TAG_PARTS_DELIMITER, tagParts);
    }
}
