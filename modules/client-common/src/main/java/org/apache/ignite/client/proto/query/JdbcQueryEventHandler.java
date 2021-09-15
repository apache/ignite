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

package org.apache.ignite.client.proto.query;

import org.apache.ignite.client.proto.query.event.BatchExecuteRequest;
import org.apache.ignite.client.proto.query.event.BatchExecuteResult;
import org.apache.ignite.client.proto.query.event.QueryCloseRequest;
import org.apache.ignite.client.proto.query.event.QueryCloseResult;
import org.apache.ignite.client.proto.query.event.QueryExecuteRequest;
import org.apache.ignite.client.proto.query.event.QueryExecuteResult;
import org.apache.ignite.client.proto.query.event.QueryFetchRequest;
import org.apache.ignite.client.proto.query.event.QueryFetchResult;

/**
 * Jdbc client request handler.
 */
public interface JdbcQueryEventHandler {
    /**
     * {@link QueryExecuteRequest} command handler.
     *
     * @param req Execute query request.
     * @return Result.
     */
    QueryExecuteResult query(QueryExecuteRequest req);

    /**
     * {@link QueryFetchRequest} command handler.
     *
     * @param req Fetch query request.
     * @return Result.
     */
    QueryFetchResult fetch(QueryFetchRequest req);

    /**
     * {@link BatchExecuteRequest} command handler.
     *
     * @param req Batch query request.
     * @return Result.
     */
    BatchExecuteResult batch(BatchExecuteRequest req);

    /**
     * {@link QueryCloseRequest} command handler.
     *
     * @param req Close query request.
     * @return Result.
     */
    QueryCloseResult close(QueryCloseRequest req);
}
