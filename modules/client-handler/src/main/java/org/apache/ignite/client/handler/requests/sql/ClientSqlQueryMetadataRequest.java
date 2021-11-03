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

package org.apache.ignite.client.handler.requests.sql;

import java.util.concurrent.CompletableFuture;
import org.apache.ignite.client.proto.query.JdbcQueryEventHandler;
import org.apache.ignite.client.proto.query.event.JdbcMetaColumnsResult;
import org.apache.ignite.client.proto.query.event.JdbcQueryMetadataRequest;
import org.apache.ignite.internal.client.proto.ClientMessagePacker;
import org.apache.ignite.internal.client.proto.ClientMessageUnpacker;

/**
 * Client sql query metadata request handler.
 */
public class ClientSqlQueryMetadataRequest {
    /**
     * Processes remote {@code JdbcQueryMetadataRequest}.
     *
     * @param in      Client message unpacker.
     * @param out     Client message packer.
     * @param handler Query event handler.
     * @return null value indicates synchronous operation.
     */
    public static CompletableFuture<Void> process(
            ClientMessageUnpacker in,
            ClientMessagePacker out,
            JdbcQueryEventHandler handler
    ) {
        var req = new JdbcQueryMetadataRequest();

        req.readBinary(in);

        JdbcMetaColumnsResult res = handler.queryMetadata(req);

        res.writeBinary(out);

        return null;
    }
}
