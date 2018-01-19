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

package org.apache.ignite.internal.processors.bulkload;

import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadContext;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcSendFileBatchRequest;

import java.util.List;

/** FIXME SHQ */
// Currently processing is VERY BASIC
public abstract class BulkLoadParser {

    private final BulkLoadFormat format;

    protected BulkLoadParser(BulkLoadFormat format) {
        this.format = format;
    }

    public abstract Iterable<List<Object>> processBatch(JdbcBulkLoadContext ctx, JdbcSendFileBatchRequest req);

    public static BulkLoadParser createParser(BulkLoadFormat format) {
        if (format instanceof BulkLoadCsvFormat)
            return new BulkLoadCsvParser(format);

        throw new IllegalArgumentException("Internal error: format is not defined");
    }
}
