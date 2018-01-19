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

import org.apache.ignite.IgniteIllegalStateException;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcBulkLoadContext;
import org.apache.ignite.internal.processors.odbc.jdbc.JdbcSendFileBatchRequest;
import org.apache.ignite.internal.processors.query.IgniteSQLException;
import org.apache.ignite.internal.sql.command.SqlBulkLoadCommand;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/** FIXME SHQ */
public class BulkLoadCsvParser extends BulkLoadParser {

    private final LinkedList<byte []> inputBatches;
    private long lastBatchNum;

    public BulkLoadCsvParser(BulkLoadFormat format) {
        super(format);
        inputBatches = new LinkedList<>();
        lastBatchNum = 0;
    }

    @Override public Iterable<List<Object>> processBatch(JdbcBulkLoadContext ctx, JdbcSendFileBatchRequest req) {

        switch (req.cmd()) {
            case CONTINUE:
                if (lastBatchNum + 1 != req.batchNum())
                    throw new IgniteSQLException("Batch #" + (lastBatchNum + 1) + " is missing");

                addBatch(req);

                lastBatchNum = req.batchNum();

                return Collections.emptyList();

            case FINISHED_ERROR:
                clearBatches();
                return Collections.emptyList();

            case FINISHED_EOF:
                return processFile(joinBatches());

            default:
                throw new IgniteIllegalStateException("Unknown state");
        }
    }

    private void addBatch(JdbcSendFileBatchRequest req) {
        inputBatches.addLast(req.data());
    }

    private void clearBatches() {
        inputBatches.clear();
    }

    private byte[] joinBatches() {
        int size = 0;

        for (byte[] batch : inputBatches)
            size += batch.length;

        byte[] fileBytes = new byte[size];

        int pos = 0;
        for (byte[] batch : inputBatches) {
            System.arraycopy(batch, 0, fileBytes, pos, batch.length);
            pos += batch.length;
        }

        return fileBytes;
    }

    // A dumb stub for now for CSV parsing with hardcoded parameters and inefficient processing

    private Iterable<List<Object>> processFile(byte[] input) {

        String inputStr = new String(input, BulkLoadFormat.DEFAULT_INPUT_CHARSET);

        String[] lines = inputStr.split(BulkLoadCsvFormat.LINE_SEP_RE);

        List<List<Object>> result = new ArrayList<>(lines.length);

        for (String line : lines) {
            String[] fields = line.split(BulkLoadCsvFormat.FIELD_SEP_RE);

            List<Object> convertedFields = new ArrayList<>(fields.length);

            for (String field : fields) {
                if (field.startsWith(BulkLoadCsvFormat.QUOTE_CHAR) && field.endsWith(BulkLoadCsvFormat.QUOTE_CHAR))
                    field = field.substring(1, field.length() - 1);

                convertedFields.add(field);
            }

            result.add(convertedFields);
        }

        return result;
    }

}
