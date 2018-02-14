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

package org.apache.ignite.yardstick.upload;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.PreparedStatement;
import java.util.Map;

public class CopyBenchmark extends AbstractUploadBenchmark {

    private String warmupCsv;
    private String realCsv;

    @Override protected void init() {
        super.init();
        warmupCsv = generateWarmupCsv();
        realCsv = generateRealCsv();
    }

    private String generateRealCsv() {
        String prefix = "data-" + INSERT_SIZE;
        return generate(prefix, INSERT_SIZE);
    }

    private String generateWarmupCsv() {
        return generate("warmup", WARMUP_ROWS_CNT);
    }

    private String generate(String filePrefix, long records){
        try {
            File f = File.createTempFile(filePrefix, ".csv");
            // TODO: delete on exit?

            try (PrintStream out = new PrintStream(f)) {
                for (long id = 1; id <= records; id++) {
                    String csvLine = queries.randomCsvLine(id);
                    out.println(csvLine);
                }
            }

            return f.getAbsolutePath();
        }
        catch (IOException ex){
            throw new RuntimeException("Couldn't generate CSV file, terminating", ex);
        }
    }

    @Override protected void warmup() throws Exception {
        try (PreparedStatement fromCsv = conn.get().prepareStatement(queries.copyFrom(warmupCsv))) {
            fromCsv.executeUpdate();
        }

        try (PreparedStatement clean = conn.get().prepareStatement(queries.deleteAll())) {
            clean.executeUpdate();
        }
    }

    @Override public boolean test(Map<Object, Object> ctx) throws Exception {
        try (PreparedStatement fromCsv = conn.get().prepareStatement(queries.copyFrom(realCsv))) {
            fromCsv.executeUpdate();
        }

        //TODO: assert count
        return true;
    }
}
