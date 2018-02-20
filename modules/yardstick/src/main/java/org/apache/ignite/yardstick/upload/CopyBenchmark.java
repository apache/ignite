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
import java.io.PrintStream;
import java.sql.PreparedStatement;
import java.util.Map;
import org.yardstickframework.BenchmarkUtils;

/**
 * Single shot benchmark for the COPY FROM... sql query.
 * Measures total time of load number of rows from csv file using thin driver
 */
public class CopyBenchmark extends AbstractUploadBenchmark {
    /** csv file for warmup */
    private String warmupCsv;

    /** csv file for benchmared action */
    private String realCsv;

    /** {@inheritDoc} */
    @Override protected void init() {
        super.init();

        warmupCsv = generateWarmupCsv();
        realCsv = generateRealCsv();
    }

    /** Generate csv file for copy operation being benchmarked. */
    private String generateRealCsv() {
        String prefix = "data-" + INSERT_SIZE;

        return generate(prefix, INSERT_SIZE);
    }

    /** Generate csv file for copy operation being performed during warmup. */
    private String generateWarmupCsv() {
        return generate("warmup", WARMUP_ROWS_CNT);
    }

    /**
     * Generate csv file with specified prefix and containing specified number
     * of records (lines).
     * <br/>
     * Each record contain incremental (starting 1) id and set of random values.
     * <br/>
     * File is created in current directory and is deleted on JVM exits.
     *
     * @param filePrefix prefix for csv to be generated.
     * @param records how many csv lines generate.
     *
     * @return absolute path for generated csv file.
     */
    private String generate(String filePrefix, long records){
        try {
            File workDir = new File(System.getProperty("user.dir"));

            File f = File.createTempFile(filePrefix, ".csv", workDir);

            f.deleteOnExit();

            BenchmarkUtils.println("Generating file: " + f.getAbsolutePath());

            try (PrintStream out = new PrintStream(f)) {
                for (long id = 1; id <= records; id++) {
                    String csvLine = queries.randomCsvLine(id);
                    out.println(csvLine);
                }
            }

            long sizeMb = f.length() / (1024L * 1024L);

            BenchmarkUtils.println("File have been generated (" + sizeMb + "MiB). It will be removed on exit");

            return f.getAbsolutePath();
        }
        catch (Exception ex){
            throw new RuntimeException("Couldn't generate CSV file, terminating", ex);
        }
    }

    /** {@inheritDoc} */
    @Override protected void warmup() throws Exception {
        try (PreparedStatement fromCsv = conn.get().prepareStatement(queries.copyFrom(warmupCsv))) {
            fromCsv.executeUpdate();
        }

        try (PreparedStatement clean = conn.get().prepareStatement(queries.deleteAll())) {
            clean.executeUpdate();
        }
    }

    /** {@inheritDoc} */
    @Override public void upload() throws Exception {
        try (PreparedStatement fromCsv = conn.get().prepareStatement(queries.copyFrom(realCsv))) {
            fromCsv.executeUpdate();
        }

        //TODO: assert count
    }
}
