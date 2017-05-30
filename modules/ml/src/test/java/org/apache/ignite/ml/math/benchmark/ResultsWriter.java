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

package org.apache.ignite.ml.math.benchmark;

import com.dropbox.core.DbxException;
import com.dropbox.core.DbxRequestConfig;
import com.dropbox.core.v2.DbxClientV2;
import com.dropbox.core.v2.files.WriteMode;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.UUID;

/** */
class ResultsWriter {
    /** */
    private static final String DROPBOX_PATH
        = "/benchmarks/math.benchmark.results.csv";

    /** */
    private static final String DROPBOX_URL
        = "https://www.dropbox.com/s/r7tcle31r7gaty8/math.benchmark.results.csv";

    /** */
    private static final String ACCESS_TOKEN
        = "1MMmQjEyzGAAAAAAAAAAfDFrQ6oBPPi4NX-iU_VrgmXB2JDXqRHGa125cTkkEQ0V";

    /** */
    private final String dropboxPath;
    /** */
    private final String dropboxUrl;
    /** */
    private final String accessTok;

    /** */
    ResultsWriter(String dropboxPath, String dropboxUrl, String accessTok) {
        this.dropboxPath = dropboxPath;
        this.dropboxUrl = dropboxUrl;
        this.accessTok = accessTok;

        if (dropboxPath == null || dropboxUrl == null || accessTok == null)
            throw new IllegalArgumentException("Neither of dropbox path, URL, access token can be null.");
    }

    /** **/
    ResultsWriter() {
        this(DROPBOX_PATH, DROPBOX_URL, ACCESS_TOKEN);
    }

    /** */
    void append(String res) throws DbxException, IOException {
        if (res == null)
            throw new IllegalArgumentException("benchmark result is null");

        if (dropboxPath == null) {
            System.out.println(res);

            return;
        }

        append(res, client());
    }

    /** */
    private void append(String res, DbxClientV2 client) throws DbxException, IOException {
        File tmp = createTmpFile();

        try (FileOutputStream out = new FileOutputStream(tmp)) {
            client.files().download(dropboxPath).download(out);
        }

        writeResults(res, tmp);

        try (FileInputStream in = new FileInputStream(tmp)) {
            client.files().uploadBuilder(dropboxPath).withMode(WriteMode.OVERWRITE).uploadAndFinish(in);
        }

        if (!tmp.delete())
            System.out.println("Failed to delete " + tmp.getAbsolutePath());

        System.out.println("Uploaded benchmark results to: " + dropboxUrl);
    }

    /** */
    private void writeResults(String res, File tmp) throws IOException {
        final String unixLineSeparator = "\n";

        try (final PrintWriter writer = new PrintWriter(Files.newBufferedWriter(Paths.get(tmp.toURI()),
            StandardOpenOption.APPEND, StandardOpenOption.CREATE))) {
            writer.write(res + unixLineSeparator);
        }
    }

    /** */
    private File createTmpFile() throws IOException {
        File tmp = File.createTempFile(UUID.randomUUID().toString(), ".csv");

        tmp.deleteOnExit();

        return tmp;
    }

    /** */
    private DbxClientV2 client() {
        return new DbxClientV2(DbxRequestConfig.newBuilder("dropbox/MathBenchmark").build(), accessTok);
    }
}
