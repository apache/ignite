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

package org.apache.ignite.testframework.junits.multijvm;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.ignite.internal.util.GridJavaProcess;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.Assert.assertTrue;

/**
 * Utility to work with 'java -version' command.
 */
class JavaVersionCommand {
    /**
     * Obtains major Java version for Java located at the given path.
     *
     * @param javaHome Java home path.
     * @return Major Java version (like 8 or 11).
     * @throws IOException If something goes wrong.
     */
    int majorVersion(String javaHome) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(GridJavaProcess.resolveJavaBin(javaHome), "-version").start();
        assertTrue(process.waitFor(10, TimeUnit.SECONDS));

        if (process.exitValue() != 0) {
            throw new IllegalStateException("'java -version' failed, stdin '" +
                readStream(process.getInputStream()) + "', stdout '" +
                readStream(process.getErrorStream()) + "'");
        }

        String versionOutput = readStream(process.getErrorStream());

        return JavaVersionCommandParser.extractMajorVersion(versionOutput);
    }

    /**
     * Reads whole stream content and returns it as a string. UTF-8 is used to convert from bytes to string.
     *
     * @param inputStream   Stream to read.
     * @return Stream content as a string.
     * @throws IOException If something goes wrong.
     */
    private String readStream(InputStream inputStream) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        IOUtils.copy(inputStream, baos);
        return new String(baos.toByteArray(), UTF_8);
    }
}
