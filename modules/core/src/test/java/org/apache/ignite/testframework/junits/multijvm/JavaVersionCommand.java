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

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import org.apache.ignite.GridTestIoUtils;
import org.apache.ignite.internal.util.GridJavaProcess;
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
        Process proc = new ProcessBuilder(GridJavaProcess.resolveJavaBin(javaHome), "-version").start();
        assertTrue(proc.waitFor(10, TimeUnit.SECONDS));

        if (proc.exitValue() != 0) {
            throw new IllegalStateException("'java -version' failed, stdin '" +
                GridTestIoUtils.readStream(proc.getInputStream()) + "', stdout '" +
                GridTestIoUtils.readStream(proc.getErrorStream()) + "'");
        }

        String verOutput = GridTestIoUtils.readStream(proc.getErrorStream());

        return JavaVersionCommandParser.extractMajorVersion(verOutput);
    }
}
