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

import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Parses output of 'java -version'.
 */
class JavaVersionCommandParser {
    /** Pattern for parsing 'java -version' command output. */
    private static final Pattern versionPattern = Pattern.compile("(java|openjdk) version \"([^\"]+)\".*", Pattern.DOTALL);

    /**
     * Extracts major java version (like '17' or '1.8') from 'java -version' output.
     *
     * @param versionCommandOutput Output to parse.
     * @return Major java version.
     */
    static int extractMajorVersion(String versionCommandOutput) {
        Matcher matcher = versionPattern.matcher(versionCommandOutput);
        if (!matcher.matches()) {
            throw new IllegalArgumentException("Cannot parse the following as java version output: '" +
                versionCommandOutput + "'");
        }

        String fullJavaVersion = matcher.group(2);

        return U.majorJavaVersion(fullJavaVersion);
    }
}
