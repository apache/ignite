/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.maven.plugin;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Functionality to check whether java source file can be compiled and executed on specified Ignite version.
 */
public class FileVersionChecker {
    /** Since annotation. */
    private static final String SINCE_ANNOTATION = "@Since";

    /** Since annotation pattern. */
    private static final Pattern SINCE_ANNOTATION_PATTERN = Pattern.compile(SINCE_ANNOTATION + "\\(\"([0-9.]+)\"\\)");

    /**
     * @param productVer Target Ignite version
     * @param compatibleVer List of compatible versions.
     * @return {@code True} if productVer is compatible with given compatible version.
     */
    private boolean satisfies(String productVer, String compatibleVer) {
        IgniteProductVersion igniteVer = IgniteProductVersion.fromString(productVer);
        IgniteProductVersion compVer = IgniteProductVersion.fromString(compatibleVer);

        return igniteVer.compareTo(compVer) >= 0;
    }

    /**
     * @param file File.
     * @param productVer Ignite version.
     */
    public boolean satisfiesVersion(File file, String productVer) throws IOException {
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String line;

            while ((line = reader.readLine()) != null) {
                if (line.startsWith(SINCE_ANNOTATION)) {
                    Matcher matcher = SINCE_ANNOTATION_PATTERN.matcher(line);

                    if (matcher.matches()) {
                        String version = matcher.group(1);

                        return satisfies(productVer, version);
                    }
                }
            }
        }

        return true;
    }
}
