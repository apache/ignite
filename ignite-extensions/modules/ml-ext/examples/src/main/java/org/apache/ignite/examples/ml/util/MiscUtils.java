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

package org.apache.ignite.examples.ml.util;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.ProtectionDomain;

import static org.apache.ignite.internal.util.IgniteUtils.isWindows;

/** */
public class MiscUtils {
    /** */
    private static String rootPath;

    static {
        try {
            ProtectionDomain domain = MiscUtils.class.getProtectionDomain();

            URI clsUri = domain.getCodeSource().getLocation().toURI();

            // Overcome UNC path problem on Windows (http://www.tomergabel.com/JavaMishandlesUNCPathsOnWindows.aspx)
            if (isWindows() && clsUri.getAuthority() != null)
                clsUri = new URI(clsUri.toString().replace("file://", "file:/"));

            for (Path cur = Paths.get(clsUri); cur != null; cur = cur.getParent()) {
                if (!new File(cur.toFile(), "ml-ext").isDirectory())
                    continue;

                rootPath = cur.toString();
            }

            if (rootPath == null)
                throw new RuntimeException("Failed to resolve root path");
        }
        catch (Exception e) {
            throw new RuntimeException("Failed to resolve resource path", e);
        }
    }

    /** */
    public static File resolveResourceFile(String relativePath) {
        return Paths.get(rootPath, "ml-ext/examples/resources", relativePath).toFile();
    }

    /** */
    public static String resolveIgniteConfig(String fileName) {
        return Paths.get(rootPath, "ml-ext/examples/resources", fileName).toAbsolutePath().toString();
    }
}
