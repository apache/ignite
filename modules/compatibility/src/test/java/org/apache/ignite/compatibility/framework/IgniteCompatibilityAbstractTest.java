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

package org.apache.ignite.compatibility.framework;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.LogManager;
import java.util.logging.Logger;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.compatibility.framework.node.IgniteCompatibilityRemoteNode;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteProductVersion;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestRule;

import static org.apache.ignite.internal.processors.cache.persistence.file.FilePageStoreManager.DFLT_STORE_DIR;
import static org.springframework.util.FileSystemUtils.deleteRecursively;

/**
 *
 */
public class IgniteCompatibilityAbstractTest {
    /** */
    private static final String LOG_CONFIG = "/java.util.logging.properties";

    static {
        InputStream in = IgniteCompatibilityAbstractTest.class.getResourceAsStream(LOG_CONFIG);

        if (in == null)
            throw new RuntimeException("Failed to load logging configuration: " + LOG_CONFIG);

        try {
            LogManager.getLogManager().readConfiguration(in);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read logging configuration: " + LOG_CONFIG, e);
        }

        Runtime.getRuntime().addShutdownHook(IgniteCompatibilityTestConfig.get().shutdownHook());
    }

    /** */
    protected Logger log = Logger.getLogger(getClass().getName());

    /** */
    @Rule
    public TestRule runRule = IgniteCompatibilityTestConfig.get().runRule(getTestTimeout());

    /**
     * @return Test timeout.
     */
    protected long getTestTimeout() {
        return 10 * 60_000;
    }

    /**
     * @throws Exception If failed.
     */
    @Before
    public void cleanPersistenceDir() throws Exception {
        cleanPersistenceDir(U.defaultWorkDirectory());

        String igniteHome = IgniteSystemProperties.getString(IgniteSystemProperties.IGNITE_HOME);

        FilenameFilter workDirFilter = new FilenameFilter() {
            @Override public boolean accept(File dir, String name) {
                return name.startsWith(IgniteCompatibilityRemoteNode.WORK_DIR_PREFIX);
            }
        };

        File[] files = new File(igniteHome).listFiles(workDirFilter);

        if (files != null) {
            for (File f : files)
                deleteRecursively(f);
        }
    }

    /**
     * @param workDir Work directory.
     * @throws Exception If failed.
     */
    private void cleanPersistenceDir(String workDir) throws Exception {
        U.delete(U.resolveWorkDirectory(workDir, "cp", false));
        U.delete(U.resolveWorkDirectory(workDir, DFLT_STORE_DIR, false));
        U.delete(U.resolveWorkDirectory(workDir, "marshaller", false));
        U.delete(U.resolveWorkDirectory(workDir, "binary_meta", false));
        U.delete(U.resolveWorkDirectory(workDir, "snapshot", false));
    }

    /**
     * @param verStr1 First version.
     * @param verStr2 Second version.
     * @return Comparison result.
     */
    public static int compareVersions(String verStr1, String verStr2) {
        IgniteProductVersion ver1 = IgniteProductVersion.fromString(verStr1);
        IgniteProductVersion ver2 = IgniteProductVersion.fromString(verStr2);

        return ver1.compareTo(ver2);
    }

    /**
     * @param fromVer Minimum version.
     * @param cnt Max number of versions.
     * @return Versions.
     */
    public static List<String> getVersionsFrom(String fromVer, int cnt) {
        List<String> res = new ArrayList<>();

        for (String prevVer : IgniteCompatibilityRemoteNode.previousVersions()) {
            if (compareVersions(prevVer, fromVer) >= 0) {
                res.add(prevVer);

                if (res.size() >= cnt)
                    break;
            }
        }

        assert !res.isEmpty() : "Failed to find previous versions from: " + fromVer;

        return res;
    }

    /**
     * @param beforeVer Maximum version (exclusive).
     * @param cnt Max number of versions.
     * @return Versions.
     */
    public static List<String> getVersionsBefore(String beforeVer, int cnt) {
        List<String> res = new ArrayList<>();

        for (String prevVer : IgniteCompatibilityRemoteNode.previousVersions()) {
            if (compareVersions(prevVer, beforeVer) < 0)
                res.add(prevVer);

            if (res.size() >= cnt)
                break;
        }

        return res;
    }
}
