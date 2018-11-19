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

package org.apache.ignite.compatibility.testframework.junits;

import org.jetbrains.annotations.Nullable;

/**
 * Module dependency: Should be filtered out from current test classpath for separate JVM classpath.
 */
public class Dependency {
    /** Local module name. Folder name where module is located. */
    private String locModuleName;

    /** Group name. Null means ignite default group name. */
    @Nullable
    private String groupName;

    /** Artifact name (artifact ID) without group name. */
    private String artifactName;

    /** Version. Null means default Ignite version is to be used. May be used for 3rd party dependencies. */
    @Nullable
    private String version;

    /** Test flag. Test jar should have {@code true} value. Default is {@code false}. */
    private boolean test;

    /** */
    private String exludePathFromClassPath;

    /**
     * Creates dependency.
     *
     * @param locModuleName Local module name. Folder name where module is located.
     * @param artifactName Artifact name (artifact ID) without group name.
     * @param test Test flag. Test jar should have {@code true} value. Default is {@code false}.
     */
    public Dependency(String locModuleName, String artifactName, boolean test) {
        this.locModuleName = locModuleName;
        this.artifactName = artifactName;
        this.test = test;
    }

    /**
     * Creates dependency.
     *
     * @param locModuleName Local module name. Folder name where module is located.
     * @param artifactName Artifact name (artifact ID) without group name.
     */
    public Dependency(String locModuleName, String artifactName) {
        this.locModuleName = locModuleName;
        this.artifactName = artifactName;
    }

    /**
     * @param locModuleName Local module name. Folder name where module is located.
     * @param grpName Group name. Null means ignite default group name.
     * @param artifactName Artifact name (artifact ID) without group na
     * @param version Version. Null means default Ignite version is to be used. M
     */
    public Dependency(String locModuleName, @Nullable String grpName, String artifactName, @Nullable String version) {
        this(locModuleName, grpName, artifactName, version, false);
    }

    /**
     * @param excludeName Local module name or part of exclude path.
     * @param grpName Group name. Null means ignite default group name.
     * @param artifactName Artifact name (artifact ID) without group na
     * @param version Version. Null means default Ignite version is to be used. M
     * @param exludeNotLocModule {@code true} In case param @excludeName should exclude path instead of local module.
     */
    public Dependency(String excludeName, @Nullable String grpName, String artifactName, @Nullable String version,
        boolean exludeNotLocModule) {
        if (exludeNotLocModule)
            this.exludePathFromClassPath = excludeName;
        else
            this.locModuleName = excludeName;

        this.groupName = grpName;
        this.artifactName = artifactName;
        this.version = version;
    }

    /**
     * @return Path to exclude form classpath.
     */
    public String excludePathFromClassPath() {
        return exludePathFromClassPath;
    }

    /**
     * @return path based on local module name to exclude from classpath
     */
    public String localPathTemplate() {
        return "modules/" +
            locModuleName +
            "/target/" +
            (test ? "test-classes" : "classes");
    }

    /**
     * @return {@link #artifactName}
     */
    public String artifactName() {
        return artifactName;
    }

    /**
     * @return classifier or {@code} null depending on {@link #test} flag
     */
    @Nullable public String classifier() {
        return test ? "tests" : null;
    }

    /**
     * @return {@link #version}
     */
    @Nullable public String version() {
        return version;
    }

    /**
     * @return {@link #groupName}
     */
    @Nullable public String groupName() {
        return groupName;
    }
}
