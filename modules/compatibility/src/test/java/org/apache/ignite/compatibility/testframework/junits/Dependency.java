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

import java.util.Properties;
import org.apache.ignite.internal.IgniteProperties;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Module dependency: Should be filtered out from current test classpath for separate JVM classpath.
 */
public class Dependency {
    /** The Apache Ignite group id required for preparing a classpath in a dedicated JVM. */
    public static final String APACHE_IGNITE_GROUP_ID = "org.apache.ignite";

    /** The project group id. */
    private static final String PROJECT_GROUP_ID;

    /** Local module name. Folder name where module is located. */
    private final String locModuleName;

    /** Group id. */
    private final String groupId;

    /** Artifact id. */
    private final String artifactId;

    /** Version. {@code null} means default Ignite version is to be used. May be used for 3rd party dependencies. */
    @Nullable private final String ver;

    /** Test flag. Test jar should have {@code true} value. */
    private final boolean test;

    /** */
    static {
        Properties props = new Properties();

        IgniteProperties.readProperties("compatibility.properties", props, true);

        PROJECT_GROUP_ID = props.getProperty("ignite.groupId", APACHE_IGNITE_GROUP_ID);
    }

    /**
     * Creates dependency with {@link #PROJECT_GROUP_ID} as group id.
     *
     * @param locModuleName Local module name. Folder name where module is located.
     * @param artifactId Artifact id.
     * @param test Test flag. Test jar should have {@code true} value.
     */
    public Dependency(String locModuleName, String artifactId, boolean test) {
        this(locModuleName, PROJECT_GROUP_ID, artifactId, null, test);
    }

    /**
     * Creates dependency with given parameters.
     *
     * @param locModuleName Local module name. Folder name where module is located.
     * @param groupId Group id.
     * @param artifactId Artifact id.
     * @param ver Dependency version, {@code null} means default Ignite version is to be used.
     * @param test Test flag. Test jar should have {@code true} value.
     */
    public Dependency(@NotNull String locModuleName, @NotNull String groupId, @NotNull String artifactId,
        @Nullable String ver, boolean test) {
        this.locModuleName = locModuleName;
        this.groupId = groupId;
        this.artifactId = artifactId;
        this.ver = ver;
        this.test = test;
    }

    /**
     * @return Template of sources path based on local module name.
     */
    public String sourcePathTemplate() {
        return "modules/" +
            locModuleName +
            "/target/" +
            (test ? "test-classes" : "classes");
    }

    /**
     * @return Template of artifact's path in Maven repository.
     */
    public String artifactPathTemplate() {
        return "repository/" + groupId.replaceAll("\\.", "/") + "/" + artifactId + "/";
    }

    /**
     * @return Dependency artifact id.
     */
    public String artifactId() {
        return artifactId;
    }

    /**
     * @return Classifier or {@code null} depending on {@link #test} flag.
     */
    @Nullable public String classifier() {
        return test ? "tests" : null;
    }

    /**
     * @return Dependency version.
     */
    @Nullable public String version() {
        return ver;
    }

    /**
     * @return Dependency group id.
     */
    public String groupId() {
        return groupId;
    }
}
