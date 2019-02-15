/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.compatibility.testframework.junits;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Module dependency: Should be filtered out from current test classpath for separate JVM classpath.
 */
public class Dependency {
    /** Default value of group id. */
    private static final String DEFAULT_GROUP_ID = "org.apache.ignite";

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

    /**
     * Creates dependency with {@link #DEFAULT_GROUP_ID} as group id.
     *
     * @param locModuleName Local module name. Folder name where module is located.
     * @param artifactId Artifact id.
     * @param test Test flag. Test jar should have {@code true} value.
     */
    public Dependency(String locModuleName, String artifactId, boolean test) {
        this(locModuleName, artifactId, null, test);
    }

    /**
     * Creates dependency with {@link #DEFAULT_GROUP_ID} as group id.
     *
     * @param locModuleName Local module name. Folder name where module is located.
     * @param artifactId Artifact id.
     * @param ver Version, {@code null} means default Ignite version is to be used.
     * @param test Test flag. Test jar should have {@code true} value.
     */
    public Dependency(String locModuleName, String artifactId, String ver, boolean test) {
        this(locModuleName, DEFAULT_GROUP_ID, artifactId, ver, test);
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
        return "repository/" + groupId.replaceAll("\\.", "/") + "/" + artifactId;
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
