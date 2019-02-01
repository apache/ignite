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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.ignite.compatibility.framework.node.IgniteCompatibilityRemoteNode;
import org.jetbrains.annotations.Nullable;
import org.junit.Assert;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Generates tests for all pairs 'curVer' vs 'prevVer', e.g. if current version is v3 and previous versions in
 * 'ignite-versions' are [v0, v1, v2] then it generate tests for pairs [v3, v0], [v3, v1], [v3, v2].
 */
@RunWith(Parameterized.class)
public abstract class IgniteCompatibilityTwoVersionsAbstractTest extends IgniteCompatibilityAbstractTest {
    /** For quick run during development. */
    private static final boolean SINGLE_TEST = false;

    /**
     * @return List of versions pairs to test.
     */
    @Parameterized.Parameters(name = "{0} vs {1}")
    public static Collection<Object[]> testData() {
        return testDataSince(null);
    }

    /**
     * @param fromVer Minimum supported version.
     * @return List of versions pairs to test.
     */
    public static Collection<Object[]> testDataSince(@Nullable String fromVer) {
        List<Object[]> versions = new ArrayList<>();

        List<String> previous = fromVer == null ?
            IgniteCompatibilityRemoteNode.previousVersions() : getVersionsFrom(fromVer, 1000);

        Assert.assertTrue("Empty list of previous versions", !previous.isEmpty());

        for (String otherVer: previous) {
            versions.add(new Object[]{IgniteCompatibilityTestConfig.get().currentVersion(), otherVer});

            if (SINGLE_TEST)
                break;
        }

        return versions;
    }

    /**
     * @param fromVer Minimum supported version.
     * @return List of versions pairs to test.
     */
    public static Collection<Object[]> testDataBounded(String fromVer, String toVerExclusive) {
        List<Object[]> versions = new ArrayList<>();

        List<String> previous = fromVer == null ?
            IgniteCompatibilityRemoteNode.previousVersions() : getVersionsFrom(fromVer, 1000);

        Assert.assertTrue("Empty list of previous versions", !previous.isEmpty());

        for (String otherVer: previous) {
            if (compareVersions(otherVer, toVerExclusive) < 0)
                versions.add(new Object[]{IgniteCompatibilityTestConfig.get().currentVersion(), otherVer});

            if (SINGLE_TEST)
                break;
        }

        return versions;
    }

    /**
     * @param ver Specified version.
     * @param otherVers Other specified versions.
     * @return List of version pairs to test.
     */
    public static Collection<Object[]> testExactVersions(String ver, String... otherVers) {
        List<Object[]> versions = new ArrayList<>();

        versions.add(new Object[]{IgniteCompatibilityTestConfig.get().currentVersion(), ver});

        for (String otherVer : otherVers)
            versions.add(new Object[]{IgniteCompatibilityTestConfig.get().currentVersion(), otherVer});

        return versions;
    }

    /** */
    @Parameterized.Parameter(0)
    public String curVer;

    /** */
    @Parameterized.Parameter(1)
    public String prevVer;
}
