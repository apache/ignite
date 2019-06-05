/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.testsuites;

import junit.framework.Test;
import org.h2.test.H2TestSuiteBuilder;
import org.junit.internal.runners.SuiteMethod;
import org.junit.runner.RunWith;

/**
 * H2 test suite with MVStore and networking enabled.
 */
@RunWith(SuiteMethod.class)
public class H2MVStoreWithNetworkingTestSuite {
    /** */
    public static Test suite() {
        H2TestSuiteBuilder builder = new H2TestSuiteBuilder();

        builder.mvStore = true;
        builder.memory = false;
        builder.multiThreaded = false;
        builder.networked = true;

        return builder.buildSuite(H2MVStoreWithNetworkingTestSuite.class, true);
    }
}
