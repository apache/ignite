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

package org.apache.ignite.testsuites;

import java.util.ArrayList;
import java.util.List;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.model.InitializationError;

/**
 * Test suite for cycled run tests on PR code. <br>
 * This empty suite may be used in case it is needed to run
 * some test subset to reproduce an issue.<br>
 *
 * You may launch and check results on
 * https://ci.ignite.apache.org/viewType.html?buildTypeId=Ignite20Tests_IgniteReproducingSuite
 *
 * This suite is not included into main build.
 */
@RunWith(IgniteReproducingSuite.DynamicReproducingSuite.class)
public class IgniteReproducingSuite {
    /** */
    public static class DynamicReproducingSuite extends Suite {
        /**
         * @return List of test(s) for reproduction some problem.
         */
        private static List<Class<?>> classes() {
            List <Class<?>> suite = new ArrayList<>();

            suite.add(IgniteReproducingSuite.TestStub.class);

            //uncomment to add some test
            //for (int i = 0; i < 100; i++)
            //    suite.add(IgniteCheckpointDirtyPagesForLowLoadTest.class);

            return suite;
        }

        /** */
        public DynamicReproducingSuite(Class<?> cls) throws InitializationError {
            super(cls, classes().toArray(new Class<?>[] {null}));
        }
    }

    /** IMPL NOTE execution of the (empty) test suite was failing with NPE without this stub. */
    @Ignore
    public static class TestStub {
        /** */
        @Test
        public void dummy() {
        }
    }
}
