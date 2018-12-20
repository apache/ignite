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

package org.apache.ignite.testframework.configvariations;

import java.util.IdentityHashMap;
import java.util.Map;
import junit.framework.JUnit4TestAdapter;
import junit.framework.TestResult;
import org.apache.ignite.testframework.junits.IgniteConfigVariationsAbstractTest;

/**
 * Mediates passing config variations to respective tests.
 */
class ConfigVariationsMediator {
    /** IMPL NOTE this relies on serialized execution of test cases. */
    private static final Map<JUnit4TestAdapter, VariationsTestsConfig> map = new IdentityHashMap<>();

    /** */
    private final JUnit4TestAdapter adapter;

    /** */
    ConfigVariationsMediator(Class<? extends IgniteConfigVariationsAbstractTest> cls, VariationsTestsConfig cfg) {
        adapter = prepare(cls, cfg);
    }

    /** */
    JUnit4TestAdapter adapter() {
        return adapter;
    }

    /** */
    private JUnit4TestAdapter prepare(Class<? extends IgniteConfigVariationsAbstractTest> cls,
        VariationsTestsConfig cfg) {
        JUnit4TestAdapter res = new JUnit4TestAdapter(cls) {
            final JUnit4TestAdapter self = this;

            @Override public void run(TestResult tr) {
                IgniteConfigVariationsAbstractTest.injectTestsConfiguration(map.get(self));

                super.run(tr);
            }
        };

        map.put(res, cfg);

        return res;
    }
}
