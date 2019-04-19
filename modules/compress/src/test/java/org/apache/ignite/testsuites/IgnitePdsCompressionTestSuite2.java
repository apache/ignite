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

import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.testframework.junits.DynamicSuite;
import org.junit.runner.RunWith;

import static org.apache.ignite.testsuites.IgnitePdsCompressionTestSuite.enableCompressionByDefault;

/** */
@RunWith(DynamicSuite.class)
public class IgnitePdsCompressionTestSuite2 {
    /**
     * @return Suite.
     */
    public static List<Class<?>> suite() {
        List<Class<?>> suite = new ArrayList<>();

        enableCompressionByDefault();
        IgnitePdsTestSuite2.addRealPageStoreTests(suite, null);

        return suite;
    }
}
