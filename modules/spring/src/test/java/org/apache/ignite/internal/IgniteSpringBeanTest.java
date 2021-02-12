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

package org.apache.ignite.internal;

import org.apache.ignite.IgniteSpringBean;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;
import org.junit.Test;

/**
 *
 */
public class IgniteSpringBeanTest extends GridCommonAbstractTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testInitialization() throws Exception {
        try (IgniteSpringBean bean = new IgniteSpringBean()) {
            bean.setConfiguration(getConfiguration("test"));

            bean.afterSingletonsInstantiated();

            bean.compute();
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testIllegalState() throws Exception {
        IgniteSpringBean bean = new IgniteSpringBean();

        try {
            bean.compute();

            fail("Failed as Ignite should not have been initialized");
        }
        catch (IllegalStateException e) {
            info("Caught expected exception: " + e);
        }
    }
}
