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

package org.apache.ignite.gridify;

import org.apache.ignite.compute.gridify.aop.spring.GridifySpringEnhancer;
import org.apache.ignite.testframework.junits.common.GridCommonTest;

/**
 * Spring AOP test.
 */
@GridCommonTest(group="AOP")
public class SpringAopSelfTest extends AbstractAopTest {
    /** {@inheritDoc} */
    @Override protected Object target() {
        return GridifySpringEnhancer.enhance(new TestAopTarget());
    }

    /** {@inheritDoc} */
    @Override public String getTestIgniteInstanceName() {
        return "TestAopTargetInterface";
    }

    /** {@inheritDoc} */
    @Override protected Object targetWithUserClassLoader() throws Exception {
        Object res = super.targetWithUserClassLoader();

        ClassLoader cl = Thread.currentThread().getContextClassLoader();

        Thread.currentThread().setContextClassLoader(res.getClass().getClassLoader());

        res = GridifySpringEnhancer.enhance(res);

        Thread.currentThread().setContextClassLoader(cl);

        return res;
    }
}