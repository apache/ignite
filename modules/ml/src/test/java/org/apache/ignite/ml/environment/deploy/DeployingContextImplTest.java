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


package org.apache.ignite.ml.environment.deploy;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.ignite.ml.environment.LearningEnvironment;
import org.apache.ignite.ml.environment.LearningEnvironmentBuilder;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Tests for DeployingContextImpl.
 */
public class DeployingContextImplTest {
    /** */
    private LearningEnvironment environment;

    /** */
    @Before
    public void setUp() throws Exception {
        environment = LearningEnvironmentBuilder.defaultBuilder().buildForTrainer();
    }

    /** */
    @Test
    public void testSimpleCase() {
        environment.initDeployingContext(new A());
        assertEquals(A.class, environment.deployingContext().userClass());
        assertEquals(A.class.getClassLoader(), environment.deployingContext().clientClassLoader());
    }

    /** */
    @Test
    public void testStraightDependency() {
        environment.initDeployingContext(new C(new A()));
        assertEquals(A.class, environment.deployingContext().userClass());
        assertEquals(A.class.getClassLoader(), environment.deployingContext().clientClassLoader());
    }

    /** */
    @Test
    public void testNestedDependencies() {
        environment.initDeployingContext(new C(new C(new C(new A()))));
        assertEquals(A.class, environment.deployingContext().userClass());
        assertEquals(A.class.getClassLoader(), environment.deployingContext().clientClassLoader());
    }

    /** */
    @Test
    public void testClassWithoutDependencies() {
        environment.initDeployingContext(new C(new C(new B(new A()))));
        assertEquals(B.class, environment.deployingContext().userClass());
        assertEquals(B.class.getClassLoader(), environment.deployingContext().clientClassLoader());
    }

    /** */
    @Test
    public void testClassWithSeveralDeps1() {
        // TODO: GG-19105
        environment.initDeployingContext(new C(new C(new D(new C(new C(new A())), new B(new A())))));
        // in this case we should get only first dependency
        assertEquals(A.class, environment.deployingContext().userClass());
        assertEquals(A.class.getClassLoader(), environment.deployingContext().clientClassLoader());
    }

    /** */
    @Test
    public void testClassWithSeveralDeps2() {
        // TODO: GG-19105
        environment.initDeployingContext(new C(new C(new D(new B(new A()), new C(new C(new A()))))));
        // in this case we should get only first dependency
        assertEquals(B.class, environment.deployingContext().userClass());
        assertEquals(B.class.getClassLoader(), environment.deployingContext().clientClassLoader());
    }

    /** */
    private static class A {

    }

    /** */
    private static class B implements DeployableObject {
        /** */
        private final Object obj;

        /** */
        public B(Object obj) {
            this.obj = obj;
        }

        /** {@inheritDoc} */
        @Override public List<Object> getDependencies() {
            return Collections.emptyList();
        }
    }

    /** */
    private static class C implements DeployableObject {
        /** */
        private final Object obj;

        /** */
        public C(Object obj) {
            this.obj = obj;
        }

        /** {@inheritDoc} */
        @Override public List<Object> getDependencies() {
            return Collections.singletonList(obj);
        }
    }

    /** */
    private static class D implements DeployableObject {
        /** */
        private final Object obj1;

        /** */
        private final Object obj2;

        /** */
        public D(Object obj1, Object obj2) {
            this.obj1 = obj1;
            this.obj2 = obj2;
        }

        /** {@inheritDoc} */
        @Override public List<Object> getDependencies() {
            return Arrays.asList(obj1, obj2);
        }
    }
}
