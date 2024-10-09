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

package org.apache.ignite.ioc;

import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.when;

/** Composite registry test. */
@RunWith(MockitoJUnitRunner.class)
public class CompositeRegistryTest {
    /** Mock bean 1. */
    private static final BeanA BEAN_A = new BeanA();

    /** Mock bean 2. */
    private static final BeanB BEAN_B = new BeanB();

    /** Mock registry 1. */
    @Mock
    Registry registryA;

    /** Mock registry 2. */
    @Mock
    Registry registryB;

    /** Registry. */
    private Registry registry;

    /** Sets up tests. */
    @Before
    public void setUp() {
        registry = new CompositeRegistry(Arrays.asList(registryA, registryB));

        when(registryA.lookup("A")).thenReturn(BEAN_A);
        when(registryB.lookup("B")).thenReturn(BEAN_B);

        when(registryA.lookup(BeanA.class)).thenReturn(BEAN_A);
        when(registryB.lookup(BeanB.class)).thenReturn(BEAN_B);
    }

    /**
     * Tests lookup by name.
     */
    @Test
    public void lookuByName() {
        assertSame(BEAN_A, registry.lookup("A"));
        assertSame(BEAN_B, registry.lookup("B"));
    }

    /**
     * Tests lookup by type.
     */
    @Test
    public void lookuByType() {
        assertSame(BEAN_A, registry.lookup(BeanA.class));
        assertSame(BEAN_B, registry.lookup(BeanB.class));
    }

    /** Bean type 1. */
    private static class BeanA {
    }

    /** Bean type 2. */
    private static class BeanB {
    }
}
