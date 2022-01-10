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

package org.apache.ignite.internal.network.serialization.marshal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class BestEffortInstantiationTest {
    @Mock
    private Instantiation delegate1;
    @Mock
    private Instantiation delegate2;

    private BestEffortInstantiation instantiation;

    @BeforeEach
    void initMocks() throws Exception {
        lenient().when(delegate1.newInstance(any())).thenReturn("first");
        lenient().when(delegate2.newInstance(any())).thenReturn("second");
    }

    @BeforeEach
    void createObjectUnderTest() {
        instantiation = new BestEffortInstantiation(delegate1, delegate2);
    }

    @Test
    void whenFirstDelegateSupportsThenThisSupports() {
        when(delegate1.supports(any())).thenReturn(true);

        assertTrue(instantiation.supports(Object.class));
    }

    @Test
    void whenOnlySecondDelegateSupportsThenThisSupports() {
        when(delegate2.supports(any())).thenReturn(true);

        assertTrue(instantiation.supports(Object.class));
    }

    @Test
    void whenNoDelegateSupportsThenThisDoesNotSupport() {
        assertFalse(instantiation.supports(Object.class));
    }

    @Test
    void whenFirstDelegateSupportsThenItIsUsedForInstantiation() throws Exception {
        when(delegate1.supports(any())).thenReturn(true);

        Object instance = instantiation.newInstance(Object.class);

        assertThat(instance, is("first"));
    }

    @Test
    void whenOnlySecondDelegateSupportsThenItIsUsedForInstantiation() throws Exception {
        when(delegate2.supports(any())).thenReturn(true);

        Object instance = instantiation.newInstance(Object.class);

        assertThat(instance, is("second"));
    }

    @Test
    void whenNoDelegateSupportsThenInstantiationFails() {
        InstantiationException ex = assertThrows(InstantiationException.class, () -> instantiation.newInstance(Object.class));
        assertThat(ex.getMessage(), is("No delegate supports " + Object.class));
    }
}
