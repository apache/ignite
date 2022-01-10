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
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class UnsafeInstantiationTest {
    private final Instantiation instantiation = new UnsafeInstantiation();

    @Test
    void supportsClassesHavingNoArgConstructor() {
        assertTrue(instantiation.supports(WithPrivateNoArgConstructor.class));
    }

    @Test
    void supportsClassesWithoutNoArgConstructor() {
        assertTrue(instantiation.supports(WithoutNoArgConstructor.class));
    }

    @Test
    void instantiatesClassesHavingNoArgConstructor() throws Exception {
        Object instance = instantiation.newInstance(WithPrivateNoArgConstructor.class);

        assertThat(instance, is(notNullValue()));
    }

    @Test
    void instantiatesClassesWithoutNoArgConstructor() throws Exception {
        Object instance = instantiation.newInstance(WithoutNoArgConstructor.class);

        assertThat(instance, is(notNullValue()));
    }
}
