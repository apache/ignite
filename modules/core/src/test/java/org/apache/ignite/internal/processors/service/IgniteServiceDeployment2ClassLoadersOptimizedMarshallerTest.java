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
package org.apache.ignite.internal.processors.service;

import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.internal.marshaller.optimized.OptimizedMarshaller;

/**
 * Tests that not all nodes in cluster need user's service definition (only nodes according to filter).
 */
public class IgniteServiceDeployment2ClassLoadersOptimizedMarshallerTest
    extends IgniteServiceDeployment2ClassLoadersDefaultMarshallerTest{
    /** {@inheritDoc} */
    @Override protected Marshaller marshaller() {
        return new OptimizedMarshaller(false);
    }
}
