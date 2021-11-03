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

package org.apache.ignite.internal.network.processor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import org.apache.ignite.network.TestMessagesFactory;
import org.junit.jupiter.api.Test;

/**
 *
 */
public class EmptyMessageTest {
    /**
     *
     */
    private final TestMessagesFactory factory = new TestMessagesFactory();

    /**
     * Test that {@code hashCode} and {@code equals} are generated correctly for empty messages.
     */
    @Test
    public void testEqualsAndHashCode() {
        EmptyMessage msg = factory.emptyMessage().build();

        assertEquals(msg, msg);
        assertNotEquals(factory.serializationOrderMessage().build(), msg);

        assertEquals(EmptyMessageImpl.class.hashCode(), msg.hashCode());
    }
}
