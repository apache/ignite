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

package org.apache.ignite.network;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceArray;
import org.apache.ignite.network.annotations.MessageGroup;
import org.junit.jupiter.api.Test;

/**
 * Test suite for the {@link AbstractMessagingService} class.
 */
public class AbstractMessagingServiceTest {
    /**
     * Tests a situation when multiple modules declare message group descriptors with the same group ID. Adding handlers for both of these
     * groups should result in an exception being thrown.
     *
     * <p>Since we can't declare multiple message groups in a single module, we have to reside to hacks using reflection.
     */
    @Test
    public void testGroupIdClash() throws Exception {
        var messagingService = mock(
                AbstractMessagingService.class,
                withSettings().useConstructor().defaultAnswer(CALLS_REAL_METHODS)
        );

        // get the static inner class that is stored inside the handlers list
        Class<?> handlerClass = AbstractMessagingService.class.getDeclaredClasses()[0];

        Constructor<?> constructor = handlerClass.getDeclaredConstructor(Class.class, List.class);
        constructor.setAccessible(true);
        // create a dummy handler
        Object dummyHandler = constructor.newInstance(Object.class, List.of());

        short groupType = TestMessageTypes.class.getAnnotation(MessageGroup.class).groupType();

        // get the array of handlers and inject the dummy handler
        Field handlersField = AbstractMessagingService.class.getDeclaredField("handlersByGroupType");
        handlersField.setAccessible(true);

        var handlers = (AtomicReferenceArray) handlersField.get(messagingService);
        // use the groupType of the TestMessageTypes class to get a clash
        handlers.set(groupType, dummyHandler);

        Exception e = assertThrows(
                IllegalArgumentException.class,
                () -> messagingService.addMessageHandler(TestMessageTypes.class, (m, s, c) -> {
                })
        );

        assertThat(e.getMessage(), startsWith("Handlers are already registered"));
    }
}
