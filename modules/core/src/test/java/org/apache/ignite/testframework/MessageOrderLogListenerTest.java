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
package org.apache.ignite.testframework;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link MessageOrderLogListener}
 */
public class MessageOrderLogListenerTest {
    /** */
    @Test
    public void testMessageOrderLogListener() {
        MessageOrderLogListener lsnr = new MessageOrderLogListener("a", "b");

        lsnr.accept("a");
        lsnr.accept("b");

        assertTrue(lsnr.check());

        lsnr.reset();

        lsnr.accept("b");
        lsnr.accept("a");

        assertFalse(lsnr.check());

        lsnr.reset();

        lsnr.accept("b");
        lsnr.accept("a");
        lsnr.accept("b");

        assertFalse(lsnr.check());

        lsnr = new MessageOrderLogListener(new MessageOrderLogListener.MessageGroup(true)
            .add(new MessageOrderLogListener.MessageGroup(false).add("a").add("b"))
            .add(new MessageOrderLogListener.MessageGroup(true).add("c").add("d"))
        );

        lsnr.accept("b");
        lsnr.accept("a");
        lsnr.accept("c");
        lsnr.accept("d");

        assertTrue(lsnr.check());

        lsnr.reset();

        lsnr.accept("b");
        lsnr.accept("a");
        lsnr.accept("d");
        lsnr.accept("c");

        assertFalse(lsnr.check());

        lsnr.reset();

        lsnr.accept("b");
        lsnr.accept("c");
        lsnr.accept("a");
        lsnr.accept("d");

        assertFalse(lsnr.check());

        lsnr = new MessageOrderLogListener(new MessageOrderLogListener.MessageGroup(true)
            .add(
                new MessageOrderLogListener.MessageGroup(false)
                    .add(new MessageOrderLogListener.MessageGroup(true).add("a").add("b"))
                    .add(new MessageOrderLogListener.MessageGroup(true).add("c").add("d"))
            )
            .add("e")
        );

        lsnr.accept("c");
        lsnr.accept("d");
        lsnr.accept("a");
        lsnr.accept("b");
        lsnr.accept("e");

        assertTrue(lsnr.check());
    }
}
