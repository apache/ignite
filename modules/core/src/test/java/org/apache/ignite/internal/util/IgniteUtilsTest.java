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

package org.apache.ignite.internal.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Test suite for {@link IgniteUtils}.
 */
class IgniteUtilsTest {
    /**
     * Tests that all resources are closed by the {@link IgniteUtils#closeAll} even if {@link AutoCloseable#close} throws an exception.
     */
    @Test
    void testCloseAll() {
        class TestCloseable implements AutoCloseable {
            private boolean closed = false;

            /** {@inheritDoc} */
            @Override
            public void close() throws Exception {
                closed = true;

                throw new IOException();
            }
        }

        var closeables = List.of(new TestCloseable(), new TestCloseable(), new TestCloseable());

        Exception e = assertThrows(IOException.class, () -> IgniteUtils.closeAll(closeables));

        assertThat(e.getSuppressed(), arrayWithSize(2));

        closeables.forEach(c -> assertTrue(c.closed));
    }
}
