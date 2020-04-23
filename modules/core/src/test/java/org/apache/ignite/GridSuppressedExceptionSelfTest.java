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

package org.apache.ignite;

import java.io.IOException;
import java.util.List;
import org.apache.ignite.internal.util.typedef.X;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 *
 */
public class GridSuppressedExceptionSelfTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testHasCause() throws Exception {
        IgniteCheckedException me = prepareMultiException();

        assertFalse(me.hasCause(IOException.class));
        assertTrue(me.hasCause(IgniteCheckedException.class));
        assertTrue(me.hasCause(IllegalArgumentException.class));
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testGetCause() throws Exception {
        IgniteCheckedException me = prepareMultiException();

        assertNull(me.getCause(IOException.class));

        assertNotNull(me.getCause(IgniteCheckedException.class));
        assertTrue(me.getCause(IgniteCheckedException.class) instanceof IgniteCheckedException);

        assertNotNull(me.getCause(IllegalArgumentException.class));
        assertTrue(me.getCause(IllegalArgumentException.class) instanceof IllegalArgumentException);
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testXHasCause() throws Exception {
        IgniteCheckedException me = prepareMultiException();

        try {
            throw new RuntimeException("Test.", me);
        }
        catch (RuntimeException e) {
            assertFalse(X.hasCause(e, IOException.class));
            assertTrue(X.hasCause(e, IgniteCheckedException.class));
            assertTrue(X.hasCause(e, IllegalArgumentException.class));
        }
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testXGetSuppressedList() throws Exception {
        IgniteCheckedException me = prepareMultiException();

        assertEquals(3, X.getSuppressedList(me).size());

        RuntimeException e = new RuntimeException();
        e.addSuppressed(me);

        List<Throwable> suppresseds = X.getSuppressedList(e);

        assertEquals(4, suppresseds.size());

        assertEquals("Test message.", suppresseds.get(0).getMessage());
        for (int i = 1; i <= 3; i++)
            assertEquals("Demo exception.", suppresseds.get(1).getMessage());
    }

    /**
     * @throws Exception If failed.
     */
    @Test
    public void testXCause() throws Exception {
        IgniteCheckedException me = prepareMultiException();

        try {
            throw new RuntimeException("Test.", me);
        }
        catch (RuntimeException e) {
            assertNull(X.cause(e, IOException.class));

            assertNotNull(X.cause(e, IgniteCheckedException.class));
            assertTrue(X.cause(e, IgniteCheckedException.class) instanceof IgniteCheckedException);

            assertNotNull(X.cause(e, IllegalArgumentException.class));
            assertTrue(X.cause(e, IllegalArgumentException.class) instanceof IllegalArgumentException);
        }
    }

    /**
     * @return A multi exception with few nested causes and
     *  {@link IllegalAccessException} in hierarchy.
     */
    private IgniteCheckedException prepareMultiException() {
        IgniteCheckedException me = new IgniteCheckedException("Test message.");

        for (int i = 0; i < 3; i++) {
            try {
                generateException(3, new IllegalArgumentException());
            }
            catch (IgniteCheckedException e) {
                me.addSuppressed(e);
            }
        }
        return me;
    }

    /**
     * @param calls Stack depth to throw exception.
     * @param cause Cause for the generated exception.
     * @throws IgniteCheckedException Exception.
     */
    private void generateException(int calls, Throwable cause) throws IgniteCheckedException {
        if (calls == 1)
            throw new IgniteCheckedException("Demo exception.", cause);
        else
            generateException(calls - 1, cause);
    }
}
