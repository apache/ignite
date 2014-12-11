/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid;

import junit.framework.*;
import org.apache.ignite.*;
import org.gridgain.grid.util.typedef.*;

import java.io.*;

/**
 *
 */
public class GridSuppressedExceptionSelfTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    public void testHasCause() throws Exception {
        IgniteCheckedException me = prepareMultiException();

        assertFalse(me.hasCause(IOException.class));
        assertTrue(me.hasCause(IgniteCheckedException.class));
        assertTrue(me.hasCause(IllegalArgumentException.class));
    }

    /**
     * @throws Exception If failed.
     */
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
     * Made to demonstrate stack printing for {@link IgniteCheckedException}. Do not enable.
     *
     * @throws Exception If failed.
     */
    public void _testStackTrace() throws Exception {
        IgniteCheckedException me = new IgniteCheckedException("Test message.");

        for (int i = 5; i < 20; i++) {
            try {
                generateException(i, null);
            }
            catch (IgniteCheckedException e) {
                me.addSuppressed(e);
            }
        }

        me.printStackTrace();
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
