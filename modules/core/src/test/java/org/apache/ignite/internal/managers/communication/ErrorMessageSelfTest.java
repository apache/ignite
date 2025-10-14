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

package org.apache.ignite.internal.managers.communication;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.typedef.X;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Test for {@link ErrorMessage}. */
public class ErrorMessageSelfTest {
    /** */
    @Test
    public void testDirectAndInsverseConversion() {
        IgniteException e = new IgniteException("Test exception", new IgniteCheckedException("Test cause"));

        ErrorMessage msg0 = new ErrorMessage(e);
        
        assertSame(e, msg0.toThrowable());

        byte[] errBytes = msg0.errorBytes();

        assertNotNull(errBytes);

        ErrorMessage msg1 = new ErrorMessage();
        msg1.errorBytes(errBytes);
        
        Throwable t = msg1.toThrowable();
        
        assertNotNull(t);
        assertTrue(X.hasCause(t, "Test exception", IgniteException.class));
        assertTrue(X.hasCause(t, "Test cause", IgniteCheckedException.class));
    }
    
    /** */
    @Test
    public void testNull() {
        assertNull(new ErrorMessage(null).toThrowable());
        assertNull(new ErrorMessage(null).errorBytes());

        ErrorMessage msg = new ErrorMessage();
        
        msg.errorBytes(null);
        
        assertNull(msg.toThrowable());
        assertNull(msg.errorBytes());
    }
}
