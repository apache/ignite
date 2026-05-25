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
import org.apache.ignite.internal.util.typedef.internal.U;
import org.junit.Test;

import static org.apache.ignite.marshaller.Marshallers.jdk;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/** Test for {@link ErrorMessage}. */
public class ErrorMessageSelfTest {
    /** */
    @Test
    public void testDirectAndInsverseConversion() throws IgniteCheckedException {
        IgniteException e = new IgniteException("Test exception", new IgniteCheckedException("Test cause"));

        ErrorMessage msg0 = new ErrorMessage(e);

        assertSame(e, msg0.error());

        msg0.prepareMarshal(jdk());

        byte[] errBytes = msg0.errBytes;

        assertNotNull(errBytes);

        ErrorMessage msg1 = new ErrorMessage();
        msg1.errBytes = errBytes;

        msg1.finishUnmarshal(jdk(), U.gridClassLoader());

        Throwable t = msg1.error();
        
        assertNotNull(t);
        assertTrue(X.hasCause(t, "Test exception", IgniteException.class));
        assertTrue(X.hasCause(t, "Test cause", IgniteCheckedException.class));
    }
    
    /** */
    @Test
    public void testNull() {
        assertNull(new ErrorMessage(null).error());
        assertNull(new ErrorMessage(null).errBytes);

        ErrorMessage msg = new ErrorMessage();
        
        msg.errBytes = null;
        
        assertNull(msg.error());
        assertNull(msg.errBytes);
    }
}
