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

package org.apache.ignite.tests.p2p.compute;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Field;
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteRunnable;

/** */
public class ExternalRunnableWithExternalizableException implements IgniteRunnable {
    /** */
    private static final String EX_MSG = "Message from Exception";

    /** {@inheritDoc} */
    @Override public void run() {
        throw new ExternalizableException(EX_MSG);
    }

    /** Custom {@link Externalizable} Exception */
    public static class ExternalizableException extends IgniteException implements Externalizable {
        /** */
        public ExternalizableException() {
            // No-op.
        }

        /** */
        public ExternalizableException(String msg) {
            super(msg);
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            out.writeObject(getMessage());
            out.writeObject(getStackTrace());
            out.writeObject(getCause());
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            String msg = (String)in.readObject();
            setMessage(msg);

            setStackTrace((StackTraceElement[])in.readObject());

            Throwable cause = (Throwable)in.readObject();

            if (cause != null)
                initCause(cause);
        }

        /** */
        private void setMessage(String msg) {
            try {
                Field detailMsg = Throwable.class.getDeclaredField("detailMessage");

                detailMsg.setAccessible(true);
                detailMsg.set(this, msg);
            }
            catch (Exception e) {
                throw new RuntimeException("Deserialization for exception is broken!", e);
            }
        }
    }
}
