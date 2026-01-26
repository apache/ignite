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
import org.apache.ignite.IgniteException;
import org.apache.ignite.lang.IgniteRunnable;

/** */
public class ExternalRunnableWithSerializableException implements IgniteRunnable {
    /** */
    private static final String EX_MSG = "Message from Exception";

    /** */
    private static final int EX_CODE = 127;

    /** */
    private static final String EX_DETAILS = "Details from Exception";

    /** {@inheritDoc} */
    @Override public void run() {
        throw new SerializableException(EX_MSG, EX_CODE, EX_DETAILS);
    }

    /** Custom {@link Externalizable} Exception */
    public static class SerializableException extends IgniteException {
        /** */
        protected int code;

        /** */
        protected String details;

        /** */
        public SerializableException() {
            // No-op.
        }

        /** */
        public SerializableException(String msg, int code, String details) {
            super(msg);

            this.code = code;
            this.details = details;
        }

        /** */
        public int getCode() {
            return code;
        }

        /** */
        public void setCode(int code) {
            this.code = code;
        }

        /** */
        public String getDetails() {
            return details;
        }

        /** */
        public void setDetails(String details) {
            this.details = details;
        }
    }
}
