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

package org.apache.ignite.cdc;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

/** */
public class ConflictResolvableTestData {
    /** */
    public static final AtomicLong REQUEST_ID = new AtomicLong();

    /** */
    private final byte[] payload;

    /** */
    private final long reqId;

    /** */
    public ConflictResolvableTestData(byte[] payload, long reqId) {
        this.payload = payload;
        this.reqId = reqId;
    }

    /**
     * @return Generated data object.
     */
    public static ConflictResolvableTestData create() {
        byte[] payload = new byte[1024];

        ThreadLocalRandom.current().nextBytes(payload);

        return new ConflictResolvableTestData(payload, REQUEST_ID.incrementAndGet());
    }

    /** {@inheritDoc} */
    @Override public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        ConflictResolvableTestData data = (ConflictResolvableTestData)o;
        return reqId == data.reqId && Arrays.equals(payload, data.payload);
    }

    /** {@inheritDoc} */
    @Override public int hashCode() {
        int result = Objects.hash(reqId);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }
}
