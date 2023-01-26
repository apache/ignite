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

package org.apache.ignite.client.monitoring;

import java.util.concurrent.TimeUnit;

/** */
public class HandshakeSuccessEvent extends ConnectionEvent {
    /** */
    private final long elapsedTimeNanos;

    /**
     * @param conn Connection description.
     * @param elapsedTimeNanos Elapsed time in nanoseconds.
     */
    public HandshakeSuccessEvent(
        ConnectionDescription conn,
        long elapsedTimeNanos
    ) {
        super(conn);

        this.elapsedTimeNanos = elapsedTimeNanos;
    }

    /**
     * Get the elapsed time of the query.
     *
     * @param timeUnit Desired time unit in which to return the elapsed time.
     * @return the elapsed time.
     */
    public long elapsedTime(TimeUnit timeUnit) {
        return timeUnit.convert(elapsedTimeNanos, TimeUnit.NANOSECONDS);
    }
}
