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

import org.jetbrains.annotations.Nullable;

/** */
public class ConnectionClosedEvent extends ConnectionEvent {
    /** */
    private final Throwable throwable;

    /**
     * @param conn Connection description.
     * @param throwable Throwable that caused the failure if any.
     */
    public ConnectionClosedEvent(
        ConnectionDescription conn,
        Throwable throwable
    ) {
        super(conn);

        this.throwable = throwable;
    }

    /**
     * Get a cause of the failure if any.
     *
     * @return A cause of the failure if any.
     */
    @Nullable public Throwable throwable() {
        return throwable;
    }
}
