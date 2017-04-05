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

package org.apache.ignite.internal.util.nio;

import org.apache.ignite.spi.communication.BackPressureTracker;

/**
 * Back pressure wrapper around {@link GridNioMessageTracker}.
 */
public class BackPressureNioTracker implements BackPressureTracker {
    /** */
    private final GridNioMessageTracker tracker;

    /**
     * @param tracker Nio message tracker.
     */
    public BackPressureNioTracker(final GridNioMessageTracker tracker) {
        assert tracker != null;

        this.tracker = tracker;
    }

    /** {@inheritDoc} */
    @Override public void registerMessage() {
        tracker.onMessageReceived();
    }

    /** {@inheritDoc} */
    @Override public void deregisterMessage() {
        tracker.run();
    }
}
