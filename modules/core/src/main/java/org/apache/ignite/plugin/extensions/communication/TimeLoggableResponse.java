/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.plugin.extensions.communication;

/**
 * Base interface for messages that support message network time measuring.
 */
public interface TimeLoggableResponse extends Message {
    /**
     * Returns request time data which is sum of request send
     * timestamp and request processing time. This value is usable only on
     * request sender node in construction {@code System.nanoTime() - reqTimeData()}<br>
     *
     * NOTE: For every class implementing {@code TimeLoggableResponse} a field storing reqTimeData
     * must be the ONLY one that carries time logging related data between nodes.
     *
     * @return Request time data.
     */
    long reqTimeData();

    /**
     * Sets request time data.
     */
    void reqTimeData(long reqTimeData);
}
