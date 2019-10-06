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
