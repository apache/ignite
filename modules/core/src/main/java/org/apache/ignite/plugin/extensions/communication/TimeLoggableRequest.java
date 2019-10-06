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

import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;

/**
 * Common interface for requests that support network time logging.<br>
 *
 * This is how message network time calculated:<br>
 * <ol>
 *     <li>Send timestamp is written to request when request is sent. See {@link #sendTimestamp(long)}<li/>
 *     <li>When request is received on target node it's receive timestamp is written. See {@link #receiveTimestamp(long)}
 *          Later this timestamp is passed to response message which is triggered by received request.
 *          See {@link ProcessingTimeLoggableResponse#reqReceivedTimestamp(long)}<li/>
 *     <li>When response is send back from target node sum of request send timestamp and message process time
 *          is written to response. See {@link TimeLoggableResponse#reqTimeData(long)}<li/>
 *     <li>When response is received on initial node timestamp from step 3 is deducted from current time.
 *          See {@link TimeLoggableResponse#reqTimeData()}. This leaves time that messages spend in network.<li/>
 * <ol/>
 *
 * @see TcpCommunicationMetricsListener
 */
public interface TimeLoggableRequest extends Message {
    /**
     * @return Message send timestamp in sender node time.
     */
    long sendTimestamp();

    /**
     * Sets send timestamp.
     */
    void sendTimestamp(long sendTimestamp);

    /**
     * @return Message receive timestamp in receiver node time.
     */
    long receiveTimestamp();

    /**
     * Sets receive timestamp.
     */
    void receiveTimestamp(long receiveTimestamp);
}
