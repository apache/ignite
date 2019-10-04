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

import org.apache.ignite.spi.communication.tcp.TcpCommunicationMetricsListener;

/**
 * Interface for responses that support network time logging.
 * Declares methods that are used on response sender node<br>
 *
 * This is how message network time calculated:<br>
 * <ol>
 *     <li>Send timestamp is written to request when request is sent.
 *          See {@link TimeLoggableRequest#sendTimestamp(long)}<li/>
 *     <li>When request is received on target node it's receive timestamp is written.
 *          See {@link TimeLoggableRequest#receiveTimestamp(long)}.
 *          Later this timestamp is passed to response message which is triggered by received request.
 *          See {@link #reqReceivedTimestamp(long)}<li/>
 *     <li>When response is send back from target node sum of request send timestamp and message process time
 *          is written to response. See {@link #reqTimeData(long)}<li/>
 *     <li>When response is received on initial node timestamp from step 3 is deducted from current time.
 *          See {@link #reqTimeData()}. This leaves time that messages spend in network.<li/>
 * <ol/>
 *
 * @see TcpCommunicationMetricsListener
 */
public interface ProcessingTimeLoggableResponse extends TimeLoggableResponse {
    /** */
    long INVALID_TIMESTAMP = -1;

    /**
     * Returns send timestamp of request that triggered this response
     * in request sender node time. {@code INVALID_TIMESTAMP} if request wasn't
     * {@code TimeLoggableRequest} or it's send timestamp wasn't logged.<br>
     *
     * NOTE: For every class implementing {@code ProcessingTimeLoggableResponse} a field
     * storing reqSentTimestamp MUST be annotated with {@code GridDirectTransient}.
     * It is not supposed to be passed between nodes.
     *
     * @return Send timestamp of request that triggered this response.
     */
    long reqSentTimestamp();

    /**
     * Sets request send timestamp in sender node time.
     */
    void reqSentTimestamp(long reqSentTimestamp);

    /**
     * Returns received timestamp of request that triggered this response
     * in request receiver node time. {@code INVALID_TIMESTAMP} if request wasn't
     * {@code TimeLoggableRequest} or it's timestamp wasn't logged.<br>
     *
     * NOTE: For every class implementing {@code ProcessingTimeLoggableResponse} a field
     * storing reqReceivedTimestamp MUST be annotated with {@code GridDirectTransient}.
     * It is not supposed to be passed between nodes.
     *
     * @return Received timestamp of request that triggered this response.
     */
    long reqReceivedTimestamp();

    /**
     * Sets request receive timestamp in receiver time.
     */
    void reqReceivedTimestamp(long reqReceivedTimestamp);

    /**
     * Copies request timestamps.
     *
     * @param req Request that triggered this response.
     */
    default void copyTimestamps(TimeLoggableRequest req) {
        reqReceivedTimestamp(req.receiveTimestamp());
        reqSentTimestamp(req.sendTimestamp());
    }
}
