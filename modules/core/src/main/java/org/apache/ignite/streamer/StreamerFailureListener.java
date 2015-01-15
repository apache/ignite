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

package org.apache.ignite.streamer;

import java.util.*;

/**
 * Streamer failure listener. Asynchronous callback passed to user in case of any failure determined by streamer.
 *
 * @see org.apache.ignite.IgniteStreamer#addStreamerFailureListener(StreamerFailureListener)
 *
 */
public interface StreamerFailureListener {
    /**
     * Callback invoked when unrecoverable failure is detected by streamer.
     * <p>
     * If {@link StreamerConfiguration#isAtLeastOnce()} is set to {@code false}, then this callback
     * will be invoked on node on which failure occurred. If {@link StreamerConfiguration#isAtLeastOnce()}
     * is set to {@code true}, then this callback will be invoked on node on which
     * {@link org.apache.ignite.IgniteStreamer#addEvents(Collection)} or its variant was called. Callback will be called if maximum
     * number of failover attempts exceeded or failover cannot be performed (for example, if router
     * returned {@code null}).
     *
     * @param stageName Failed stage name.
     * @param evts Failed set of events.
     * @param err Error cause.
     */
    public void onFailure(String stageName, Collection<Object> evts, Throwable err);
}
