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

package org.apache.ignite.internal.processors.hadoop.shuffle;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Local shuffle state.
 */
class HadoopShuffleLocalState {
    /** Message counter. */
    private final AtomicLong msgCnt = new AtomicLong();

    /** Reply guard. */
    private final AtomicBoolean replyGuard = new AtomicBoolean();

    /** Total message count.*/
    private volatile long totalMsgCnt;

    /**
     * Callback invoked when shuffle message arrived.
     *
     * @return Whether to perform reply.
     */
    public boolean onShuffleMessage() {
        long msgCnt0 = msgCnt.incrementAndGet();

        return msgCnt0 == totalMsgCnt && reserve();
    }

    /**
     * Callback invoked when shuffle is finished.
     *
     * @param totalMsgCnt Message count.
     * @return Whether to perform reply.
     */
    public boolean onShuffleFinishMessage(long totalMsgCnt) {
        this.totalMsgCnt = totalMsgCnt;

        return msgCnt.get() == totalMsgCnt && reserve();
    }

    /**
     * Reserve reply.
     *
     * @return {@code True} if reserved.
     */
    private boolean reserve() {
        return replyGuard.compareAndSet(false, true);
    }
}
