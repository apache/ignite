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
