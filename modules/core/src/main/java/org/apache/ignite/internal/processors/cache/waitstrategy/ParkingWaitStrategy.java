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

package org.apache.ignite.internal.processors.cache.waitstrategy;

import java.util.concurrent.locks.LockSupport;
import org.apache.ignite.internal.util.typedef.internal.U;

public class ParkingWaitStrategy implements WaitStrategy {
    /** Next expire time. */
    private volatile long nextExpireTime;

    @Override public void notify0(long expireTime, Thread thread) {
        if (expireTime < nextExpireTime)
            LockSupport.unpark(thread);

    }

    @Override public void waitFor(long expireTime) throws InterruptedException {
        long curTime = U.currentTimeMillis();

        if (expireTime > curTime) {
            long nextExpireTime = expireTime != -1 ? expireTime : curTime + 500;

            this.nextExpireTime = nextExpireTime;

            LockSupport.parkUntil(nextExpireTime);
        }
    }
}
