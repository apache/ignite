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
package org.apache.ignite.internal.processors.marshaller;

import java.util.concurrent.CountDownLatch;

/**
 * A {@link MappedName} implementation allowing to block clients requesting mapping for typeId currently missing in {@link org.apache.ignite.internal.MarshallerContextImpl} local cache.
 *
 * In this case an instance of this class is created and put in the cache so other threads that may request the same mapping will be blocked on the same object.
 * At the same time missing mapping is requested using custom {@link org.apache.ignite.internal.managers.discovery.DiscoveryCustomMessage} event (see more in {@link GridMarshallerMappingProcessor}).
 *
 * Upon receiving a response all threads blocked on the instance get unblocked and are provided with mapping from that response.
 */
public final class MappingRequestFuture implements MappedName {

    private CountDownLatch latch = new CountDownLatch(1);

    private MappedName mappedName;

    private boolean resolutionSuccessful;

    @Override
    public String className() {
        if (mappedName == null)
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        return resolutionSuccessful ? mappedName.className() : null;
    }

    @Override
    public boolean isAccepted() {
        throw new UnsupportedOperationException("Operation is not supported by MappingRequestFuture");
    }

    public void onMappingResolved(MappedName mappedName) {
        this.mappedName = mappedName;
        resolutionSuccessful = true;
        latch.countDown();
    }

    public void onMappingResolutionFailed() {
        resolutionSuccessful = false;
        latch.countDown();
    }
}
