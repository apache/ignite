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

package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteFutureTimeoutCheckedException;
import org.apache.ignite.internal.pagemem.store.PageStore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class CheckpointFsyncScope {
    private List<Stripe> independentStripes = new ArrayList<>();

    @NotNull public Stripe newStripe() {
        Stripe stripeScope = new Stripe();
        independentStripes.add(stripeScope);
        return stripeScope;
    }

    public void get() throws IgniteCheckedException {
        for (Stripe next : independentStripes) {
            next.future.get();
        }
    }

    public List<Map.Entry<PageStore, LongAdder>> updatedStores() {
        return independentStripes.stream()
            .flatMap(scope -> scope.fsyncScope.entrySet().stream())
            .collect(Collectors.toList());
    }

    public List<Stripe> stripes() {
        return independentStripes;
    }

    public int stripesCount() {
        return independentStripes.size();
    }

    public static class Stripe {
        public ConcurrentHashMap<PageStore, LongAdder> fsyncScope = new ConcurrentHashMap<>();
        public CountDownDynamicFuture future = new CountDownDynamicFuture(0);

        public void incrementTasksCount() {
            future.incrementTasksCount();
        }

        public void decrementTasksCount() {
            future.onDone((Void)null);
        }

        public boolean isWriteDone() {
            return future.isDone();
        }

        public Set<Map.Entry<PageStore, LongAdder>> waitAndCheckForErrors() throws IgniteCheckedException {
            future.get();

            return fsyncScope.entrySet();
        }

        @Nullable
        public Set<Map.Entry<PageStore, LongAdder>> tryWaitAndCheckForErrors(long ms) throws IgniteCheckedException {
            try {
                future.get(ms);
            }
            catch (IgniteFutureTimeoutCheckedException ignored) {
                return null;
            }

            return fsyncScope.entrySet();
        }
    }
}
