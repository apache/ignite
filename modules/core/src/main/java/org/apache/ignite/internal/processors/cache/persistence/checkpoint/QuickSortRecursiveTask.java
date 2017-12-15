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

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;

/**
 *
 */
class QuickSortRecursiveTask implements Callable<Void> {
    private static final int ONE_CHUNK_THRESHOLD = 1024 * 16;
    /** Source array to sort. */
    private final FullPageId[] array;
    /** Start position. Index of first element inclusive. */
    private final int position;
    /** Limit. Index of last element exclusive. */
    private final int limit;

    CpSettings settings;

    public QuickSortRecursiveTask(FullPageId[] arr,
        Comparator<FullPageId> comp,
        IgniteClosure<FullPageId[], Callable<Void>> taskFactory,
        IgniteInClosure<Callable<Void>> forkSubmitter, int checkpointThreads,
        ForkNowForkLaterStrategy strategy) {
        this(arr, 0, arr.length,  new CpSettings(comp, taskFactory, forkSubmitter, checkpointThreads));
    }

    private QuickSortRecursiveTask(FullPageId[] arr, int position, int limit,
        CpSettings settings) {
        this.array = arr;
        this.position = position;
        this.limit = limit;
        this.settings = settings;
    }

    private static boolean isUnderThreshold(int cnt) {
        return cnt < ONE_CHUNK_THRESHOLD;
    }

    @Override public Void call() throws Exception {
        final int remaining = limit - position;
        if (remaining == 0)
            return null;

        Comparator<FullPageId> comp = settings.comp;
        if (isUnderThreshold(remaining)) {
            final FullPageId[] arrCopy = Arrays.copyOfRange(array, position, limit);

            Arrays.sort(arrCopy, comp);

            runPayload(arrCopy);
        }
        else {
            int centerIndex = partition(array, position, limit, comp);
            Callable<Void> t1 = new QuickSortRecursiveTask(array, position, centerIndex, settings);
            Callable<Void> t2 = new QuickSortRecursiveTask(array, centerIndex, limit, settings);

            if (settings.runningWriters.get() < settings.checkpointThreads / 2) {
                t1.call(); //to low number of writers, try to get to bottom and start asap
                settings.forkSubmitter.apply(t2);
            } else {
                //half of threads are already write
                settings.forkSubmitter.apply(t2);
                t1.call();
            }
        }
        return null;
    }

    private void runPayload(FullPageId[] data) throws Exception {
        final Callable<Void> apply = settings.taskFactory.apply(data);
        settings.runningWriters.incrementAndGet();
        try {
            apply.call();
        }
        finally {
            settings.runningWriters.decrementAndGet();
        }
    }

    public static int partition(FullPageId[] arr, int position, int limit,
        Comparator<FullPageId> comp) {
        int left = position;
        int right = limit - 1;
        final int randomIdx = (limit - position) / 2 + position;
        FullPageId referenceElement = arr[randomIdx]; // taking middle element as reference

        while (left <= right) {
            //searching number which is greater than reference
            while (comp.compare(arr[left], referenceElement) < 0)
                left++;
            //searching number which is less than reference
            while (comp.compare(arr[right], referenceElement) > 0)
                right--;

            // swap the values
            if (left <= right) {
                swap(arr, left, right);

                //increment left index and decrement right index
                left++;
                right--;
            }
        }
        return left;
    }

    static void swap(FullPageId[] arr, int i, int j) {
        FullPageId tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

    private static class CpSettings {
        private final IgniteClosure<FullPageId[], Callable<Void>> taskFactory;

        private final IgniteInClosure<Callable<Void>> forkSubmitter;

        private final Comparator<FullPageId> comp;

        private final AtomicInteger runningWriters = new AtomicInteger();

        private int checkpointThreads;

        CpSettings(Comparator<FullPageId> comp,
            IgniteClosure<FullPageId[], Callable<Void>> taskFactory,
            IgniteInClosure<Callable<Void>> forkSubmitter,
            int checkpointThreads) {
            this.comp = comp;
            this.taskFactory = taskFactory;
            this.forkSubmitter = forkSubmitter;
            this.checkpointThreads = checkpointThreads;
        }
    }
}
