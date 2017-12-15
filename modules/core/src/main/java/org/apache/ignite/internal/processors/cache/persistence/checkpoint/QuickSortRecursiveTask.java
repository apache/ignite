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
import java.util.Collection;
import java.util.Comparator;
import java.util.concurrent.Callable;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.lang.IgniteInClosure;

/**
 * Task for sorting pages for sequential write, replicated its subtasks as new callable.
 * Uses strategy to determine fork now or later.
 */
class QuickSortRecursiveTask implements Callable<Void> {
    /** One chunk threshold. Determines when to start apply single threaded sort and write. */
    private static final int ONE_CHUNK_THRESHOLD = 1024 * 16;

    /** One write chunk threshold. */
    private static final int ONE_WRITE_CHUNK_THRESHOLD = 1024;

    /** Source array to sort. Shared between threads */
    private final FullPageId[] arr;

    /** Start position. Index of first element inclusive. */
    private final int position;

    /** Limit. Index of last element exclusive. */
    private final int limit;

    /** This task global settings. */
    private final CpSettings settings;

    /**
     * @param arr Array.
     * @param comp Comparator.
     * @param taskFactory Task factory.
     * @param forkSubmitter Fork submitter.
     * @param checkpointThreads Checkpoint threads.
     * @param stgy Strategy.
     */
    QuickSortRecursiveTask(FullPageId[] arr,
        Comparator<FullPageId> comp,
        IgniteClosure<FullPageId[], Callable<Void>> taskFactory,
        IgniteInClosure<Callable<Void>> forkSubmitter, int checkpointThreads,
        ForkNowForkLaterStrategy stgy) {
        this(arr, 0, arr.length, new CpSettings(comp, taskFactory, forkSubmitter, stgy));
    }

    /**
     * @param arr Array.
     * @param position Position.
     * @param limit Limit.
     * @param settings Settings.
     */
    private QuickSortRecursiveTask(FullPageId[] arr, int position, int limit,
        CpSettings settings) {
        this.arr = arr;
        this.position = position;
        this.limit = limit;
        this.settings = settings;
    }

    /**
     * @param cnt size of local sub array.
     * @return {@code true} for small chunks to be sorted under 1 thread.
     */
    private static boolean isUnderThreshold(int cnt) {
        return cnt < ONE_CHUNK_THRESHOLD;
    }

    /** {@inheritDoc} */
    @Override public Void call() throws Exception {
        final int remaining = limit - position;

        if (remaining == 0)
            return null;

        if (isUnderThreshold(remaining)) {
            Arrays.sort(arr, position, limit, settings.comp);

            FullPageId[] arrCp = Arrays.copyOfRange(arr, position, limit);

            int subArrays = (remaining / ONE_WRITE_CHUNK_THRESHOLD) + 1;

            Collection<FullPageId[]> split = CheckpointScope.split(arrCp, subArrays);

            for (FullPageId[] nextSubArray : split) {
                final Callable<Void> task = settings.taskFactory.apply(nextSubArray);

                if (subArrays > 1 && settings.stgy.forkNow())
                    settings.forkSubmitter.apply(task);
                else
                    task.call();
            }
        }
        else {
            int centerIdx = partition(arr, position, limit, settings.comp);

            Callable<Void> t1 = new QuickSortRecursiveTask(arr, position, centerIdx, settings);
            Callable<Void> t2 = new QuickSortRecursiveTask(arr, centerIdx, limit, settings);

            if (settings.stgy.forkNow()) {
                settings.forkSubmitter.apply(t2); //not all threads working or half of threads are already write
                t1.call();
            }
            else {
                t1.call(); //to low number of writers, try to get to bottom and start asap
                settings.forkSubmitter.apply(t2);
            }
        }
        return null;
    }

    /**
     * @param arr Array.
     * @param position Start position inclusive.
     * @param limit End position exclusive.
     * @param comp Comparator.
     * @return Position of array split
     */
    public static int partition(FullPageId[] arr, int position, int limit,
        Comparator<FullPageId> comp) {
        int left = position;
        int right = limit - 1;
        final int randomIdx = (limit - position) / 2 + position;
        FullPageId refElement = arr[randomIdx]; // taking middle element as reference

        while (left <= right) {
            //searching number which is greater than reference
            while (comp.compare(arr[left], refElement) < 0)
                left++;
            //searching number which is less than reference
            while (comp.compare(arr[right], refElement) > 0)
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

    /**
     * @param arr Array.
     * @param i First element.
     * @param j Second element.
     */
    private static void swap(FullPageId[] arr, int i, int j) {
        FullPageId tmp = arr[i];

        arr[i] = arr[j];
        arr[j] = tmp;
    }

    /**
     * Shared settings for current task.
     */
    private static class CpSettings {
        /** Task factory. */
        private final IgniteClosure<FullPageId[], Callable<Void>> taskFactory;

        /** Forked task submitter. */
        private final IgniteInClosure<Callable<Void>> forkSubmitter;

        /** Comparator. */
        private final Comparator<FullPageId> comp;

        /** Strategy of forking. */
        private final ForkNowForkLaterStrategy stgy;

        /**
         * @param comp Comparator.
         * @param taskFactory Task factory.
         * @param forkSubmitter Fork submitter.
         * @param stgy Strategy.
         */
        CpSettings(Comparator<FullPageId> comp,
            IgniteClosure<FullPageId[], Callable<Void>> taskFactory,
            IgniteInClosure<Callable<Void>> forkSubmitter,
            ForkNowForkLaterStrategy stgy) {
            this.comp = comp;
            this.taskFactory = taskFactory;
            this.forkSubmitter = forkSubmitter;
            this.stgy = stgy;
        }
    }
}
