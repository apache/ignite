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

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
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

    /** One write chunk threshold. Determines if it reasonable to submit one page saving task. */
    private static final int ONE_WRITE_CHUNK_THRESHOLD = 1024;

    /**
     * Source array to sort, split and write. Shared between threads.
     * May be {@code null} for initial task, {@link #setCollector} is provided instead.
     */
    private final FullPageIdsBuffer buf;

    /** This task global settings. */
    private final CpSettings settings;

    /** Set collector. Should be provided if {@link #buf} is null */
    private MultiSetForSameStripeCollector setCollector;

    /**
     * @param setCollector Multiset collector from one bucket of one region.
     * @param comp Comparator.
     * @param taskFactory Task factory.
     * @param forkSubmitter Fork submitter.
     * @param stgy Strategy.
     */
    QuickSortRecursiveTask(MultiSetForSameStripeCollector setCollector,
        Comparator<FullPageId> comp,
        IgniteClosure<FullPageIdsBuffer, Callable<Void>> taskFactory,
        IgniteInClosure<Callable<Void>> forkSubmitter,
        ForkNowForkLaterStrategy stgy) {
        this(null, new CpSettings(comp, taskFactory, forkSubmitter, stgy));
        this.setCollector = setCollector;
    }

    /**
     * @param buf Buffer, source array to sort.
     * @param settings Settings.
     */
    private QuickSortRecursiveTask(FullPageIdsBuffer buf, CpSettings settings) {
        this.buf = buf;
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
        assert buf != null || setCollector != null;

        if (setCollector != null) {
            Iterable<? extends Collection<FullPageId>> sets = setCollector.unmergedSets();

            if (setCollector.isSorted(settings.comp)) {
                FullPageIdsBuffer.createBufferFromSortedCollections(sets, settings.comp, ONE_WRITE_CHUNK_THRESHOLD,
                    (mayHaveNext, buffer) -> {
                        try {
                            validateBufferOrder(buffer);

                            writeOneBuffer(buffer, mayHaveNext);
                        }
                        catch (Exception e) {
                            throw new RuntimeException(e);
                        }
                    });
            }
            else
                handleBuffer(FullPageIdsBuffer.createBufferFromMultiCollection(sets));

            return null;
        }

        handleBuffer(buf);

        return null;
    }

    /**
     * @param buf buffer to process: sort and write, or separare and fork
     * @throws Exception if failed.
     */
    private void handleBuffer(FullPageIdsBuffer buf) throws Exception {
        final int remaining = buf.remaining();

        if (remaining == 0)
            return;

        if (isUnderThreshold(remaining)) {
            buf.sort(settings.comp);

            int subArrays = (int)Math.round(Math.ceil(1.0 * remaining / ONE_WRITE_CHUNK_THRESHOLD));

            for (Iterator<FullPageIdsBuffer> iter = buf.split(subArrays).iterator(); iter.hasNext(); )
                writeOneBuffer(iter.next(), iter.hasNext());
        }
        else {
            final FullPageId[] arr = buf.internalArray();
            final int position = buf.position();
            final int limit = buf.limit();
            int centerIdx = partition(arr, position, limit, settings.comp);

            Callable<Void> t1 = new QuickSortRecursiveTask(buf.bufferOfRange(position, centerIdx), settings);
            Callable<Void> t2 = new QuickSortRecursiveTask(buf.bufferOfRange(centerIdx, limit), settings);

            if (settings.stgy.forkNow()) {
                settings.forkSubmitter.apply(t2); //not all threads working
                t1.call();
            }
            else {
                t1.call(); //to low number of writers, try to get to bottom and start asap
                settings.forkSubmitter.apply(t2);
            }
        }
    }

    /**
     * @param buf buffer to write.
     * @param forkAllowed for is reasonable, usually not reasonable for last chunk.
     * @throws Exception if failed.
     */
    private void writeOneBuffer(FullPageIdsBuffer buf, boolean forkAllowed) throws Exception {
        final Callable<Void> task = settings.taskFactory.apply(buf);

        if (forkAllowed && settings.stgy.forkNow())
            settings.forkSubmitter.apply(task);
        else
            task.call();
    }

    /**
     * @param buf buffer to check order of elements
     */
    private void validateBufferOrder(FullPageIdsBuffer buf) {
        FullPageId prevId = null;
        for (int i = buf.position(); i < buf.limit(); i++) {
            FullPageId id = buf.internalArray()[i];

            if (!(prevId == null || settings.comp.compare(id, prevId) >= 0))
                throw new AssertionError("Invalid order of element in presorted set");

            prevId = id;
        }
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
        private final IgniteClosure<FullPageIdsBuffer, Callable<Void>> taskFactory;

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
            IgniteClosure<FullPageIdsBuffer, Callable<Void>> taskFactory,
            IgniteInClosure<Callable<Void>> forkSubmitter,
            ForkNowForkLaterStrategy stgy) {
            this.comp = comp;
            this.taskFactory = taskFactory;
            this.forkSubmitter = forkSubmitter;
            this.stgy = stgy;
        }
    }
}
