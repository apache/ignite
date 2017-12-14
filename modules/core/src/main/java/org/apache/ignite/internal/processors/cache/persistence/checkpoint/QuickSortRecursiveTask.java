package org.apache.ignite.internal.processors.cache.persistence.checkpoint;

import java.util.Arrays;
import java.util.Comparator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.RecursiveTask;
import org.apache.ignite.internal.pagemem.FullPageId;
import org.jetbrains.annotations.Nullable;

public class QuickSortRecursiveTask extends RecursiveTask<Integer> {
    /** Source array to sort. */
    private final FullPageId[] array;
    /** Start position. Index of first element inclusive. */
    private final int position;
    /** Limit. Index of last element exclusive. */
    private final int limit;

    private boolean rootTask;
    private Comparator<FullPageId> comp;
    @Nullable private final BlockingQueue<FullPageId[]> queue;

    public QuickSortRecursiveTask(FullPageId[] arr,
        Comparator<FullPageId> comp,
        @Nullable BlockingQueue<FullPageId[]> queue) {
        this(arr, 0, arr.length, comp, queue);
        this.rootTask = true;
    }

    private QuickSortRecursiveTask(FullPageId[] arr, int position, int limit,
        Comparator<FullPageId> comp,
        @Nullable BlockingQueue<FullPageId[]> queue) {
        this.array = arr;
        this.position = position;
        this.limit = limit;
        this.comp = comp;
        this.queue = queue;
    }

    @Override protected Integer compute() {
        final int remaining = limit - position;
        int chunks;
        if (isUnderThreshold(remaining)) {
            Arrays.sort(array, position, limit, comp);
            if (false)
                System.err.println("Sorted [" + remaining + "] in " + Thread.currentThread().getName());

            if (queue != null) {
                final FullPageId[] ids = Arrays.copyOfRange(array, position, limit);
                try {
                    queue.put(ids);
                }
                catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            chunks =  1; // one chunk was produced
        }
        else {
            int centerIndex = partition2(array, position, limit, comp);
            if (false)
                System.err.println("centerIndex=" + centerIndex);
            QuickSortRecursiveTask t1 = new QuickSortRecursiveTask(array, position, centerIndex, comp, queue);
            QuickSortRecursiveTask t2 = new QuickSortRecursiveTask(array, centerIndex, limit, comp, queue);
            t1.fork();
            final Integer t2Result = t2.compute();
            final Integer t1Result = t1.join();
            chunks = t1Result + t2Result;
        }

        if(rootTask && queue!=null)
            queue.offer(AsyncCheckpointer.POISON_PILL);

        return chunks;
    }

    public static boolean isUnderThreshold(int cnt) {
        return cnt < 1024 * 16;
    }

    int partition(FullPageId[] arr, int position, int limit,
        Comparator<FullPageId> comp) {
        if (false)
            System.err.println("Partition from " + position + " to " + limit); //todo remove
        int i = position;
        FullPageId x = arr[limit - 1];
        for (int j = position; j < limit - 1; j++) {
            if (comp.compare(arr[j], x) < 0) {
                swap(arr, i, j);
                i++;
            }
        }
        swap(arr, i, limit - 1);
        return i;
    }

    static int partition2(FullPageId[] arr, int position, int limit,
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
                FullPageId tmp = arr[left];
                arr[left] = arr[right];
                arr[right] = tmp;

                //increment left index and decrement right index
                left++;
                right--;
            }
        }
        return left;
    }

    void swap(FullPageId[] arr, int i, int j) {
        FullPageId tmp = arr[i];
        arr[i] = arr[j];
        arr[j] = tmp;
    }

}
