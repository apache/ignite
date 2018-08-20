package org.apache.ignite.internal.util;

import lib.llpl.Heap;
import lib.llpl.MemoryBlock;
import lib.llpl.Transaction;
import org.apache.ignite.Ignition;

public class PersistentLinkedListOfLong<T extends MemoryBlock.Kind> extends PersistentLinkedList<T, Long> {

    /**
     * Constructs a persistent singly linked list. Construction does not allocate memory; adding nodes does.
     *
     * @param heap the AEP heap this list is stored on.
     */
    public PersistentLinkedListOfLong(Class<T> kind, Heap heap) {
        super(kind, heap);
    }
    /**
     * Appends item to list.
     *
     * @param item is the item to store.
     */
    @Override
    public synchronized void add(Long item) {
        Transaction.run(Ignition.getAepHeap(), () -> {
            PersistentNodeOfLong node = new PersistentNodeOfLong(item);
            long address = node.getBaseAddress();
            if (head == 0) {
                head = address;
                tail = address;
            } else {
                setNextLink(tail, address);
                tail = address;
            }
        });
    }

    /**
     * Adds item at index.
     *
     * @param index the index after which the item is stored.
     * @param item the entry to store.
     */
    @Override
    public synchronized void add(final int index, Long item) {
        if (index < 0 || index >= size()) throw new IndexOutOfBoundsException();

        Transaction.run(Ignition.getAepHeap(), () -> {
            int idx = index;
            PersistentNodeOfLong node = new PersistentNodeOfLong(item);
            long address = node.getBaseAddress();

            if (idx == 0) {
                setNextLink(address, head);
                head = address;
            } else {
                long curr = head;
                while (--idx > 0)
                    curr = getNextLink(curr);

                setNextLink(address, getNextLink(curr));
                setNextLink(curr, address);
            }
        });
    }

    /**
     * Gets the stored entry of a node whose base address is baseAddress.
     *
     * @param baseAddress of a node.
     * @return the stored entry of the node.
     */
    @Override
    public Long getItem(long baseAddress) {
        if (baseAddress == 0)
            return null;

        return getMemoryBlock(baseAddress).getLong(0);
    }

    private class PersistentNodeOfLong {

        private MemoryBlock<T> memoryBlock;

        public PersistentNodeOfLong(Long obj) {
            memoryBlock = heap.allocateMemoryBlock(kind, Long.BYTES + Long.BYTES);
            memoryBlock.setLong(0, obj);
            memoryBlock.setLong(Long.BYTES, 0);
        }

        long getBaseAddress() {
            return this.memoryBlock.address();
        }
    }
}
