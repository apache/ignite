package org.apache.ignite.internal.util;

import lib.llpl.Heap;
import lib.llpl.MemoryBlock;
import lib.llpl.Transaction;
import org.apache.ignite.Ignition;

import java.io.*;
import java.util.Iterator;

public class PersistentLinkedList<T extends MemoryBlock.Kind, E extends Serializable> implements Iterator<E> {

    /** The head of the list. */
    protected long head;

    /** The tail of the list. */
    protected long tail;

    /** The class type of the memory region */
    protected Class<T> kind;

    /** The AEP heap the list is on */
    protected Heap heap;

    /** The current iterator index. */
    protected int index = 0;

    /**
     * Constructs a persistent singly linked list. Construction does not allocate memory; adding nodes does.
     *
     * @param heap the AEP heap this list is stored on.
     */
    public PersistentLinkedList(Class<T> kind, Heap heap) {
        this.heap = heap;
        this.kind = kind;
    }

    /**
     * Appends item to list.
     *
     * @param item is the item to store.
     */
    public synchronized void add(E item) {
        Transaction.run(Ignition.getAepHeap(), () -> {
            PersistentNode node = new PersistentNode(item);
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
    public synchronized void add(final int index, E item) {
        if (index < 0 || index >= size()) throw new IndexOutOfBoundsException();

        Transaction.run(Ignition.getAepHeap(), () -> {
            int idx = index;
            PersistentNode node = new PersistentNode(item);
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
     * Gets the entry stored at index.
     *
     * @param index the index.
     * @return the entry at the index.
     */
    public E get(int index) {
        if (index < 0 || index >= size()) throw new IndexOutOfBoundsException();

        long address = head;
        while (index-- > 0)
            address = getNextLink(address);

        return getItem(address);
    }

    /**
     * Gets the stored entry of a node whose base address is baseAddress.
     *
     * @param baseAddress of a node.
     * @return the stored entry of the node.
     */
    public E getItem(long baseAddress) {
        if (baseAddress == 0)
            return null;

        MemoryBlock<T> mr = getMemoryBlock(baseAddress);

        int size = (int) mr.size();
        byte[] ba = new byte[size];
        for (int i = 0; i < size; i++)
            ba[i] = mr.getByte(i);

        return (E) SerializationUtils.deserialize(ba);
    }

    /**
     * Checks to see if the item exists in the list.
     *
     * @param item
     * @return true if the item is in the list, false otherwise.
     */
    public boolean contains(Object item) {
        long curr = head;

        while (curr != 0 && getItem(curr).hashCode() != item.hashCode())
            curr = getNextLink(curr);

        if (curr == 0)
            return false;

        return true;
    }

    /**
     * Removes the entry at index.
     *
     * @param index is the index of the entry to remove.
     */
    public synchronized void removeAtIndex(int index) {
        if (index < 0 || index >= size()) throw new IndexOutOfBoundsException();

        Transaction.run(Ignition.getAepHeap(), () -> {
            int c = index;
            long curr = head;
            while (--c > 0)
                curr = getNextLink(curr);

            long r = getNextLink(curr);
            if (index == 0) {
                r = curr;
                head = getNextLink(r);
            }

            setNextLink(curr, getNextLink(r));
            setNextLink(r, 0);
            heap.freeMemoryBlock(getMemoryBlock(r));

            if (getNextLink(curr) == 0)
                tail = curr;
        });
    }

    /**
     * Removes entry from the list. Comparision is based on object's hashcode.
     *
     * @param item is the entry to remove.
     * @return true if the item has been successfully removed, false otherwise.
     */
    public boolean remove(Object item) {
        long curr = head;

        int i = 0;
        while (curr != 0 && getItem(curr).hashCode() != item.hashCode()) {
            curr = getNextLink(curr);
            i++;
        }

        if (i == size())
            return false;

        removeAtIndex(i);

        return true;
    }

    /**
     * Removes and frees all entries from this list.
     */
    public void removeAll() {
        int s = size();
        while (s-- > 0)
            removeAtIndex(0);
    }

    /**
     * Calculates the number of entries.
     *
     * @return the number of entries in the list.
     */
    public int size() {
        int i = 0;
        long curr = head;
        if (curr == 0)
            return 0;

        while (getNextLink(curr) != 0) {
            curr = getNextLink(curr);
            i++;
        }

        return i + 1;
    }

    /** */
    @Override
    public boolean hasNext() {
        if (index < size())
            return true;
        return false;
    }

    /** */
    @Override
    public E next() {
        if (this.hasNext())
            return get(index++);
        return null;
    }

    /**
     * Gets the root of the memory region of this list.
     *
     * @return the base address of the memory region of the persistent list.
     */
    public long getRoot() {
        return head;
    }

    /**
     * Sets the root of this persistent list.
     *
     * @param head a base address.
     */
    public synchronized void setRoot(long head) {
        // Reset iterator's index
        index = 0;

        this.head = head;
        this.tail = head;

        // Update tail
        long curr = head;
        while (getNextLink(curr) != 0) {
            curr = getNextLink(curr);
            tail = curr;
        }
    }

    /**
     * Sets the next pointer.
     *
     * @param baseAddress the base address of a node.
     * @param next the next address of the node.
     */
    protected void setNextLink(long baseAddress, long next) {
        MemoryBlock<T> mr = getMemoryBlock(baseAddress);
        mr.setLong(mr.size(), next);
    }

    /**
     * Returns the address of the next node.
     *
     * @param baseAddress of a node.
     * @return the next pointer.
     */
    protected long getNextLink(long baseAddress) {
        MemoryBlock<T> mr = getMemoryBlock(baseAddress);
        return mr.getLong(mr.size());
    }


    /**
     * Gets the memory region whose base address equals baseAddress.
     *
     * @param baseAddress is a base address.
     * @return the memory region.
     */
    protected MemoryBlock<T> getMemoryBlock(long baseAddress) {
        return heap.memoryBlockFromAddress(kind, baseAddress);
    }

    /**
     * Prints the content of this list.
     *
     */
    public void print() {
        long curr = head;
        if (curr == 0) {
            System.out.println(size());
            return;
        }

        while (getNextLink(curr) != 0) {
            System.out.print(getItem(curr) + " --> ");
            curr = getNextLink(curr);
        }

        System.out.println(getItem(curr));
    }

    private class PersistentNode {

        private MemoryBlock<T> memoryBlock;

        public PersistentNode(E obj) {
            byte[] objData = SerializationUtils.serialize(obj);
            memoryBlock = heap.allocateMemoryBlock(kind, objData.length + Long.BYTES);
            memoryBlock.copyFromArray(objData, 0, 0, objData.length);
            memoryBlock.setLong(objData.length, 0);
        }

        long getBaseAddress() {
            return this.memoryBlock.address();
        }
    }
}