/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.util;

import java.util.Arrays;
import java.util.Collection;
import java.util.function.Predicate;

/**
 * A list implementation based on segments.Only supports removing elements from start or end. The list keep the elements
 * in a segment list, every segment contains at most 128 elements.
 *
 * [segment, segment, segment ...] /                 |                    \ segment             segment
 * segment [0, 1 ...  127]    [128, 129 ... 255]    [256, 1 ... 383]
 */
public class SegmentList<T> {
    private static final int SEGMENT_SHIFT = 7;
    public static final int SEGMENT_SIZE = 2 << (SEGMENT_SHIFT - 1);

    private final ArrayDeque<Segment<T>> segments;

    private int size;

    // Cached offset in first segment.
    private int firstOffset;

    private final boolean recycleSegment;

    /**
     * Create a new SegmentList
     *
     * @param recycleSegment true to enable recycling segment, only effective in same thread.
     */
    public SegmentList(final boolean recycleSegment) {
        this.segments = new ArrayDeque<>();
        this.size = 0;
        this.firstOffset = 0;
        this.recycleSegment = recycleSegment;
    }

    /**
     * A recyclable segment.
     *
     * @param <T>
     */
    private final static class Segment<T> implements Recyclable {
        private static final Recyclers<Segment<?>> recyclers = new Recyclers<Segment<?>>(16_382 / SEGMENT_SIZE) {

            @Override
            protected SegmentList.Segment<?> newObject(final Handle handle) {
                return new SegmentList.Segment<>(handle);
            }
        };

        public static Segment<?> newInstance(final boolean recycleSegment) {
            if (recycleSegment) {
                return recyclers.get();
            }
            else {
                return new Segment<>();
            }
        }

        private transient Recyclers.Handle handle;

        final T[] elements;
        int pos;     // end offset(exclusive)
        int offset;  // start offset(inclusive)

        Segment() {
            this(Recyclers.NOOP_HANDLE);
        }

        Segment(final Recyclers.Handle handle) {
            this.elements = (T[]) new Object[SEGMENT_SIZE];
            this.pos = this.offset = 0;
            this.handle = handle;
        }

        void clear() {
            this.pos = this.offset = 0;
            Arrays.fill(this.elements, null);
        }

        @Override
        public boolean recycle() {
            clear();
            return recyclers.recycle(this, this.handle);
        }

        int cap() {
            return SEGMENT_SIZE - this.pos;
        }

        @SuppressWarnings("SuspiciousSystemArraycopy")
        private void addAll(final Object[] src, final int srcPos, final int len) {
            System.arraycopy(src, srcPos, this.elements, this.pos, len);
            this.pos += len;
        }

        boolean isReachEnd() {
            return this.pos == SEGMENT_SIZE;
        }

        boolean isEmpty() {
            return this.size() == 0;
        }

        void add(final T e) {
            this.elements[this.pos++] = e;
        }

        T get(final int index) {
            if (index >= this.pos || index < this.offset) {
                throw new IndexOutOfBoundsException("Index=" + index + ", Offset=" + this.offset + ", Pos=" + this.pos);
            }
            return this.elements[index];
        }

        T peekLast() {
            return this.elements[this.pos - 1];
        }

        int size() {
            return this.pos - this.offset;
        }

        T peekFirst() {
            return this.elements[this.offset];
        }

        int removeFromLastWhen(final Predicate<T> predicate) {
            int removed = 0;
            for (int i = this.pos - 1; i >= this.offset; i--) {
                T e = this.elements[i];
                if (predicate.test(e)) {
                    this.elements[i] = null;
                    removed++;
                }
                else {
                    break;
                }
            }
            this.pos -= removed;
            return removed;
        }

        int removeFromFirstWhen(final Predicate<T> predicate) {
            int removed = 0;
            for (int i = this.offset; i < this.pos; i++) {
                T e = this.elements[i];
                if (predicate.test(e)) {
                    this.elements[i] = null;
                    removed++;
                }
                else {
                    break;
                }
            }
            this.offset += removed;
            return removed;
        }

        int removeFromFirst(final int toIndex) {
            int removed = 0;
            for (int i = this.offset; i < Math.min(toIndex, this.pos); i++) {
                this.elements[i] = null;
                removed++;
            }
            this.offset += removed;
            return removed;
        }

        @Override
        public String toString() {
            StringBuilder b = new StringBuilder();
            for (int i = this.offset; i < this.pos; i++) {
                b.append(this.elements[i]);
                if (i != this.pos - 1) {
                    b.append(", ");
                }
            }
            return "Segment [elements=" + b.toString() + ", offset=" + this.offset + ", pos=" + this.pos + "]";
        }

    }

    public T get(int index) {
        index += this.firstOffset;
        return this.segments.get(index >> SEGMENT_SHIFT).get(index & (SEGMENT_SIZE - 1));
    }

    public T peekLast() {
        Segment<T> lastSeg = getLast();
        return lastSeg == null ? null : lastSeg.peekLast();
    }

    public T peekFirst() {
        Segment<T> firstSeg = getFirst();
        return firstSeg == null ? null : firstSeg.peekFirst();
    }

    private Segment<T> getFirst() {
        if (!this.segments.isEmpty()) {
            return this.segments.peekFirst();
        }
        return null;
    }

    public void add(final T e) {
        Segment<T> lastSeg = getLast();
        if (lastSeg == null || lastSeg.isReachEnd()) {
            lastSeg = (Segment<T>) Segment.newInstance(this.recycleSegment);
            this.segments.add(lastSeg);
        }
        lastSeg.add(e);
        this.size++;
    }

    private Segment<T> getLast() {
        if (!this.segments.isEmpty()) {
            return this.segments.peekLast();
        }
        return null;
    }

    public int size() {
        return this.size;
    }

    public int segmentSize() {
        return this.segments.size();
    }

    public boolean isEmpty() {
        return this.size == 0;
    }

    /**
     * Remove elements from first until predicate returns false.
     *
     * @param predicate predicate functional interface
     */
    public void removeFromFirstWhen(final Predicate<T> predicate) {
        Segment<T> firstSeg = getFirst();
        while (true) {
            if (firstSeg == null) {
                this.firstOffset = this.size = 0;
                return;
            }
            int removed = firstSeg.removeFromFirstWhen(predicate);
            if (removed == 0) {
                break;
            }
            this.size -= removed;
            this.firstOffset = firstSeg.offset;
            if (firstSeg.isEmpty()) {
                RecycleUtil.recycle(this.segments.pollFirst());
                firstSeg = getFirst();
                this.firstOffset = 0;
            }
        }
    }

    public void clear() {
        while (!this.segments.isEmpty()) {
            RecycleUtil.recycle(this.segments.pollLast());
        }
        this.size = this.firstOffset = 0;
    }

    /**
     * Remove elements from last until predicate returns false.
     *
     * @param predicate predicate functional interface
     */
    public void removeFromLastWhen(final Predicate<T> predicate) {
        Segment<T> lastSeg = getLast();
        while (true) {
            if (lastSeg == null) {
                this.firstOffset = this.size = 0;
                return;
            }
            int removed = lastSeg.removeFromLastWhen(predicate);
            if (removed == 0) {
                break;
            }
            this.size -= removed;
            if (lastSeg.isEmpty()) {
                RecycleUtil.recycle(this.segments.pollLast());
                lastSeg = getLast();
            }
        }
    }

    /**
     * Removes from this list all of the elements whose index is between 0, inclusive, and {@code toIndex}, exclusive.
     * Shifts any succeeding elements to the left (reduces their index).
     */
    public void removeFromFirst(final int toIndex) {
        int alignedIndex = toIndex + this.firstOffset;
        int toSegmentIndex = alignedIndex >> SEGMENT_SHIFT;

        int toIndexInSeg = alignedIndex & (SEGMENT_SIZE - 1);

        if (toSegmentIndex > 0) {
            this.segments.removeRange(0, toSegmentIndex);
            this.size -= ((toSegmentIndex << SEGMENT_SHIFT) - this.firstOffset);
        }

        Segment<T> firstSeg = this.getFirst();
        if (firstSeg != null) {
            this.size -= firstSeg.removeFromFirst(toIndexInSeg);
            this.firstOffset = firstSeg.offset;
            if (firstSeg.isEmpty()) {
                RecycleUtil.recycle(this.segments.pollFirst());
                this.firstOffset = 0;
            }
        }
        else {
            this.firstOffset = this.size = 0;
        }
    }

    public void addAll(final Collection<T> coll) {
        Object[] src = coll2Array(coll);

        int srcPos = 0;
        int srcSize = coll.size();

        Segment<T> lastSeg = getLast();
        while (srcPos < srcSize) {
            if (lastSeg == null || lastSeg.isReachEnd()) {
                lastSeg = (Segment<T>) Segment.newInstance(this.recycleSegment);
                this.segments.add(lastSeg);
            }

            int len = Math.min(lastSeg.cap(), srcSize - srcPos);
            lastSeg.addAll(src, srcPos, len);
            srcPos += len;
            this.size += len;
        }
    }

    private Object[] coll2Array(final Collection<T> coll) {
        Object[] src = coll.toArray();

        return src;
    }

    @Override
    public String toString() {
        return "SegmentList [segments=" + this.segments + ", size=" + this.size + ", firstOffset=" + this.firstOffset
            + "]";
    }

}
