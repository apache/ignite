/*
 * Copyright 2019 Yang Wang
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.stuart.ext.collections;

import java.util.Collection;
import java.util.concurrent.ConcurrentLinkedQueue;

public class BoundedConcurrentLinkedQueue<E> extends ConcurrentLinkedQueue<E> {

    private static final long serialVersionUID = -7195600488213610106L;

    private final int capacity;

    public BoundedConcurrentLinkedQueue(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public boolean add(E element) {
        if (capacity > 0 && size() >= capacity) {
            // poll the first element
            super.poll();
        }

        // add to the last
        return super.add(element);
    }

    @Override
    public boolean addAll(Collection<? extends E> elements) {
        boolean result = false;

        for (E element : elements) {
            result = add(element);
        }

        return result;
    }

    @Override
    public boolean offer(E element) {
        if (capacity > 0 && size() >= capacity) {
            // poll the first element
            super.poll();
        }

        // add to the last
        return super.offer(element);
    }

    @Override
    public E poll() {
        return super.poll();
    }

    @Override
    public E peek() {
        return super.peek();
    }

    @Override
    public boolean isEmpty() {
        return super.isEmpty();
    }

    @Override
    public int size() {
        return super.size();
    }

    public int addExt(E element) {
        // poll count
        int count = 0;

        if (capacity > 0 && size() >= capacity) {
            // poll the first element
            super.poll();
            // poll count + 1
            count = 1;
        }

        // add to the last
        if (super.add(element)) {
            // return poll count
            return count;
        } else {
            // return failed
            return -1;
        }
    }

    public int addAllExt(Collection<? extends E> elements) {
        int count = 0;

        for (E element : elements) {
            int result = addExt(element);

            if (result != -1) {
                count += result;
            }
        }

        return count;
    }

    public int offerExt(E element) {
        // poll count
        int count = 0;

        if (capacity > 0 && size() >= capacity) {
            // poll the first element
            super.poll();
            // poll count + 1
            count = 1;
        }

        // add to the last
        if (super.offer(element)) {
            // return poll count
            return count;
        } else {
            // return failed
            return -1;
        }
    }

}
