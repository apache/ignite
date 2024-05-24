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

import java.io.Serializable;
import java.util.Collection;

import org.apache.ignite.IgniteQueue;

public class BoundedIgniteQueue<E> implements Serializable {

    private static final long serialVersionUID = -777307706128837112L;

    private final IgniteQueue<E> queue;

    public BoundedIgniteQueue(IgniteQueue<E> queue) {
        this.queue = queue;
    }

    public boolean add(E element) {
        return offer(element);
    }

    public boolean addAll(Collection<? extends E> elements) {
        boolean result = false;

        for (E element : elements) {
            result = add(element);
        }

        return result;
    }

    public boolean offer(E element) {
        checkNotNull(element);

        while (true) {
            // add new item to the end
            if (queue.offer(element)) {
                return true;
            } else {
                // poll the first item
                queue.poll();
            }
        }
    }

    public E poll() {
        return queue.poll();
    }

    public E peek() {
        return queue.peek();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public int size() {
        return queue.size();
    }

    public int addExt(E element) {
        return offerExt(element);
    }

    public int addAllExt(Collection<? extends E> elements) {
        int count = 0;

        for (E element : elements) {
            count += addExt(element);
        }

        return count;
    }

    public int offerExt(E element) {
        checkNotNull(element);

        // poll count
        int count = 0;

        while (true) {
            // add new item to the end
            if (queue.offer(element)) {
                return count;
            } else {
                // poll the first item
                queue.poll();
                // set result = 1(poll element count)
                count = 1;
            }
        }
    }

    private static void checkNotNull(Object v) {
        if (v == null)
            throw new NullPointerException();
    }

}
