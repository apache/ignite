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

package org.apache.ignite.internal.util.lang;

import java.util.Iterator;
import org.apache.ignite.lang.IgniteBiClosure;
import org.apache.ignite.lang.IgniteClosure;

/**
 * Parent-child iterator. Lazy 2 levels iterator, which iterates over child items for each parent item.
 *
 * @param <P> Parent class.
 * @param <C> Child class.
 * @param <R> Result item class.
 */
public class IgniteParentChildIterator<P, C, R> implements Iterator<R> {
    /** Parent iterator. */
    private final Iterator<P> parentIter;

    /** Child iterator closure. This closure helps to get child iterator for each parent item. */
    private final IgniteClosure<P, Iterator<C>> cloChildIter;

    /**
     * Result from parent and child closure. This closure helps to produce resulting item for parent and child items.
     */
    private final IgniteBiClosure<P, C, R> cloResFromParentChild;

    /** Child iterator. */
    private Iterator<C> childIter;

    /** Next parent. */
    private P nextParent;

    /** Next child. */
    private C nextChild;

    /**
     * @param parentIter Parent iterator.
     * @param cloChildIter Child iterator closure.
     * @param cloResFromParentChild Result from parent and child closure.
     */
    public IgniteParentChildIterator(Iterator<P> parentIter,
        IgniteClosure<P, Iterator<C>> cloChildIter,
        IgniteBiClosure<P, C, R> cloResFromParentChild) {
        this.parentIter = parentIter;
        this.cloChildIter = cloChildIter;
        this.cloResFromParentChild = cloResFromParentChild;

        moveChild();
    }

    /**
     * Move to next parent.
     */
    private void moveParent() {
        nextParent = parentIter.next();

        childIter = cloChildIter.apply(nextParent);
    }

    /**
     * Move to next child.
     */
    private void moveChild() {
        // First iteration.
        if (nextParent == null && parentIter.hasNext())
            moveParent();

        // Empty parent at first iteration.
        if (childIter == null)
            return;

        boolean hasNextChild;

        while ((hasNextChild = childIter.hasNext()) || parentIter.hasNext()) {
            if (hasNextChild) {
                nextChild = childIter.next();

                return;
            }
            else
                moveParent();
        }

        nextChild = null;
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        return nextChild != null;
    }

    /** {@inheritDoc} */
    @Override public R next() {
        if (nextChild == null)
            return null;

        R res = cloResFromParentChild.apply(nextParent, nextChild);

        moveChild();

        return res;
    }
}
