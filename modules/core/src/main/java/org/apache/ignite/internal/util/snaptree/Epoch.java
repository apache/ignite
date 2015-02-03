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

package org.apache.ignite.internal.util.snaptree;

/** A <code>Epoch</code> has a lifecycle consisting of three phases: active,
 *  closing, and closed.  During the active phase partipants may arrive and
 *  leave the epoch.  Once a close has been requested, new participants are not
 *  allowed, only leaving is possible.  Once close has been requested and all
 *  participants have left, the epoch is transitioned to the closed state.
 *  <p>
 *  Entry is performed with {@link #attemptArrive}, which returns a non-null
 *  ticket on success or null if {@link #beginClose} has already been called.
 *  Each successful call to <code>attemptArrive</code> must be paired by a call
 *  to {@link Ticket#leave} on the returned ticket.
 *  <p>
 *  The abstract method {@link #onClosed} will be invoked exactly once after
 *  the epoch becomes closed.  It will be passed the sum of the values passed
 *  to {@link Ticket#leave}.  There is no way to query the current participant
 *  count or state of the epoch without changing it.
 *  <p>
 *  Internally the epoch responds to contention by increasing its size,
 *  striping the participant count across multiple objects (and hopefully
 *  multiple cache lines).  Once close has begun, the epoch converts itself to
 *  a single-shot hierarchical barrier, that also performs a hierarchical
 *  reduction of the leave parameters.
 */
@SuppressWarnings("ALL")
abstract public class Epoch {

    /** Represents a single successful arrival to an {@link Epoch}. */
    public interface Ticket {
        /** Informs the epoch that returned this ticket that the participant
         *  has left.  This method should be called exactly once per ticket.
         *  The sum of the <code>data</code> values for all tickets will be
         *  computed and passed to {@link Epoch#onClosed}.
         */
        void leave(int data);
    }

    private final Root _root = new Root();

    /** Returns a {@link Ticket} indicating a successful arrival, if no call to
     *  {@link #beginClose} has been made for this epoch, or returns null if
     *  close has already begun.  {@link Ticket#leave} must be called exactly
     *  once on any returned ticket.
     */
    public Ticket attemptArrive() {
        return _root.attemptArrive();
    }

    /** Prevents new arrivals from succeeding, then returns immediately.
     *  {@link #onClosed} will be called after all outstanding tickets have
     *  been returned.  To block until close is complete, add some sort of
     *  synchronization logic to the user-defined implementation of {@link
     *  #onClosed}.
     */
    public void beginClose() {
        _root.beginClose();
    }

    /** Override this method to provide user-defined behavior.
     *  <code>dataSum</code> will be the sum of the <code>data</code> values
     *  passed to {@link Ticket#leave} for all tickets in this epoch.
     *  <p>
     *  As a simple example, a blocking close operation may be defined by:<pre>
     *    class BlockingEpoch extends Epoch {
     *        private final CountDownLatch _closed = new CountDownLatch(1);
     *
     *        public void blockingClose() throws InterruptedException {
     *            beginClose();
     *            _closed.await();
     *        }
     *
     *        protected void onClosed(int dataSum) {
     *            _closed.countDown(1);
     *        }
     *    }
     *  </pre>
     */
    abstract protected void onClosed(int dataSum);

    //////////////// debugging stuff

    int computeSpread() {
        return _root.computeSpread();
    }

    //////////////// internal implementation

    private class Root extends EpochNode {
        /** */
        private static final long serialVersionUID = 0L;

        protected void onClosed(final int dataSum) {
            Epoch.this.onClosed(dataSum);
        }
    }
}
