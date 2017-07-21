/*
 * Copyright (c) 2009 Stanford University, unless otherwise specified.
 * All rights reserved.
 *
 * This software was developed by the Pervasive Parallelism Laboratory of
 * Stanford University, California, USA.
 *
 * Permission to use, copy, modify, and distribute this software in source
 * or binary form for any purpose with or without fee is hereby granted,
 * provided that the following conditions are met:
 *
 *    1. Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *
 *    2. Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *
 *    3. Neither the name of Stanford University nor the names of its
 *       contributors may be used to endorse or promote products derived
 *       from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE REGENTS AND CONTRIBUTORS ``AS IS'' AND
 * ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE REGENTS OR CONTRIBUTORS BE LIABLE
 * FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
 * SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
 * CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY
 * OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
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