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

package org.apache.ignite;

import java.io.Closeable;
import java.util.concurrent.TimeUnit;

/**
 * This interface provides a rich API for working with distributed count down latch.
 * <p>
 * <h1 class="header">Functionality</h1>
 * Distributed count down latch provides functionality similar to {@code java.util.CountDownLatch}.
 * Note that you cannot remove count down latch having count greater that zero. It should be
 * counted down to zero first.
 * <h1 class="header">Creating Distributed Count Down Latch</h1>
 * Instance of cache count down latch can be created by calling the following method:
 * {@link Ignite#countDownLatch(String, int, boolean, boolean)}.
 */
public interface IgniteCountDownLatch extends Closeable {
    /**
     * Gets name of the latch.
     *
     * @return Name of the latch.
     */
    public String name();

    /**
     * Gets current count value of the latch.
     *
     * @return Current count.
     */
    public int count();

    /**
     * Gets initial count value of the latch.
     *
     * @return Initial count.
     */
    public int initialCount();

    /**
     * Gets {@code autoDelete} flag. If this flag is {@code true} latch is removed
     * from cache when it has been counted down to 0.
     *
     * @return Value of {@code autoDelete} flag.
     */
    public boolean autoDelete();

    /**
     * Causes the current thread to wait until the latch has counted down to
     * zero, unless current thread is interrupted.
     * <p>
     * If the current count of the latch is zero then this method returns immediately.
     * <p>
     * If the current count is greater than zero then the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of two things happen:
     * <ul>
     *     <li>The count reaches zero due to invocations of the
     *      {@link #countDown} method on any node; or
     *      <li>Some other thread interrupts the current thread.
     * </ul>
     * <p>
     * If the current thread:
     * <ul>
     *      <li>has its interrupted status set on entry to this method; or
     *      <li>is interrupted while waiting,
     * </ul>
     * then {@link org.apache.ignite.internal.IgniteInterruptedCheckedException} is thrown and the current thread's
     * interrupted status is cleared.
     *
     * @throws IgniteException If operation failed.
     * @throws org.apache.ignite.IgniteInterruptedException if the current thread is interrupted
     *      while waiting
     */
    public void await() throws IgniteException;

    /**
     * Causes the current thread to wait until the latch has counted down to
     * zero, unless the thread is interrupted, or the specified waiting time elapses.
     * <p>
     * If the current count is zero then this method returns immediately
     * with the value {@code true}.
     * <p>
     * If the current count is greater than zero then the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of three things happen:
     * <ul>
     *      <li>The count reaches zero due to invocations of the
     *      {@link #countDown} method on any node; or
     *      <li>Some other thread interrupts the current thread; or
     *      <li>The specified waiting time elapses.
     * </ul>
     * <p>
     * If the count reaches zero then the method returns with the
     * value {@code true}.
     * <p>
     * If the current thread:
     * <ul>
     *      <li>has its interrupted status set on entry to this method; or
     *      <li>is interrupted while waiting,
     * </ul>
     * then {@link org.apache.ignite.internal.IgniteInterruptedCheckedException} is thrown and the current thread's
     * interrupted status is cleared.
     * <p>
     * If the specified waiting time elapses then the value {@code false}
     * is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.
     *
     * @param timeout The maximum time to wait in milliseconds.
     * @return {@code True} if the count reached zero and {@code false}
     *      if the waiting time elapsed before the count reached zero.
     * @throws org.apache.ignite.IgniteInterruptedException If the current thread is interrupted
     *      while waiting.
     * @throws IgniteException If operation failed.
     */
    public boolean await(long timeout) throws IgniteException;

    /**
     * Causes the current thread to wait until the latch has counted down to
     * zero, unless the thread is interrupted, or the specified waiting time elapses.
     * <p>
     * If the current count is zero then this method returns immediately
     * with the value {@code true}.
     * <p>
     * If the current count is greater than zero then the current
     * thread becomes disabled for thread scheduling purposes and lies
     * dormant until one of three things happen:
     * <ul>
     *      <li>The count reaches zero due to invocations of the
     *      {@link #countDown} method on any node; or
     *      <li>Some other thread interrupts the current thread; or
     *      <li>The specified waiting time elapses.
     * </ul>
     * <p>
     * If the count reaches zero then the method returns with the
     * value {@code true}.
     * <p>
     * If the current thread:
     * <ul>
     *      <li>has its interrupted status set on entry to this method; or
     *      <li>is interrupted while waiting,
     * </ul>
     * then {@link org.apache.ignite.internal.IgniteInterruptedCheckedException} is thrown and the current thread's
     * interrupted status is cleared.
     * <p>
     * If the specified waiting time elapses then the value {@code false}
     * is returned.  If the time is less than or equal to zero, the method
     * will not wait at all.
     *
     *
     * @param timeout The maximum time to wait.
     * @param unit The time unit of the {@code timeout} argument.
     * @return {@code True} if the count reached zero and {@code false}
     *      if the waiting time elapsed before the count reached zero.
     * @throws org.apache.ignite.IgniteInterruptedException If the current thread is interrupted
     *      while waiting.
     * @throws IgniteException If operation failed.
     */
    public boolean await(long timeout, TimeUnit unit) throws IgniteException;

    /**
     * Decrements the count of the latch, releasing all waiting threads
     * on all nodes if the count reaches zero.
     * <p>
     * If the current count is greater than zero then it is decremented.
     * If the new count is zero then all waiting threads are re-enabled for
     * thread scheduling purposes.
     * <p>
     * If the current count equals zero then nothing happens.
     *
     * @return Count after decrement.
     * @throws IgniteException If operation failed.
     */
    public int countDown() throws IgniteException;

    /**
     * Decreases the count of the latch using passed in value,
     * releasing all waiting threads on all nodes if the count reaches zero.
     * <p>
     * If the current count is greater than zero then it is decreased.
     * If the new count is zero then all waiting threads are re-enabled for
     * thread scheduling purposes.
     * <p>
     * If the current count equals zero then nothing happens.
     *
     * @param val Value to decrease counter on.
     * @return Count after decreasing.
     * @throws IgniteException If operation failed.
     */
    public int countDown(int val) throws IgniteException;

    /**
     * Counts down this latch to zero, releasing all waiting threads on all nodes.
     * <p>
     * If the current count equals zero then nothing happens.
     *
     * @throws IgniteException If operation failed.
     */
    public void countDownAll() throws IgniteException;

    /**
     * Gets {@code removed} status of the latch.
     *
     * @return {@code True} if latch was removed from cache, {@code false} otherwise.
     */
    public boolean removed();

    /**
     * Removes this count down latch.
     *
     * @throws IgniteException If operation failed.
     */
    @Override public void close();
}