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

package org.apache.ignite.internal.managers.discovery;

import java.io.Serializable;

/**
 * This class represents runtime information available for current VM.
 */
public interface GridLocalMetrics extends Serializable {
    /**
     * Returns the number of processors available to the Java virtual machine.
     * This method is equivalent to the {@link Runtime#availableProcessors()}
     * method.
     * <p> This value may change during a particular invocation of
     * the virtual machine.
     *
     * @return The number of processors available to the virtual
     *      machine; never smaller than one.
     */
    public int getAvailableProcessors();

    /**
     * Returns the system load average for the last minute.
     * The system load average is the sum of the number of runnable entities
     * queued to the {@linkplain #getAvailableProcessors available processors}
     * and the number of runnable entities running on the available processors
     * averaged over a period of time.
     * The way in which the load average is calculated is operating system
     * specific but is typically a damped time-dependent average.
     * <p>
     * If the load average is not available, a negative value is returned.
     * <p>
     * This method is designed to provide a hint about the system load
     * and may be queried frequently. The load average may be unavailable on
     * some platform where it is expensive to implement this method.
     *
     * @return The system load average; or a negative value if not available.
     */
    public double getCurrentCpuLoad();

    /**
     * Returns amount of time spent in GC since the last update.
     * <p>
     * The return value is a percentage of time.
     *
     * @return Amount of time, spent in GC since the last update.
     */
    public double getCurrentGcCpuLoad();

    /**
     * Returns the amount of heap memory in bytes that the Java virtual machine
     * initially requests from the operating system for memory management.
     * This method returns {@code -1} if the initial memory size is undefined.
     * <p>
     * This value represents a setting of the heap memory for Java VM and is
     * not a sum of all initial heap values for all memory pools.
     *
     * @return The initial size of memory in bytes; {@code -1} if undefined.
     */
    public long getHeapMemoryInitialized();

    /**
     * Returns the current heap size that is used for object allocation.
     * The heap consists of one or more memory pools. This value is
     * the sum of {@code used} heap memory values of all heap memory pools.
     * <p>
     * The amount of used memory in the returned is the amount of memory
     * occupied by both live objects and garbage objects that have not
     * been collected, if any.
     *
     * @return Amount of heap memory used.
     */
    public long getHeapMemoryUsed();

    /**
     * Returns the amount of heap memory in bytes that is committed for
     * the Java virtual machine to use. This amount of memory is
     * guaranteed for the Java virtual machine to use.
     * The heap consists of one or more memory pools. This value is
     * the sum of {@code committed} heap memory values of all heap memory pools.
     *
     * @return The amount of committed memory in bytes.
     */
    public long getHeapMemoryCommitted();

    /**
     * Returns the maximum amount of heap memory in bytes that can be
     * used for memory management. This method returns {@code -1}
     * if the maximum memory size is undefined.
     * <p>
     * This amount of memory is not guaranteed to be available
     * for memory management if it is greater than the amount of
     * committed memory.  The Java virtual machine may fail to allocate
     * memory even if the amount of used memory does not exceed this
     * maximum size.
     * <p>
     * This value represents a setting of the heap memory for Java VM and is
     * not a sum of all initial heap values for all memory pools.
     *
     * @return The maximum amount of memory in bytes; {@code -1} if undefined.
     */
    public long getHeapMemoryMaximum();

    /**
     * Returns the amount of non-heap memory in bytes that the Java virtual machine
     * initially requests from the operating system for memory management.
     * This method returns {@code -1} if the initial memory size is undefined.
     * <p>
     * This value represents a setting of non-heap memory for Java VM and is
     * not a sum of all initial heap values for all memory pools.
     *
     * @return The initial size of memory in bytes; {@code -1} if undefined.
     */
    public long getNonHeapMemoryInitialized();

    /**
     * Returns the current non-heap memory size that is used by Java VM.
     * The non-heap memory consists of one or more memory pools. This value is
     * the sum of {@code used} non-heap memory values of all non-heap memory pools.
     *
     * @return Amount of none-heap memory used.
     */
    public long getNonHeapMemoryUsed();

    /**
     * Returns the amount of non-heap memory in bytes that is committed for
     * the Java virtual machine to use. This amount of memory is
     * guaranteed for the Java virtual machine to use.
     * The non-heap memory consists of one or more memory pools. This value is
     * the sum of {@code committed} non-heap memory values of all non-heap memory pools.
     *
     * @return The amount of committed memory in bytes.
     */
    public long getNonHeapMemoryCommitted();

    /**
     * Returns the maximum amount of non-heap memory in bytes that can be
     * used for memory management. This method returns {@code -1}
     * if the maximum memory size is undefined.
     * <p>
     * This amount of memory is not guaranteed to be available
     * for memory management if it is greater than the amount of
     * committed memory. The Java virtual machine may fail to allocate
     * memory even if the amount of used memory does not exceed this
     * maximum size.
     * <p>
     * This value represents a setting of the non-heap memory for Java VM and is
     * not a sum of all initial non-heap values for all memory pools.
     *
     * @return The maximum amount of memory in bytes; {@code -1} if undefined.
     */
    public long getNonHeapMemoryMaximum();

    /**
     * Returns the uptime of the Java virtual machine in milliseconds.
     *
     * @return Uptime of the Java virtual machine in milliseconds.
     */
    public long getUptime();

    /**
     * Returns the start time of the Java virtual machine in milliseconds.
     * This method returns the approximate time when the Java virtual
     * machine started.
     *
     * @return Start time of the Java virtual machine in milliseconds.
     */
    public long getStartTime();

    /**
     * Returns the current number of live threads including both
     * daemon and non-daemon threads.
     *
     * @return the current number of live threads.
     */
    public int getThreadCount();

    /**
     * Returns the peak live thread count since the Java virtual machine
     * started or peak was reset.
     *
     * @return The peak live thread count.
     */
    public int getPeakThreadCount();

    /**
     * Returns the total number of threads created and also started
     * since the Java virtual machine started.
     *
     * @return The total number of threads started.
     */
    public long getTotalStartedThreadCount();

    /**
     * Returns the current number of live daemon threads.
     *
     * @return The current number of live daemon threads.
     */
    public int getDaemonThreadCount();
}