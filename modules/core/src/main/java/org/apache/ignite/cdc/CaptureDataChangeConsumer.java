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

package org.apache.ignite.cdc;

import java.util.Iterator;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.cdc.IgniteCDC;
import org.apache.ignite.lang.IgniteExperimental;

/**
 * Consumer of WAL data change events.
 * This consumer will receive event of data changes during {@link IgniteCDC} application invocation.
 * Lifecycle of consumer is the following:
 * <ul>
 *     <li>Start of the consumer {@link #start(IgniteLogger)}.</li>
 *     <li>Notification of the consumer by the {@link #onChange(Iterator)} call.</li>
 *     <li>Stop of the consumer {@link #stop()}.</li>
 * </ul>
 *
 * Note, consumption of the {@link ChangeEvent} will started from the last saved offset.
 * Offset of consumptions is saved on the disk every time {@link #onChange(Iterator)} returns {@code true}.
 *
 * @see IgniteCDC
 * @see ChangeEvent
 * @see ChangeEventOrder
 */
@IgniteExperimental
public interface CaptureDataChangeConsumer {
    /**
     * Starts the consumer.
     *
     * @param log Logger.
     */
    void start(IgniteLogger log);

    /**
     * Handles entry changes events.
     * If this method return {@code true} then current offset will be stored and ongoing notifications after CDC application fail/restart
     * will be started from it.
     *
     * @param events Entry change events.
     * @return {@code True} if current offset should be saved on the disk to continue from it in case any failures or restart.
     */
    boolean onChange(Iterator<ChangeEvent> events);

    /**
     * Stops the consumer.
     * This methods can be invoked only after {@link #start(IgniteLogger)}.
     */
    void stop();
}
