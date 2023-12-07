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
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.binary.BinaryIdMapper;
import org.apache.ignite.binary.BinaryType;
import org.apache.ignite.cache.CacheEntryVersion;
import org.apache.ignite.internal.cdc.CdcMain;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.lang.IgniteExperimental;
import org.apache.ignite.resources.LoggerResource;
import org.apache.ignite.spi.systemview.view.CacheView;

/**
 * Consumer of WAL data change events.
 * This consumer will receive data change events during {@link CdcMain} application invocation.
 * The lifecycle of the consumer is the following:
 * <ul>
 *     <li>Start of the consumer {@link #start(MetricRegistry)}.</li>
 *     <li>Notification of the consumer by the {@link #onEvents(Iterator)} call.</li>
 *     <li>Stop of the consumer {@link #stop()}.</li>
 * </ul>
 *
 * In case consumer implementation wants to use {@link IgniteLogger}, please, use, {@link LoggerResource} annotation:
 * <pre>
 * public class ChangeDataCaptureConsumer implements ChangeDataCaptureConsumer {
 *     &#64;LoggerResource
 *     private IgniteLogger log;
 *
 *     ...
 * }
 * </pre>
 *
 * Note, consumption of the {@link CdcEvent} will be started from the last saved offset.
 * The offset of consumptions is saved on the disk every time {@link #onEvents(Iterator)} returns {@code true}.
 * Note, order of notifications are following:
 * <ul>
 *     <li>{@link #onMappings(Iterator)}</li>
 *     <li>{@link #onTypes(Iterator)}</li>
 *     <li>{@link #onCacheChange(Iterator)}</li>
 *     <li>{@link #onCacheDestroy(Iterator)}</li>
 * </ul>
 * Note, {@link CdcConsumer} receive notifications on each running CDC application(node).
 *
 * @see CdcMain
 * @see CdcEvent
 * @see CacheEntryVersion
 */
@IgniteExperimental
public interface CdcConsumer {
    /**
     * Starts the consumer.
     * @param mreg Metric registry for consumer specific metrics.
     */
    public void start(MetricRegistry mreg);

    /**
     * Handles entry changes events.
     * If this method return {@code true} then current offset will be stored
     * and ongoing notifications after CDC application fail/restart will be started from it.
     *
     * @param events Entry change events.
     * @return {@code True} if current offset should be saved on the disk
     * to continue from it in case any failures or restart.
     */
    public boolean onEvents(Iterator<CdcEvent> events);

    /**
     * Handles new binary types. State of the types processing will be stored after method invocation
     * and ongoing notifications after CDC application fail/restart will be continued for newly created/updates types.
     *
     * Note, unlike {@link #onEvents(Iterator)} this method MUST process all types or CDC will fail.
     * Because, in time of invocation {@link #onEvents(Iterator)} all changed types must be available on destionation.
     *
     * @param types Binary types iterator.
     * @see IgniteBinary
     * @see IgniteBinary#type(int)
     * @see IgniteBinary#type(Class)
     * @see IgniteBinary#type(String)
     * @see IgniteBinary#types()
     */
    public void onTypes(Iterator<BinaryType> types);

    /**
     * Handles new mappings from type name to id. State of the types processing will be stored after method invocation
     * and ongoing notifications after CDC application fail/restart will be continued for newly created/updates mappings.
     *
     * @param mappings Binary mapping iterator.
     * @see IgniteBinary
     * @see IgniteBinary#typeId(String)
     * @see BinaryIdMapper
     */
    public void onMappings(Iterator<TypeMapping> mappings);

    /**
     * Handles caches changes(create, edit) events. State of cache processing will be stored after method invocation
     * and ongoing notifications after CDC application fail/restart will be continued for newly changed caches.
     *
     * @param cacheEvents Cache change events.
     * @see Ignite#createCache(String)
     * @see Ignite#getOrCreateCache(String)
     * @see CdcCacheEvent
     */
    public void onCacheChange(Iterator<CdcCacheEvent> cacheEvents);

    /**
     * Handles cache destroy events. State of cache processing will be stored after method invocation
     * and ongoing notifications after CDC application fail/restart will be continued for newly changed caches.
     *
     * @param caches Destroyed caches.
     * @see Ignite#destroyCache(String)
     * @see CdcCacheEvent
     * @see CacheView#cacheId()
     */
    public void onCacheDestroy(Iterator<Integer> caches);

    /**
     * Stops the consumer.
     * This method can be invoked only after {@link #start(MetricRegistry)}.
     */
    public void stop();

    /**
     * Checks that consumer still alive.
     * This method helps to determine {@link CdcConsumer} errors in case {@link CdcEvent} is rare or source cluster is down.
     *
     * @return {@code True} in case consumer alive, {@code false} otherwise.
     */
    public default boolean alive() {
        return true;
    }
}
