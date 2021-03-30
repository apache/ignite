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

package org.apache.ignite.internal.metastorage.watch;

import org.apache.ignite.metastorage.client.WatchListener;

/**
 * Watch implementation with associated revision.
 * Instance of this watch produced by {@link WatchAggregator}.
 */
public class AggregatedWatch {
    /** Watch key criterion. */
    private final KeyCriterion keyCriterion;

    /** Aggregated watch listener. */
    private final WatchListener lsnr;

    /** Watch revision. */
    private final long revision;

    /**
     * Creates the instance of aggregated watch.
     *
     * @param keyCriterion Aggregated key criterion.
     * @param revision Aggregated revision.
     * @param lsnr Aggregated listener.
     */
    public AggregatedWatch(KeyCriterion keyCriterion, long revision, WatchListener lsnr) {
        this.keyCriterion = keyCriterion;
        this.revision = revision;
        this.lsnr = lsnr;
    }

    /**
     * @return Key criterion.
     */
    public KeyCriterion keyCriterion() {
        return keyCriterion;
    }

    /**
     * @return Watch listener.
     */
    public WatchListener listener() {
        return lsnr;
    }

    /**
     * @return Revision.
     */
    public long revision() {
        return revision;
    }
}
