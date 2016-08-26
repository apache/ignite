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

package org.apache.ignite.source.flink;

import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.lang.IgnitePredicate;

/**
 * Ignite Predicate filter for {@link IgnitePredicate}.
 */
public class IgnitePredicateInteger implements IgnitePredicate<CacheEvent> {
    /** Default max event new value. */
    private static final int MAX_EVT = 10;

    /**
     * Checks event new value is less than max event value.
     *
     * @param cacheEvt CacheEvent.
     * @return boolean value for filter function.
     */
    @Override
    public boolean apply(CacheEvent cacheEvt) {
        return (Integer)cacheEvt.newValue() >= MAX_EVT;
    }
}
