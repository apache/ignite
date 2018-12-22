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

package org.apache.ignite.internal.util;

/**
 * Allows to track usages of an object.
 * <p/>
 * It's useful when it is necessary to provide additional actions depending on usages, e.g. cleaning.
 */
public interface UsagesTrackable {
    /**
     * Callback to confirm that consumer is going to work with the object.
     *
     * @return Count of usages after increment.
     */
    public default int incrementAndGet() {
        return 0;
    }

    /**
     * Callback to confirm that consumer has finished work with the object.
     *
     * @return Count of usages after decrement.
     */
    public default int decrementAndGet() {
        return 0;
    }

    /**
     * Returns value of usages.
     *
     * @return Count of usages.
     */
    public default int get() {
        return 0;
    }
}
