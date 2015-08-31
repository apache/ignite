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

package org.apache.ignite.spi;

import org.apache.ignite.lang.IgniteUuid;

/**
 * Provides possibility to schedule delayed execution,
 * see {@link IgniteSpiContext#addTimeoutObject(IgniteSpiTimeoutObject)}.
 * <p>
 * Note: all timeout objects are executed in single dedicated thread, so implementation
 * of {@link #onTimeout()} should not use time consuming and blocking method.
 */
public interface IgniteSpiTimeoutObject {
    /**
     * @return Unique object ID.
     */
    public IgniteUuid id();

    /**
     * @return End time.
     */
    public long endTime();

    /**
     * Timeout callback.
     */
    public void onTimeout();
}