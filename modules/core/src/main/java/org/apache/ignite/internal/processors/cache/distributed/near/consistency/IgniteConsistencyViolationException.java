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

package org.apache.ignite.internal.processors.cache.distributed.near.consistency;

import java.util.Set;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.processors.cache.KeyCacheObject;
import org.apache.ignite.internal.util.future.GridCompoundFuture.SkipLoggingException;

/**
 * Consistency violation exception.
 */
public abstract class IgniteConsistencyViolationException extends IgniteCheckedException implements SkipLoggingException {
    /** The value is equal to the one computed by JVM for the class in Ignite 2.18.0. Keep it to provide compatibility. */
    private static final long serialVersionUID = 7803760939138900218L;

    /**
     * Inconsistent entries keys.
     */
    public abstract Set<KeyCacheObject> keys();
}
