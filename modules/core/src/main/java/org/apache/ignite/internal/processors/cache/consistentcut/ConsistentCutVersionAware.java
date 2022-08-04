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

package org.apache.ignite.internal.processors.cache.consistentcut;

import org.jetbrains.annotations.Nullable;

/**
 * This interface marks messages that responsible for notifying nodes with Consistent Cut Version.
 */
public interface ConsistentCutVersionAware {
    /**
     * Version of the latest known Consistent Cut on sender node.
     *
     * It is used to trigger Consistent Cut procedure on receiver.
     */
    public default ConsistentCutVersion cutVersion() {
        return null;
    }

    /**
     * Version of the latest finished Consistent Cut AFTER which the specified transaction committed.
     *
     * It is used to notify a transaction in the check-list whether to include it to this Consistent Cut.
     *
     * @return {@code null} if transaction will commit BEFORE the latest known Consistent Cut version.
     */
    public default @Nullable ConsistentCutVersion txCutVersion() {
        return null;
    }

    /**
     * Version of the latest finished Consistent Cut AFTER which the specified transaction committed.
     *
     * It is used to notify a transaction in the check-list whether to include it to this Consistent Cut.
     *
     * @param cutVer {@code null} if transaction will commit BEFORE the latest known Consistent Cut version.
     */
    public default void txCutVersion(@Nullable ConsistentCutVersion cutVer) {}
}
