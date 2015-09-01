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

package org.apache.ignite.cache.jta;

import javax.transaction.TransactionManager;
import org.apache.ignite.IgniteException;
import org.apache.ignite.configuration.TransactionConfiguration;
import org.jetbrains.annotations.Nullable;

/**
 * Allows grid to use different transactional systems. Implement this interface
 * to look up native transaction manager within your environment. Transaction
 * manager lookup is configured via {@link TransactionConfiguration#getTxManagerLookupClassName()}
 * method.
 * <p>
 * The following implementations are provided out of the box:
 * <ul>
 * <li>
 *  {@link org.apache.ignite.cache.jta.jndi.CacheJndiTmLookup} utilizes a configured JNDI name to look up a transaction manager.
 * </li>
 * <li>
 *  {@link org.apache.ignite.cache.jta.reflect.CacheReflectionTmLookup} uses reflection to call a method on a given class
 *  to get to transaction manager.
 * </li>
 * </ul>
 */
public interface CacheTmLookup {
    /**
     * Gets Transaction Manager (TM).
     *
     * @return TM or {@code null} if TM cannot be looked up.
     * @throws IgniteException In case of error.
     */
    @Nullable public TransactionManager getTm() throws IgniteException;
}