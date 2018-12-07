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

package org.apache.ignite.internal.processors.cache.persistence.tree.reuse;

/**
 * Reuse bag for free index pages.
 */
public interface ReuseBag {
    /**
     * @param pageId Free page ID for reuse.
     */
    public void addFreePage(long pageId);

    /**
     * @return Free page ID for reuse or {@code 0} if empty.
     */
    public long pollFreePage();

    /**
     * @return {@code true} if no contained page IDs for reuse.
     */
    public boolean isEmpty();
}
