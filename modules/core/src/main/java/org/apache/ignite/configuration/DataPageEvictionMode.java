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

package org.apache.ignite.configuration;

/**
 * Enumeration defines data page eviction modes.
 */
public enum DataPageEvictionMode {
    /** Disabled. */
    DISABLED,

    /**
     * Random-LRU algorithm. In a nutshell:
     * 1) During memory policy initialization, off-heap array is allocated to track timestamp of last usage for each
     * data page.
     * 2) When data page on address X is accessed, current timestamp is stored in X / PAGE_SIZE array position.
     * 3) When there's a need for eviction, we randomly choose 5 indices of array (if some of indices point to
     * non-data pages, re-choose them) and evict data page with minimum timestamp.
     */
    RANDOM_LRU,

    /**
     * Random-2-LRU algorithm. Scan-resistant version of Random-LRU. The only difference is that we store two
     * previous timestamps for each data page, and choose minimum between "older" timestamps. LRU-2 outperforms LRU by
     * resolving "one-hit wonder" problem: if page is accessed very rarely, but accidentally accessed once, it's
     * protected from eviction for long time.
     */
    RANDOM_2_LRU
}
