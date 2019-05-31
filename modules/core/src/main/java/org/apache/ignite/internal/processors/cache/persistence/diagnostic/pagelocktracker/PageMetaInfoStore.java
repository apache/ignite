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

package org.apache.ignite.internal.processors.cache.persistence.diagnostic.pagelocktracker;

/**
 *
 */
public interface PageMetaInfoStore {
    /**
     * @return Capacity.
     */
    int capacity();

    /**
     * @return True if empty.
     */
    boolean isEmpty();

    /**
     * Add page to store.
     *
     * @param itemIdx Index of page in store.
     * @param op Page operation.
     * @param structureId Data structure id.
     * @param pageId Page id.
     * @param pageAddrHeader Page header addres.
     * @param pageAddr Page addres.
     */
    void add(int itemIdx, int op, int structureId, long pageId, long pageAddrHeader, long pageAddr);

    /**
     * Remove page from store by index.
     */
    void remove(int itemIdx);

    /**
     * @param itemIdx Index of page in store.
     * @return Page operation.
     */
    int getOperation(int itemIdx);

    /**
     * @param itemIdx Index of page in store.
     * @return Data structure id.
     */
    int getStructureId(int itemIdx);

    /**
     * @param itemIdx Index of page in store.
     * @return Page id.
     */
    long getPageId(int itemIdx);

    /**
     * @param itemIdx Index of page in store.
     * @return Page header address.
     */
    long getPageAddrHeader(int itemIdx);

    /**
     * @param itemIdx Index of page in store.
     * @return Page address.
     */
    long getPageAddr(int itemIdx);

    /**
     * @return Copy of current store state.
     */
    PageMetaInfoStore copy();

    /**
     * Free resource.
     */
    void free();
}
