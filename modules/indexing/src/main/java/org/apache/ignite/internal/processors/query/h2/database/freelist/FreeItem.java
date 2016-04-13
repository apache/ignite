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

package org.apache.ignite.internal.processors.query.h2.database.freelist;

import org.apache.ignite.internal.pagemem.FullPageId;

/**
 * Free list item.
 */
public class FreeItem extends FullPageId {
    /** */
    private short freeSpace;

    /** */
    private short dispersion;

    /**
     * @param freeSpace Free space.
     * @param dispersion Dispersion.
     * @param pageId  Page ID.
     * @param cacheId Cache ID.
     */
    public FreeItem(short freeSpace, short dispersion, long pageId, int cacheId) {
        super(pageId, cacheId);

        assert freeSpace >= 0: freeSpace;

        this.freeSpace = freeSpace;
        this.dispersion = dispersion;
    }

    /**
     * @param freeSpace Free space.
     * @param dispersion Dispersion.
     * @return Dispersed free space.
     */
    public static int disperse(int freeSpace, int dispersion) {
        return (freeSpace << 16) | dispersion;
    }

    /**
     * @return Dispersed free space.
     */
    public int dispersedFreeSpace() {
        return disperse(freeSpace, dispersion);
    }

    /**
     * @return Free space in the page.
     */
    public short freeSpace() {
        return freeSpace;
    }

    /**
     * @param freeSpace Free space.
     */
    public void freeSpace(short freeSpace) {
        this.freeSpace = freeSpace;
    }

    /**
     * @return Dispersion.
     */
    public short dispersion() {
        return dispersion;
    }

    /**
     * @param dispersion Dispersion.
     */
    public void dispersion(short dispersion) {
        this.dispersion = dispersion;
    }
}
