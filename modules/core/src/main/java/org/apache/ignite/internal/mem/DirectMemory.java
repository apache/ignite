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

package org.apache.ignite.internal.mem;

import java.util.List;

/**
 *
 */
public class DirectMemory {
    /** Will be set if  */
    private boolean restored;

    /** */
    private List<DirectMemoryRegion> regions;

    /**
     * @param restored Restored flag.
     * @param regions Memory fragments.
     */
    public DirectMemory(boolean restored, List<DirectMemoryRegion> regions) {
        this.restored = restored;
        this.regions = regions;
    }

    /**
     * @return Restored flag. If {@code true}, the memory fragments were successfully restored since the previous
     *      usage and can be reused.
     */
    public boolean restored() {
        return restored;
    }

    /**
     * @return Memory fragments.
     */
    public List<DirectMemoryRegion> regions() {
        return regions;
    }
}
