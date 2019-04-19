/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.util;

import org.junit.Test;

/**
 * Grid cleaner tests.
 */
public class GridCleanerTest {
    /**
     * @throws Exception If failed.
     */
    @Test
    public void testCreate() throws Exception {
        Object cleaner = GridCleaner.create(this, new Runnable() {
            @Override public void run() {
                // no-op
            }
        });

        assert cleaner != null;

        String clsName = cleaner.getClass().getName();

        assert clsName.equals("sun.misc.Cleaner")
            || clsName.equals("jdk.internal.ref.CleanerImpl$PhantomCleanableRef");
    }
}
