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

package org.apache.ignite.internal.util;

import junit.framework.TestCase;

/**
 * Grid cleaner tests.
 */
public class GridCleanerTest extends TestCase {
    /**
     * @throws Exception If failed.
     */
    public void testCreate() throws Exception {
        final Object cleaner = GridCleaner.create(this, new Runnable() {
            @Override public void run() {
                // no-op
            }
        });

        assert cleaner != null;

        final String cleanerClsName = cleaner.getClass().getName();

        assert cleanerClsName.equals("sun.misc.Cleaner")
            || cleanerClsName.equals("jdk.internal.ref.CleanerImpl$PhantomCleanableRef");
    }
}
