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

package org.apache.ignite.internal.processors.cache.persistence.db.checkpoint;

import org.apache.ignite.internal.pagemem.FullPageId;
import org.apache.ignite.internal.pagemem.PageIdAllocator;
import org.apache.ignite.internal.pagemem.PageIdUtils;
import org.apache.ignite.internal.processors.cache.persistence.checkpoint.CheckpointWorkflow;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test for CheckpointWorkflow.arrangeForPreallocation method.
 */
public class PageIdsArrangeForPreallocationTest {
    /** */
    @Test
    public void testSinglePageId() {
        FullPageId[] pageIds = new FullPageId[] {
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 1), 1),
        };

        CheckpointWorkflow.arrangeForPreallocation(pageIds);

        FullPageId[] exp = new FullPageId[] {
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 1), 1),
        };

        Assert.assertArrayEquals(exp, pageIds);
    }

    /** */
    @Test
    public void testSinglePartition() {
        FullPageId[] pageIds = new FullPageId[] {
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 1), 1),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 2), 1),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 3), 1),
        };

        CheckpointWorkflow.arrangeForPreallocation(pageIds);

        FullPageId[] exp = new FullPageId[] {
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 3), 1),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 1), 1),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 2), 1),
        };

        Assert.assertArrayEquals(exp, pageIds);
    }

    /** */
    @Test
    public void testSamePartitionDifferentGroups() {
        FullPageId[] pageIds = new FullPageId[] {
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 1), 1),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 1), 2),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 2), 3),
        };

        CheckpointWorkflow.arrangeForPreallocation(pageIds);

        FullPageId[] exp = new FullPageId[] {
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 1), 1),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 1), 2),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 2), 3),
        };

        Assert.assertArrayEquals(exp, pageIds);
    }

    /** */
    @Test
    public void testMixed() {
        FullPageId[] pageIds = new FullPageId[] {
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 1), 1),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 2), 1),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 5), 1),
            new FullPageId(PageIdUtils.pageId(2, PageIdAllocator.FLAG_DATA, 4), 1),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 1), 2),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 3), 2),
            new FullPageId(PageIdUtils.pageId(3, PageIdAllocator.FLAG_DATA, 1), 2),
            new FullPageId(PageIdUtils.pageId(3, PageIdAllocator.FLAG_DATA, 2), 2),
            new FullPageId(PageIdUtils.pageId(3, PageIdAllocator.FLAG_DATA, 3), 2),
            new FullPageId(PageIdUtils.pageId(3, PageIdAllocator.FLAG_DATA, 4), 2),
            new FullPageId(PageIdUtils.pageId(3, PageIdAllocator.FLAG_DATA, 5), 2),
        };

        CheckpointWorkflow.arrangeForPreallocation(pageIds);

        FullPageId[] exp = new FullPageId[] {
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 5), 1),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 1), 1),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 2), 1),
            new FullPageId(PageIdUtils.pageId(2, PageIdAllocator.FLAG_DATA, 4), 1),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 3), 2),
            new FullPageId(PageIdUtils.pageId(1, PageIdAllocator.FLAG_DATA, 1), 2),
            new FullPageId(PageIdUtils.pageId(3, PageIdAllocator.FLAG_DATA, 5), 2),
            new FullPageId(PageIdUtils.pageId(3, PageIdAllocator.FLAG_DATA, 1), 2),
            new FullPageId(PageIdUtils.pageId(3, PageIdAllocator.FLAG_DATA, 2), 2),
            new FullPageId(PageIdUtils.pageId(3, PageIdAllocator.FLAG_DATA, 3), 2),
            new FullPageId(PageIdUtils.pageId(3, PageIdAllocator.FLAG_DATA, 4), 2),
        };

        Assert.assertArrayEquals(exp, pageIds);
    }
}
