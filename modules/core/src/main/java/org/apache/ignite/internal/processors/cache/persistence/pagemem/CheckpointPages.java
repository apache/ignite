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

package org.apache.ignite.internal.processors.cache.persistence.pagemem;

import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.pagemem.FullPageId;

/**
 * View of pages which should be stored during current checkpoint.
 */
class CheckpointPages {
    /** */
    private final Collection<FullPageId> segCheckpointPages;

    /** The sign which allows to replace pages from a checkpoint by page replacer. */
    private final IgniteInternalFuture allowToReplace;

    /**
     * @param pages Pages which would be stored to disk in current checkpoint.
     * @param replaceFuture The sign which allows to replace pages from a checkpoint by page replacer.
     */
    CheckpointPages(Collection<FullPageId> pages, IgniteInternalFuture replaceFuture) {
        segCheckpointPages = pages;
        allowToReplace = replaceFuture;
    }

    /**
     * @param fullPageId Page id for checking.
     * @return {@code true} If fullPageId is allowable to store to disk.
     */
    public boolean allowToSave(FullPageId fullPageId) throws IgniteCheckedException {
        Collection<FullPageId> checkpointPages = segCheckpointPages;

        if (checkpointPages == null || allowToReplace == null)
            return false;

        //Uninterruptibly is important because otherwise in case of interrupt of client thread node would be stopped.
        allowToReplace.getUninterruptibly();

        return checkpointPages.contains(fullPageId);
    }

    /**
     * @param fullPageId Page id for checking.
     * @return {@code true} If fullPageId is candidate to stored to disk by current checkpoint.
     */
    public boolean contains(FullPageId fullPageId) {
        Collection<FullPageId> checkpointPages = segCheckpointPages;

        return checkpointPages != null && checkpointPages.contains(fullPageId);
    }

    /**
     * @param fullPageId Page id which should be marked as saved to disk.
     * @return {@code true} if is marking was successful.
     */
    public boolean markAsSaved(FullPageId fullPageId) {
        Collection<FullPageId> checkpointPages = segCheckpointPages;

        return checkpointPages != null && checkpointPages.remove(fullPageId);
    }

    /**
     * @return Size of all pages in current checkpoint.
     */
    public int size() {
        Collection<FullPageId> checkpointPages = segCheckpointPages;

        return checkpointPages == null ? 0 : checkpointPages.size();
    }
}
