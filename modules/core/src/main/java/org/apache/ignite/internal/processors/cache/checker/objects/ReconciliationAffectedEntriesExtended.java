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

package org.apache.ignite.internal.processors.cache.checker.objects;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.function.Consumer;

/**
 * Partition reconciliation result that contains only info about amount of inconsistent keys, skipped caches etc,
 * instead of full information. Used in case of non-console mode for console-scoped report.
 */
public class ReconciliationAffectedEntriesExtended extends ReconciliationAffectedEntries {
    /** */
    private static final long serialVersionUID = 0L;

    /** Inconsistent keys count. */
    private int inconsistentKeysCnt;

    /** Skipped caches count. */
    private int skippedCachesCnt;

    /** Skipped entries count. */
    private int skippedEntriesCnt;

    /**
     * Default constructor for externalization.
     */
    public ReconciliationAffectedEntriesExtended() {
        // No-op
    }

    /**
     * Constructor.
     *
     * @param inconsistentKeysCnt Inconsistent keys count.
     * @param skippedCachesCnt Skipped caches count.
     * @param skippedEntriesCnt Skipped entries count.
     */
    public ReconciliationAffectedEntriesExtended(int inconsistentKeysCnt, int skippedCachesCnt, int skippedEntriesCnt) {
        this.inconsistentKeysCnt = inconsistentKeysCnt;
        this.skippedCachesCnt = skippedCachesCnt;
        this.skippedEntriesCnt = skippedEntriesCnt;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        out.writeInt(inconsistentKeysCnt);

        out.writeInt(skippedCachesCnt);

        out.writeInt(skippedEntriesCnt);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in)
        throws IOException, ClassNotFoundException {
        inconsistentKeysCnt = in.readInt();

        skippedCachesCnt = in.readInt();

        skippedEntriesCnt = in.readInt();
    }

    /**
     * @return Inconsistent keys count.
     */
    @Override public int inconsistentKeysCount() {
        return inconsistentKeysCnt;
    }

    /**
     * @return Skipped caches count.
     */
    @Override public int skippedCachesCount() {
        return skippedCachesCnt;
    }

    /**
     * @return Skipped entries count.
     */
    @Override public int skippedEntriesCount() {
        return skippedEntriesCnt;
    }


    /** @inheritDoc */
    @Override public void merge(ReconciliationAffectedEntries outer) {
        assert outer instanceof ReconciliationAffectedEntriesExtended;

        inconsistentKeysCnt += outer.inconsistentKeysCount();

        skippedCachesCnt += outer.skippedEntriesCount();

        skippedEntriesCnt += outer.skippedCachesCount();
    }

    /** @inheritDoc */
    @Override public void print(Consumer<String> printer, boolean includeSensitive) {
        if (inconsistentKeysCnt != 0)
            printer.accept("\nINCONSISTENT KEYS: " + inconsistentKeysCount() + "\n\n");

        if (skippedCachesCnt != 0)
            printer.accept("\nSKIPPED CACHES: " + skippedCachesCount() + "\n\n");

        if (skippedEntriesCnt != 0)
            printer.accept("\nSKIPPED ENTRIES: " + skippedEntriesCount() + "\n\n");
    }
}
