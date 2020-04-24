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

package org.apache.ignite.development.utils;

import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.UnwrappedDataEntry;
import org.apache.ignite.internal.util.typedef.internal.SB;
import org.jetbrains.annotations.Nullable;

import static java.lang.String.valueOf;
import static java.util.Objects.isNull;
import static org.apache.ignite.development.utils.ProcessSensitiveData.HASH;
import static org.apache.ignite.development.utils.ProcessSensitiveData.MD5;
import static org.apache.ignite.development.utils.ProcessSensitiveDataUtils.md5;

/**
 * Wrapper {@link DataEntry} for sensitive data output.
 */
class DataEntryWrapper extends DataEntry {
    /** Unwrapped DataEntry. */
    @Nullable private final UnwrappedDataEntry unwrappedDataEntry;

    /** Strategy for the processing of sensitive data. */
    private final ProcessSensitiveData sensitiveData;

    /**
     * Constructor.
     *
     * @param dataEntry          Instance of {@link DataEntry}.
     * @param sensitiveData      Strategy for the processing of sensitive data.
     */
    public DataEntryWrapper(
        DataEntry dataEntry,
        ProcessSensitiveData sensitiveData
    ) {
        super(
            dataEntry.cacheId(),
            dataEntry.key(),
            dataEntry.value(),
            dataEntry.op(),
            dataEntry.nearXidVersion(),
            dataEntry.writeVersion(),
            dataEntry.expireTime(),
            dataEntry.partitionId(),
            dataEntry.partitionCounter()
        );

        this.sensitiveData = sensitiveData;

        this.unwrappedDataEntry = UnwrappedDataEntry.class.isInstance(dataEntry) ?
            (UnwrappedDataEntry) dataEntry : null;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        if (isNull(unwrappedDataEntry))
            return super.toString();

        Object key = unwrappedDataEntry.unwrappedKey();
        Object value = unwrappedDataEntry.unwrappedValue();

        if (HASH == sensitiveData) {
            key = valueOf(key).hashCode();
            value = valueOf(value).hashCode();
        }
        else if (MD5 == sensitiveData) {
            key = md5(valueOf(key));
            value = md5(valueOf(value));
        }

        return new SB().a(unwrappedDataEntry.getClass().getSimpleName())
            .a("[k = ").a(key).a(", v = [ ").a(value).a("], super = [").a(super.toString()).a("]]").toString();
    }
}
