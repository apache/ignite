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

package org.apache.ignite.internal.commandline.walreader;

import java.util.Base64;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.UnwrapDataEntry;
import org.apache.ignite.internal.processors.cache.CacheObject;
import org.apache.ignite.internal.processors.cache.CacheObjectValueContext;
import org.apache.ignite.internal.processors.cache.persistence.wal.serializer.RecordDataV1Serializer;
import org.apache.ignite.internal.util.IgniteUtils;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.SB;

import static org.apache.ignite.internal.commandline.walreader.ProcessSensitiveData.HASH;
import static org.apache.ignite.internal.commandline.walreader.ProcessSensitiveData.HIDE;
import static org.apache.ignite.internal.commandline.walreader.ProcessSensitiveData.MD5;

/**
 * Wrapper {@link DataEntry} for sensitive data output.
 */
class DataEntryWrapper extends DataEntry {
    /**
     * Wrapped data entry.
     */
    private final DataEntry entry;

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
            dataEntry.partitionCounter(),
            dataEntry.flags()
        );

        this.entry = dataEntry;

        this.sensitiveData = sensitiveData;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        final String keyStr;
        final String valStr;
        if (entry instanceof UnwrapDataEntry) {
            final UnwrapDataEntry unwrappedDataEntry = (UnwrapDataEntry)this.entry;

            keyStr = toString(unwrappedDataEntry.unwrappedKey(), this.entry.key());

            valStr = toString(unwrappedDataEntry.unwrappedValue(), this.entry.value());
        }
        else if (entry instanceof RecordDataV1Serializer.EncryptedDataEntry) {
            keyStr = "<encrypted>";

            valStr = "<encrypted>";
        }
        else {
            keyStr = toString(null, this.entry.key());

            valStr = toString(null, this.entry.value());
        }

        return new SB(this.entry.getClass().getSimpleName())
            .a("[k = ").a(keyStr)
            .a(", v = [").a(valStr).a("]")
            .a(", super = [").a(S.toString(DataEntry.class, entry)).a("]]")
            .toString();
    }

    /**
     * Returns a string representation of the entry key or entry value.
     *
     * @param value unwrappedKey or unwrappedValue
     * @param co    key or value
     * @return String presentation of the entry key or entry value depends on {@code isValue}.
     */
    public String toString(Object value, CacheObject co) {
        String str;
        if (sensitiveData == HIDE)
            return "";

        if (sensitiveData == HASH)
            if (value != null)
                return Integer.toString(value.hashCode());
            else
                return Integer.toString(co.hashCode());

        if (value instanceof String)
            str = (String)value;
        else if (value instanceof BinaryObject)
            str = value.toString();
        else if (value != null)
            str = toStringRecursive(value.getClass(), value);
        else if (co instanceof BinaryObject)
            str = co.toString();
        else
            str = null;

        if (str == null || str.isEmpty()) {

            try {
                CacheObjectValueContext ctx = null;
                try {
                    ctx = IgniteUtils.field(entry, "cacheObjValCtx");
                }
                catch (Exception e) {
                    throw new IgniteException(e);
                }
                str = Base64.getEncoder().encodeToString(co.valueBytes(ctx));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }

        if (sensitiveData == MD5)
            str = ProcessSensitiveDataUtils.md5(str);

        return str;
    }

    /**
     * Produces auto-generated output of string presentation for given object (given the whole hierarchy).
     *
     * @param cls Declaration class of the object.
     * @param obj Object to get a string presentation for.
     * @return String presentation of the given object.
     */
    public static String toStringRecursive(Class cls, Object obj) {
        String result = null;

        if (cls != Object.class)
            result = S.toString(cls, obj, toStringRecursive(cls.getSuperclass(), obj));

        return result;
    }
}
