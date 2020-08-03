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

package org.apache.ignite.internal.processors.cache.persistence;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.EnumSet;
import java.util.TimeZone;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.pagemem.wal.record.DataEntry;
import org.apache.ignite.internal.pagemem.wal.record.DataRecord;
import org.apache.ignite.internal.pagemem.wal.record.TxRecord;
import org.apache.ignite.internal.processors.cache.version.GridCacheVersion;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.jetbrains.annotations.Nullable;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;

/**
 *
 */
public class RecoveryDebug implements AutoCloseable {
    /** */
    private static final ThreadLocal<SimpleDateFormat> sdf = new ThreadLocal<SimpleDateFormat>() {
        /** {@inheritDoc} */
        @Override protected SimpleDateFormat initialValue() {
            SimpleDateFormat f = new SimpleDateFormat("dd-MM-yyyy-HH-mm-ss-SSS");

            f.setTimeZone(TimeZone.getTimeZone("UTC"));

            return f;
        }
    };

    /** */
    @Nullable private final IgniteLogger log;

    /** */
    @Nullable private FileChannel fc;

    /**
     * @param constId Consistent ID.
     */
    public RecoveryDebug(Object constId, long time) {
        this(constId, time, null);
    }

    /**
     * @param constId Consistent ID.
     * @param log Logger.
     */
    public RecoveryDebug(Object constId,long time, @Nullable IgniteLogger log) {
        this.log = log;

        try {
            String workDir = U.defaultWorkDirectory();

            File tmpDir = new File(workDir, "tmp");

            if (!tmpDir.exists())
                if (!tmpDir.mkdir())
                    return;

            File f = new File(tmpDir, "recovery-" +
                sdf.get().format(new Date(time)) + "-" + constId + ".log");

            f.createNewFile();

            fc = FileChannel.open(Paths.get(f.getPath()), EnumSet.of(CREATE, READ, WRITE));
        }
        catch (IgniteCheckedException | IOException e) {
            U.error(log, "Fail create recovery debug file.", e);

            fc = null;
        }
    }

    /**
     * @param rec TX record to append.
     * @return {@code this} for convenience.
     */
    public RecoveryDebug append(TxRecord rec) {
        GridCacheVersion txVer = rec.nearXidVersion();

        return fc == null ? this : appendFile(
            "Tx record " + rec.state() + " " + rec.nearXidVersion() + " timestamp " + rec.timestamp()
        );
    }

    /**
     * @param rec Data record to append.
     * @param unwrapKeyValue unwrap key and value flag.
     * @return {@code this} for convenience.
     */
    public RecoveryDebug append(DataRecord rec, boolean unwrapKeyValue) {
        if (fc == null)
            return this;

        append("Data record\n");

        for (DataEntry dataEntry : rec.writeEntries())
            append("\t" + dataEntry.op() + " " + dataEntry.nearXidVersion() +
                (unwrapKeyValue ? " " + dataEntry.key() + " " + dataEntry.value() : "") + "\n"
            );

        return this;
    }

    /**
     * @param st Statement to append.
     * @return {@code this} for convenience.
     */
    public RecoveryDebug append(Object st) {
        return fc == null ? this : appendFile(st);
    }

    /**
     * @param st Statement to append.
     * @return {@code this} for convenience.
     */
    private RecoveryDebug appendFile(Object st) {
        try {
            fc.write(ByteBuffer.wrap(st.toString().getBytes()));
        }
        catch (IOException e) {
            U.error(null, "Fail write to recovery dump file.", e);
        }

        return this;
    }

    /**
     * Closes this debug insrance.
     */
    @Override public void close() {
        if (fc != null) {
            try {
                fc.force(true);

                fc.close();
            }
            catch (IOException e) {
                U.error(null, "Fail close recovery dump file.", e);
            }
        }
    }
}
