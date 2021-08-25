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

package org.apache.ignite.internal.rocksdb;

import java.nio.file.Path;
import org.apache.ignite.lang.IgniteInternalException;
import org.rocksdb.EnvOptions;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.SstFileWriter;

/**
 * RocksDB utility functions.
 */
public class RocksUtils {
    /**
     * Creates an SST file for the column family.
     *
     * @param columnFamily Column family.
     * @param snapshot Point-in-time snapshot.
     * @param path Directory to put the SST file in.
     */
    public static void createSstFile(
        ColumnFamily columnFamily,
        Snapshot snapshot,
        Path path
    ) {
        try (
            EnvOptions envOptions = new EnvOptions();
            Options options = new Options();
            ReadOptions readOptions = new ReadOptions().setSnapshot(snapshot);
            RocksIterator it = columnFamily.newIterator(readOptions);
            SstFileWriter sstFileWriter = new SstFileWriter(envOptions, options)
        ) {
            Path sstFile = path.resolve(columnFamily.name());

            sstFileWriter.open(sstFile.toString());

            it.seekToFirst();

            forEach(it, sstFileWriter::put);

            sstFileWriter.finish();
        }
        catch (Throwable t) {
            throw new IgniteInternalException("Failed to write snapshot: " + t.getMessage(), t);
        }
    }

    /**
     * Iterates over the given iterator passing key-value pairs to the given consumer and
     * checks the iterator's status afterwards.
     *
     * @param iterator Iterator.
     * @param consumer Consumer of key-value pairs.
     * @throws RocksDBException If failed.
     */
    public static void forEach(RocksIterator iterator, RocksBiConsumer consumer) throws RocksDBException {
        for (; iterator.isValid(); iterator.next())
            consumer.accept(iterator.key(), iterator.value());

        checkIterator(iterator);
    }

    /**
     * Iterates over the given iterator testing key-value pairs with the given predicate and checks
     * the iterator's status afterwards.
     *
     * @param iterator Iterator.
     * @param consumer Consumer of key-value pairs.
     * @return {@code true} if a matching key-value pair has been found, {@code false} otherwise.
     * @throws RocksDBException If failed.
     */
    public static boolean find(RocksIterator iterator, RocksBiPredicate consumer) throws RocksDBException {
        for (; iterator.isValid(); iterator.next()) {
            boolean result = consumer.test(iterator.key(), iterator.value());

            if (result)
                return true;
        }

        checkIterator(iterator);

        return false;
    }

    /**
     * Checks the status of the iterator and throws an exception if it is not correct.
     *
     * @param it RocksDB iterator.
     * @throws IgniteInternalException if the iterator has an incorrect status.
     */
    public static void checkIterator(RocksIterator it) {
        try {
            it.status();
        }
        catch (RocksDBException e) {
            throw new IgniteInternalException(e);
        }
    }
}
