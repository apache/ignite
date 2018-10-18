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

/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.apache.ignite.internal.processors.cache.persistence.wal;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.pagemem.wal.WALPointer;
import org.apache.ignite.internal.pagemem.wal.record.WALRecord;
import org.apache.ignite.internal.processors.cache.persistence.StorageException;
import org.jetbrains.annotations.Nullable;

/**
 * TODO: Add interface description.
 *
 * @author @java.author
 * @version @java.version
 */
public interface FileWriteHandle {

    int serializerVersion();

    void finishResume();

    void writeHeader() throws IgniteCheckedException;

    @Nullable WALPointer addRecord(WALRecord rec) throws StorageException, IgniteCheckedException;

    void flushAll() throws IgniteCheckedException;

    boolean needFsync(FileWALPointer ptr);

    FileWALPointer position();

    void fsync(FileWALPointer ptr) throws StorageException, IgniteCheckedException;

    void closeBuffer();

    boolean close(boolean rollOver) throws IgniteCheckedException, StorageException;

    void signalNextAvailable();

    void awaitNext();

    String safePosition();

    public long getSegmentId();
}
