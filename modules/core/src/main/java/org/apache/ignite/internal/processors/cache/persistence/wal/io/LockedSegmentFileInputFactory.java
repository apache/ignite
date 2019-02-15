/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
 */

package org.apache.ignite.internal.processors.cache.persistence.wal.io;

import java.io.IOException;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.wal.ByteBufferExpander;
import org.apache.ignite.internal.processors.cache.persistence.wal.FileDescriptor;
import org.apache.ignite.internal.processors.cache.persistence.wal.aware.SegmentAware;
import org.apache.ignite.internal.processors.cache.persistence.wal.SegmentRouter;

/**
 * Implementation of factory to provide I/O interfaces for read primitives with files.
 *
 * Creating {@link FileInput} with ability locked segment during reading.
 */
public class LockedSegmentFileInputFactory implements SegmentFileInputFactory {
    /** Holder of actual information of latest manipulation on WAL segments. */
    private final SegmentAware segmentAware;
    /** Manager of segment location. */
    private final SegmentRouter segmentRouter;
    /** {@link FileIO} factory definition.*/
    private final FileIOFactory fileIOFactory;

    /**
     * @param segmentAware Holder of actual information of latest manipulation on WAL segments.
     * @param segmentRouter Manager of segment location.
     * @param fileIOFactory {@link FileIO} factory definition.
     */
    public LockedSegmentFileInputFactory(
        SegmentAware segmentAware,
        SegmentRouter segmentRouter,
        FileIOFactory fileIOFactory) {
        this.segmentAware = segmentAware;
        this.segmentRouter = segmentRouter;
        this.fileIOFactory = fileIOFactory;
    }

    /** {@inheritDoc} */
    @Override public FileInput createFileInput(SegmentIO segmentIO, ByteBufferExpander buf) throws IOException {
        return new LockedReadFileInput(
            buf,
            segmentIO,
            segmentAware,
            id -> {
                FileDescriptor segment = segmentRouter.findSegment(id);

                return segment.toIO(fileIOFactory);
            }
        );
    }
}
