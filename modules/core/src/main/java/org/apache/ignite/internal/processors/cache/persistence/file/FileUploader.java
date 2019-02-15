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

package org.apache.ignite.internal.processors.cache.persistence.file;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

/**
 * Part of direct node to node file downloading
 */
public class FileUploader {
    /** */
    private static final int CHUNK_SIZE = 1024 * 1024;

    /** */
    private final Path path;

    /**
     *
     */
    public FileUploader(Path path) {
        this.path = path;
    }

    /**
     *
     */
    public void upload(SocketChannel writeChannel, GridFutureAdapter<Long> finishFut) {
        FileChannel readChannel = null;

        try {
            File file = new File(path.toUri().getPath());

            if (!file.exists()) {
                finishFut.onDone(
                    new IgniteCheckedException(
                        new FileNotFoundException(file.getAbsolutePath())
                    )
                );

                return;
            }

            readChannel = FileChannel.open(path, StandardOpenOption.READ);

            long written = 0;

            long size = readChannel.size();

            while (written < size)
                written += readChannel.transferTo(written, CHUNK_SIZE, writeChannel);

            finishFut.onDone(written);
        }
        catch (IOException ex) {
            finishFut.onDone(ex);
        }
        finally {
            //FIXME: when an error occurs on writeChannel.close() no attempt to close readChannel will happen. Need to be fixed.
            try {
                if (writeChannel != null)
                    writeChannel.close();
            }
            catch (IOException ex) {
                throw new IgniteException("Could not close socket.");
            }

            try {
                if (readChannel != null)
                    readChannel.close();
            }
            catch (IOException ex) {
                throw new IgniteException("Could not close file: " + path);
            }
        }
    }
}
