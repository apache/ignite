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

package org.apache.ignite.internal.igfs.common;

import java.util.Arrays;
import java.util.List;

/**
 * Grid file system commands to call remotely.
 */
public enum IgfsIpcCommand {
    /** Handshake command which will send information necessary for client to handle requests correctly. */
    HANDSHAKE,

    /** IGFS status (free/used space). */
    STATUS,

    /** Check specified path exists in the file system. */
    EXISTS,

    /** Get information for the file in specified path. */
    INFO,

    /** Get directory summary. */
    PATH_SUMMARY,

    /** Update information for the file  in specified path. */
    UPDATE,

    /** Rename file. */
    RENAME,

    /** Delete file. */
    DELETE,

    /** Make directories. */
    MAKE_DIRECTORIES,

    /** List files under the specified path. */
    LIST_PATHS,

    /** List files under the specified path. */
    LIST_FILES,

    /** Get affinity block locations for data blocks of the file. */
    AFFINITY,

    /** Updates last access and last modification time for a path. */
    SET_TIMES,

    /** Open file for reading as an input stream. */
    OPEN_READ,

    /** Open existent file as output stream to append data to. */
    OPEN_APPEND,

    /** Create file and open output stream for writing data to. */
    OPEN_CREATE,

    /** Close stream. */
    CLOSE,

    /** Read file's data block. */
    READ_BLOCK,

    /** Write file's data block. */
    WRITE_BLOCK,

    /** Server response. */
    CONTROL_RESPONSE,

    /** Mode resolver request */
    MODE_RESOLVER;

    /** All values */
    private static final List<IgfsIpcCommand> ALL = Arrays.asList(values());

    /**
     * Resolve command by its ordinal.
     *
     * @param ordinal Command ordinal.
     * @return Resolved command.
     */
    public static IgfsIpcCommand valueOf(int ordinal) {
        return ALL.get(ordinal);
    }
}