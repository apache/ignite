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

package org.apache.ignite.internal.visor.log;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Arguments for {@link VisorLogSearchTask}.
 */
public class VisorLogSearchTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Searched string. */
    private String searchStr;

    /** Folder. */
    private String folder;

    /** File name search pattern. */
    private String filePtrn;

    /** Max number of results. */
    private int limit;

    /**
     * Default constructor.
     */
    public VisorLogSearchTaskArg() {
        // No-op.
    }

    /**
     * @param searchStr Searched string.
     * @param folder Folder.
     * @param filePtrn File name search pattern.
     * @param limit Max number of results.
     */
    public VisorLogSearchTaskArg(String searchStr, String folder, String filePtrn, int limit) {
        this.searchStr = searchStr;
        this.folder = folder;
        this.filePtrn = filePtrn;
        this.limit = limit;
    }

    /**
     * @return Searched string.
     */
    public String getSearchString() {
        return searchStr;
    }

    /**
     * @return Folder.
     */
    public String getFolder() {
        return folder;
    }

    /**
     * @return File name search pattern.
     */
    public String getFilePattern() {
        return filePtrn;
    }

    /**
     * @return Max number of results.
     */
    public int getLimit() {
        return limit;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, searchStr);
        U.writeString(out, folder);
        U.writeString(out, filePtrn);
        out.writeInt(limit);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        searchStr = U.readString(in);
        folder = U.readString(in);
        filePtrn = U.readString(in);
        limit = in.readInt();
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorLogSearchTaskArg.class, this);
    }
}
