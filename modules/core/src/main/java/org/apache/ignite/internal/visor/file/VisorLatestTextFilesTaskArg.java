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

package org.apache.ignite.internal.visor.file;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Arguments for {@link VisorLatestTextFilesTask}
 */
public class VisorLatestTextFilesTaskArg extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Folder path to search files. */
    private String path;

    /** Pattern to match file names. */
    private String regexp;

    /**
     * Default constructor.
     */
    public VisorLatestTextFilesTaskArg() {
        // No-op.
    }

    /**
     * @param path Folder path to search in files.
     * @param regexp Pattern to match file names.
     */
    public VisorLatestTextFilesTaskArg(String path, String regexp) {
        this.path = path;
        this.regexp = regexp;
    }

    /**
     * @return Folder path to search in files.
     */
    public String getPath() {
        return path;
    }

    /**
     * @return Pattern to match file names.
     */
    public String getRegexp() {
        return regexp;
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeString(out, path);
        U.writeString(out, regexp);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        path = U.readString(in);
        regexp = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorLatestTextFilesTaskArg.class, this);
    }
}
