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

package org.apache.ignite.internal.visor.log;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorDataTransferObject;

/**
 * Result for log search operation.
 * Contains found line and several lines before and after, plus other info.
 */
public class VisorLogSearchResult extends VisorDataTransferObject {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private UUID nid;

    /** File path relative to the search folder. */
    private String filePath;

    /** File size. */
    private long fileSize;

    /** Timestamp of last modification of the file. */
    private long lastModified;

    /** Lines of text including found line and several lines before and after. */
    private List<String> lines;

    /** Line number in the file, 1 based. */
    private int lineNum;

    /** Lines count in the file. */
    private int lineCnt;

    /** File content encoding. */
    private String encoding;

    /**
     * Default constructor.
     */
    public VisorLogSearchResult() {
        // No-op.
    }

    /**
     * Create log search result with given parameters.
     *
     * @param nid Node ID.
     * @param filePath File path relative to the search folder.
     * @param fileSize File size.
     * @param lastModified Timestamp of last modification of the file.
     * @param lines Lines of text including found line and several lines before and after.
     * @param lineNum Line number in the file, 1 based.
     * @param lineCnt Lines count in the file.
     * @param encoding File content encoding.
     */
    public VisorLogSearchResult(
        UUID nid,
        String filePath,
        long fileSize,
        long lastModified,
        String[] lines,
        int lineNum,
        int lineCnt,
        String encoding
    ) {
        this.nid = nid;
        this.filePath = filePath;
        this.fileSize = fileSize;
        this.lastModified = lastModified;
        this.lines = Arrays.asList(lines);
        this.lineNum = lineNum;
        this.lineCnt = lineCnt;
        this.encoding = encoding;
    }

    /**
     * @return Node ID.
     */
    public UUID getNid() {
        return nid;
    }

    /**
     * @return File path relative to the search folder.
     */
    public String getFilePath() {
        return filePath;
    }

    /**
     * @return File size.
     */
    public long getFileSize() {
        return fileSize;
    }

    /**
     * @return Timestamp of last modification of the file.
     */
    public long getLastModified() {
        return lastModified;
    }

    /**
     * @return Lines of text including found line and several lines before and after.
     */
    public List<String> getLines() {
        return lines;
    }

    /**
     * @return Line number in the file, 1 based.
     */
    public int getLineNumber() {
        return lineNum;
    }

    /**
     * @return Lines count in the file.
     */
    public int getLineCount() {
        return lineCnt;
    }

    /**
     * @return File content encoding.
     */
    public String getEncoding() {
        return encoding;
    }

    /**
     * @return Found line.
     */
    public String getLine() {
        return lines.get(lines.size() / 2);
    }

    /** {@inheritDoc} */
    @Override protected void writeExternalData(ObjectOutput out) throws IOException {
        U.writeUuid(out, nid);
        U.writeString(out, filePath);
        out.writeLong(fileSize);
        out.writeLong(lastModified);
        U.writeCollection(out, lines);
        out.writeInt(lineNum);
        out.writeInt(lineCnt);
        U.writeString(out, encoding);
    }

    /** {@inheritDoc} */
    @Override protected void readExternalData(byte protoVer, ObjectInput in) throws IOException, ClassNotFoundException {
        nid = U.readUuid(in);
        filePath = U.readString(in);
        fileSize = in.readLong();
        lastModified = in.readLong();
        lines = U.readList(in);
        lineNum = in.readInt();
        lineCnt = in.readInt();
        encoding = U.readString(in);
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorLogSearchResult.class, this);
    }
}
