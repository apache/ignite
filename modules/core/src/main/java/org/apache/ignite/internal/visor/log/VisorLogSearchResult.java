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

import java.io.Serializable;
import java.util.UUID;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * Result for log search operation.
 * Contains found line and several lines before and after, plus other info.
 */
public class VisorLogSearchResult implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    /** Node ID. */
    private final UUID nid;

    /** File path relative to the search folder. */
    private final String filePath;

    /** File size. */
    private final long fileSize;

    /** Timestamp of last modification of the file. */
    private final long lastModified;

    /** Lines of text including found line and several lines before and after. */
    private final String[] lines;

    /** Line number in the file, 1 based. */
    private final int lineNum;

    /** Lines count in the file. */
    private final int lineCnt;

    /** File content encoding. */
    private final String encoding;

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
        this.lines = lines;
        this.lineNum = lineNum;
        this.lineCnt = lineCnt;
        this.encoding = encoding;
    }

    /**
     * @return Node ID.
     */
    public UUID nid() {
        return nid;
    }

    /**
     * @return File path relative to the search folder.
     */
    public String filePath() {
        return filePath;
    }

    /**
     * @return File size.
     */
    public long fileSize() {
        return fileSize;
    }

    /**
     * @return Timestamp of last modification of the file.
     */
    public long lastModified() {
        return lastModified;
    }

    /**
     * @return Lines of text including found line and several lines before and after.
     */
    public String[] lines() {
        return lines;
    }

    /**
     * @return Line number in the file, 1 based.
     */
    public int lineNumber() {
        return lineNum;
    }

    /**
     * @return Lines count in the file.
     */
    public int lineCount() {
        return lineCnt;
    }

    /**
     * @return File content encoding.
     */
    public String encoding() {
        return encoding;
    }

    /**
     * @return Found line.
     */
    public String line() {
        return lines[lines.length / 2];
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(VisorLogSearchResult.class, this);
    }
}