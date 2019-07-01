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

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.processors.task.GridVisorManagementTask;
import org.apache.ignite.internal.util.io.GridReversedLinesFileReader;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.decode;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.matchedFiles;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.resolveIgnitePath;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.resolveSymbolicLink;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.textFile;

/**
 * Search text matching in logs
 */
@GridInternal
@GridVisorManagementTask
public class VisorLogSearchTask extends VisorMultiNodeTask<VisorLogSearchTaskArg, VisorLogSearchTaskResult,
    Collection<VisorLogSearchResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** How many lines to read around line with found text. */
    public static final int LINE_CNT = 21;

    /** How many lines should be read before and after line with found text. */
    public static final int HALF = LINE_CNT / 2;

    /** {@inheritDoc} */
    @Override protected VisorLogSearchJob job(VisorLogSearchTaskArg arg) {
        return new VisorLogSearchJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected VisorLogSearchTaskResult reduce0(List<ComputeJobResult> results) {
        List<VisorLogSearchResult> searchRes = new ArrayList<>();
        Map<Exception, UUID> exRes = U.newHashMap(0);

        // Separate successfully executed results and exceptions.
        for (ComputeJobResult result : results) {
            if (result.getException() != null)
                exRes.put(result.getException(), result.getNode().id());
            else if (result.getData() != null) {
                Collection<VisorLogSearchResult> data = result.getData();

                searchRes.addAll(data);
            }
        }

        return new VisorLogSearchTaskResult(exRes.isEmpty() ? null : exRes, searchRes.isEmpty() ? null : searchRes);
    }

    /**
     * Job to perform search on node.
     */
    private static class VisorLogSearchJob extends VisorJob<VisorLogSearchTaskArg, Collection<VisorLogSearchResult>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Search descriptor.
         * @param debug Debug flag.
         */
        private VisorLogSearchJob(VisorLogSearchTaskArg arg, boolean debug) {
            super(arg, debug);
        }

        /**
         * @param f File to read.
         * @param charset Text charset.
         * @param searchStr Search string.
         * @param limit Max number of search results.
         * @return Collection of found descriptors.
         * @throws IOException In case of I/O error.
         */
        private List<GridTuple3<String[], Integer, Integer>> searchInFile(File f, Charset charset, String searchStr,
            int limit) throws IOException {
            List<GridTuple3<String[], Integer, Integer>> searched = new ArrayList<>();

            int line = 0;

            try (GridReversedLinesFileReader reader = new GridReversedLinesFileReader(f, 4096, charset)) {
                Deque<String> lastLines = new LinkedList<>();

                String s;
                int lastFoundLine = 0, foundCnt = 0;

                while ((s = reader.readLine()) != null) {
                    line++;

                    if (lastFoundLine > 0 && line - lastFoundLine <= HALF) {
                        for (int i = searched.size() - 1; i >= 0; i--) {
                            GridTuple3<String[], Integer, Integer> tup = searched.get(i);

                            int delta = line - tup.get2();

                            if (delta <= HALF && delta != 0)
                                tup.get1()[HALF - delta] = s;
                            else
                                break;
                        }
                    }

                    if (foundCnt < limit) {
                        if (s.toLowerCase().contains(searchStr)) {
                            String[] buf = new String[LINE_CNT];

                            buf[HALF] = s;

                            int i = 1;

                            for (String l : lastLines) {
                                buf[HALF + i] = l;

                                i++;
                            }

                            lastFoundLine = line;

                            searched.add(new GridTuple3<>(buf, line, 0));

                            foundCnt++;
                        }
                    }

                    if (lastLines.size() >= HALF)
                        lastLines.removeFirst();

                    lastLines.add(s);
                }
            }

            for (GridTuple3<String[], Integer, Integer> entry : searched) {
                entry.set2(line - entry.get2() + 1);
                entry.set3(line);
            }

            return searched;
        }

        /** {@inheritDoc} */
        @Override protected Collection<VisorLogSearchResult> run(VisorLogSearchTaskArg arg) {
            try {
                File folder = resolveIgnitePath(arg.getFolder());

                if (folder == null)
                    return null;

                folder = resolveSymbolicLink(folder);

                UUID uuid = ignite.localNode().id();
                String nid = uuid.toString().toLowerCase();

                String filePtrn = arg.getFilePattern().replace("@nid8", nid.substring(0, 8)).replace("@nid", nid);

                int pathIdx = (folder.isDirectory() ? folder : folder.getParentFile()).getAbsolutePath().length() + 1;

                List<VisorLogFile> matchingFiles = matchedFiles(folder, filePtrn);

                Collection<VisorLogSearchResult> results = new ArrayList<>(arg.getLimit());

                int resCnt = 0;

                for (VisorLogFile logFile : matchingFiles) {
                    try {
                        File f = new File(logFile.getPath());

                        if (textFile(f, false)) {
                            Charset charset = decode(f);

                            if (resCnt == arg.getLimit())
                                break;

                            List<GridTuple3<String[], Integer, Integer>> searched =
                                searchInFile(f, charset, arg.getSearchString(), arg.getLimit() - resCnt);

                            resCnt += searched.size();

                            String path = f.getAbsolutePath().substring(pathIdx);
                            long sz = f.length(), lastModified = f.lastModified();

                            for (GridTuple3<String[], Integer, Integer> e : searched) {
                                results.add(new VisorLogSearchResult(uuid, path, sz, lastModified,
                                    e.get1(), e.get2(), e.get3(), charset.name()));
                            }
                        }
                    }
                    catch (IOException ignored) {
                    }
                }

                return results.isEmpty() ? null : results;
            }
            catch (Exception e) {
                throw new IgniteException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorLogSearchJob.class, this);
        }
    }
}
