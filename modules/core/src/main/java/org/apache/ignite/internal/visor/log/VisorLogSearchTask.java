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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.IgniteException;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.internal.processors.task.GridInternal;
import org.apache.ignite.internal.util.io.GridReversedLinesFileReader;
import org.apache.ignite.internal.util.lang.GridTuple3;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.internal.visor.VisorJob;
import org.apache.ignite.internal.visor.VisorMultiNodeTask;
import org.apache.ignite.lang.IgniteBiTuple;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.internal.visor.util.VisorTaskUtils.decode;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.matchedFiles;
import static org.apache.ignite.internal.visor.util.VisorTaskUtils.textFile;

/**
 * Search text matching in logs
 */
@GridInternal
public class VisorLogSearchTask extends VisorMultiNodeTask<VisorLogSearchTask.VisorLogSearchArg,
    IgniteBiTuple<Iterable<IgniteBiTuple<Exception, UUID>>, Iterable<VisorLogSearchResult>>,
    Collection<VisorLogSearchResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** How many lines to read around line with found text. */
    public static final int LINE_CNT = 21;

    /** How many lines should be read before and after line with found text. */
    public static final int HALF = LINE_CNT / 2;

    /** {@inheritDoc} */
    @Override protected VisorLogSearchJob job(VisorLogSearchArg arg) {
        return new VisorLogSearchJob(arg, debug);
    }

    /** {@inheritDoc} */
    @Nullable @Override protected IgniteBiTuple<Iterable<IgniteBiTuple<Exception, UUID>>,
        Iterable<VisorLogSearchResult>> reduce0(List<ComputeJobResult> results) {
        Collection<VisorLogSearchResult> searchRes = new ArrayList<>();
        Collection<IgniteBiTuple<Exception, UUID>> exRes = new ArrayList<>();

        // Separate successfully executed results and exceptions.
        for (ComputeJobResult result : results) {
            if (result.getException() != null)
                exRes.add(new IgniteBiTuple<Exception, UUID>(result.getException(), result.getNode().id()));
            else if (result.getData() != null) {
                Collection<VisorLogSearchResult> data = result.getData();

                searchRes.addAll(data);
            }
        }

        return new IgniteBiTuple<Iterable<IgniteBiTuple<Exception, UUID>>, Iterable<VisorLogSearchResult>>
            (exRes.isEmpty() ? null : exRes, searchRes.isEmpty() ? null : searchRes);
    }

    /**
     * Arguments for {@link VisorLogSearchTask}.
     */
    @SuppressWarnings("PublicInnerClass")
    public static class VisorLogSearchArg implements Serializable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Searched string. */
        private final String searchStr;

        /** Folder. */
        private final String folder;

        /** File name search pattern. */
        private final String filePtrn;

        /** Max number of results. */
        private final int limit;

        /**
         * @param searchStr Searched string.
         * @param folder Folder.
         * @param filePtrn File name search pattern.
         * @param limit Max number of results.
         */
        public VisorLogSearchArg(String searchStr, String folder, String filePtrn, int limit) {
            this.searchStr = searchStr;
            this.folder = folder;
            this.filePtrn = filePtrn;
            this.limit = limit;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorLogSearchArg.class, this);
        }
    }

    /**
     * Job to perform search on node.
     */
    private static class VisorLogSearchJob extends VisorJob<VisorLogSearchArg, Collection<VisorLogSearchResult>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Search descriptor.
         * @param debug Debug flag.
         */
        private VisorLogSearchJob(VisorLogSearchArg arg, boolean debug) {
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
        @Override protected Collection<VisorLogSearchResult> run(VisorLogSearchArg arg) {
            URL url = U.resolveIgniteUrl(arg.folder);

            if (url == null)
                throw new IgniteException(new FileNotFoundException("Log folder not found: " + arg.folder));

            UUID uuid = ignite.localNode().id();
            String nid = uuid.toString().toLowerCase();

            String filePtrn = arg.filePtrn.replace("@nid8", nid.substring(0, 8)).replace("@nid", nid);

            try {
                File fld = new File(url.toURI());
                int pathIdx = (fld.isDirectory() ? fld : fld.getParentFile()).getAbsolutePath().length() + 1;

                List<VisorLogFile> matchingFiles = matchedFiles(fld, filePtrn);

                Collection<VisorLogSearchResult> results = new ArrayList<>(arg.limit);

                int resCnt = 0;

                for (VisorLogFile logFile : matchingFiles) {
                    try {
                        File f = new File(logFile.path());

                        if (textFile(f, false)) {
                            Charset charset = decode(f);

                            if (resCnt == arg.limit)
                                break;

                            List<GridTuple3<String[], Integer, Integer>> searched =
                                searchInFile(f, charset, arg.searchStr, arg.limit - resCnt);

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