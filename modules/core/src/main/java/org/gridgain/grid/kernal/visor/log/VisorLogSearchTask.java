/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.kernal.visor.log;

import org.gridgain.grid.*;
import org.gridgain.grid.compute.*;
import org.gridgain.grid.kernal.*;
import org.gridgain.grid.kernal.processors.task.*;
import org.gridgain.grid.kernal.visor.*;
import org.gridgain.grid.lang.*;
import org.gridgain.grid.util.io.*;
import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.internal.*;
import org.jetbrains.annotations.*;

import java.io.*;
import java.net.*;
import java.nio.charset.*;
import java.util.*;

import static org.gridgain.grid.kernal.visor.util.VisorTaskUtils.*;

/**
 * Search text matching in logs
 */
@GridInternal
public class VisorLogSearchTask extends VisorMultiNodeTask<VisorLogSearchTask.VisorLogSearchArg,
    GridBiTuple<Iterable<GridBiTuple<Exception, UUID>>, Iterable<VisorLogSearchResult>>,
    Collection<VisorLogSearchResult>> {
    /** */
    private static final long serialVersionUID = 0L;

    /** How many lines to read around line with found text. */
    public static final int LINE_CNT = 21;

    /** How many lines should be read before and after line with found text. */
    public static final int HALF = LINE_CNT / 2;

    /** {@inheritDoc} */
    @Override protected VisorLogSearchJob job(VisorLogSearchArg arg) {
        return new VisorLogSearchJob(arg);
    }

    /** {@inheritDoc} */
    @Nullable @Override public GridBiTuple<Iterable<GridBiTuple<Exception, UUID>>,
        Iterable<VisorLogSearchResult>> reduce(List<GridComputeJobResult> results) throws GridException {

        Collection<VisorLogSearchResult> searchRes = new ArrayList<>();
        Collection<GridBiTuple<Exception, UUID>> exRes = new ArrayList<>();

        // Separate successfully executed results and exceptions.
        for (GridComputeJobResult result : results) {
            if (result.getException() != null)
                exRes.add(new GridBiTuple<Exception, UUID>(result.getException(), result.getNode().id()));
            else if(result.getData() != null) {
                Collection<VisorLogSearchResult> data = result.getData();

                searchRes.addAll(data);
            }
        }

        return new GridBiTuple<Iterable<GridBiTuple<Exception, UUID>>, Iterable<VisorLogSearchResult>>
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
     *  Job to perform search on node.
     */
    private static class VisorLogSearchJob extends VisorJob<VisorLogSearchArg,  Collection<VisorLogSearchResult>> {
        /** */
        private static final long serialVersionUID = 0L;

        /**
         * @param arg Search descriptor.
         */
        private VisorLogSearchJob(VisorLogSearchArg arg) {
            super(arg);
        }

        /**
         * @param f File to read.
         * @param charset Text charset.
         * @param searchStr Search string.
         * @param limit Max number of search results.
         * @return Collection of found descriptors.
         * @throws IOException
         */
        private List<GridTuple3<String[], Integer, Integer>> searchInFile(File f, Charset charset, String searchStr,
            int limit) throws IOException {
            List<GridTuple3<String[], Integer, Integer>> searched = new ArrayList<>();

            int line = 0;

            try (GridReversedLinesFileReader reader = new GridReversedLinesFileReader(f, 4096, charset)) {
                Collection<String> lastLines = new LinkedList<String>() {
                    @Override public boolean add(String s) {
                        if (size() >= HALF)
                            removeFirst();

                        return super.add(s);
                    }
                };

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

                            foundCnt ++;
                        }
                    }

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
        @Override protected  Collection<VisorLogSearchResult> run(VisorLogSearchArg arg) throws GridException {
            URL url = U.resolveGridGainUrl(arg.folder);

            if (url == null)
                throw new GridInternalException(new FileNotFoundException("Log folder not found: " + arg.folder));

            UUID uuid = g.localNode().id();
            String nid = uuid.toString().toLowerCase();

            String filePtrn = arg.filePtrn.replace("@nid8", nid.substring(0, 8)).replace("@nid", nid);

            try {
                File fld = new File(url.toURI());
                int pathIdx = (fld.isDirectory() ? fld : fld.getParentFile()).getAbsolutePath().length() + 1;

                List<VisorLogFile> matchingFiles = matchedFiles(fld, filePtrn);

                Collection<VisorLogSearchResult> results = new ArrayList<>(arg.limit);

                int resultCnt = 0;

                for (VisorLogFile logFile : matchingFiles) {
                    try {
                        File f = new File(logFile.path());

                        if (textFile(f, false)) {
                            Charset charset = decode(f);

                            if (resultCnt == arg.limit)
                                break;

                            List<GridTuple3<String[], Integer, Integer>> searched =
                                searchInFile(f, charset, arg.searchStr, arg.limit - resultCnt);

                            resultCnt += searched.size();

                            String path = f.getAbsolutePath().substring(pathIdx);
                            long sz = f.length(), lastModified = f.lastModified();

                            for (GridTuple3<String[], Integer, Integer> e : searched) {
                                results.add(new VisorLogSearchResult(uuid, path, sz, lastModified,
                                    e.get1(), e.get2(), e.get3(), charset.name()));
                            }
                        }
                    }
                    catch (IOException ignored) {}
                }

                return results.isEmpty() ? null : results;
            } catch (Exception e) {
                throw new GridException(e);
            }
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(VisorLogSearchJob.class, this);
        }
    }
}
