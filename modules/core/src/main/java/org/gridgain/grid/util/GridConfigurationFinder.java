/* @java.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

package org.gridgain.grid.util;

import org.gridgain.grid.util.lang.*;
import org.gridgain.grid.util.typedef.*;
import org.gridgain.grid.util.typedef.internal.*;

import java.io.*;
import java.util.*;

/**
 * Finds configuration files located in {@code GRIDGAIN_HOME} folder
 * and its subfolders.
 */
public final class GridConfigurationFinder {
    /** Path to default configuration file. */
    private static final String DFLT_CFG = "config" + File.separator + "default-config.xml";

    /** Prefix for questionable paths. */
    public static final String Q_PREFIX = "(?)";

    /** */
    private static final int Q_PREFIX_LEN = Q_PREFIX.length();

    /**
     * Ensure singleton.
     */
    private GridConfigurationFinder() {
        // no-op
    }

    /**
     * Lists paths to all GridGain configuration files located in GRIDGAIN_HOME with their
     * last modification timestamps.
     *
     * @return Collection of configuration files and their last modification timestamps.
     * @throws IOException Thrown in case of any IO error.
     */
    public static List<GridTuple3<String, Long, File>> getConfigFiles() throws IOException {
        return getConfigFiles(new File(U.getGridGainHome()));
    }

    /**
     * Lists paths to all GridGain configuration files located in given directory with their
     * last modification timestamps.
     *
     * @param dir Directory.
     * @return Collection of configuration files and their last modification timestamps.
     * @throws IOException Thrown in case of any IO error.
     */
    private static List<GridTuple3<String, Long, File>> getConfigFiles(File dir) throws IOException {
        assert dir != null;

        LinkedList<GridTuple3<String, Long, File>> lst = listFiles(dir);

        // Sort.
        Collections.sort(lst, new Comparator<GridTuple3<String, Long, File>>() {
            @Override public int compare(GridTuple3<String, Long, File> t1, GridTuple3<String, Long, File> t2) {
                String s1 = t1.get1();
                String s2 = t2.get1();


                String q1 = s1.startsWith(Q_PREFIX) ? s1.substring(Q_PREFIX_LEN + 1) : s1;
                String q2 = s2.startsWith(Q_PREFIX) ? s2.substring(Q_PREFIX_LEN + 1) : s2;

                return q1.compareTo(q2);
            }
        });

        File dflt = new File(U.getGridGainHome() + File.separator + DFLT_CFG);

        if (dflt.exists())
            lst.addFirst(F.t(DFLT_CFG, dflt.lastModified(), dflt));

        return lst;
    }

    /**
     * Lists paths to all GridGain configuration files located in given directory with their
     * last modification timestamps.
     *
     * NOTE: default configuration path will be skipped.
     *
     * @param dir Directory.
     * @return Collection of configuration files and their last modification timestamps.
     * @throws IOException Thrown in case of any IO error.
     */
    private static LinkedList<GridTuple3<String, Long, File>> listFiles(File dir) throws IOException {
        assert dir != null;

        LinkedList<GridTuple3<String, Long, File>> paths = new LinkedList<>();

        for (String name : dir.list()) {
            File file = new File(dir, name);

            if (file.isDirectory())
                paths.addAll(listFiles(file));
            else if (file.getName().endsWith(".xml")) {
                try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
                    boolean springCfg = false;
                    boolean ggCfg = false;

                    String line;

                    while ((line = reader.readLine()) != null) {
                        if (line.contains("http://www.springframework.org/schema/beans"))
                            springCfg = true;

                        if (line.contains("class=\"org.gridgain.grid.GridConfiguration\""))
                            ggCfg = true;

                        if (springCfg && ggCfg)
                            break;
                    }

                    if (springCfg) {
                        String path = file.getAbsolutePath().substring(U.getGridGainHome().length());

                        if (path.startsWith(File.separator))
                            path = path.substring(File.separator.length());

                        if (!path.equals(DFLT_CFG)) {
                            if (!ggCfg)
                                path = Q_PREFIX + ' ' + path;

                            paths.add(F.t(path, file.lastModified(), file));
                        }
                    }
                }
            }
        }

        return paths;
    }
}
