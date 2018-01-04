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

package org.apache.ignite.examples.igfs;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteFileSystem;
import org.apache.ignite.Ignition;
import org.apache.ignite.compute.ComputeJobResult;
import org.apache.ignite.igfs.IgfsOutputStream;
import org.apache.ignite.igfs.IgfsPath;
import org.apache.ignite.igfs.mapreduce.IgfsFileRange;
import org.apache.ignite.igfs.mapreduce.IgfsInputStreamJobAdapter;
import org.apache.ignite.igfs.mapreduce.IgfsJob;
import org.apache.ignite.igfs.mapreduce.IgfsRangeInputStream;
import org.apache.ignite.igfs.mapreduce.IgfsTask;
import org.apache.ignite.igfs.mapreduce.IgfsTaskArgs;
import org.apache.ignite.igfs.mapreduce.records.IgfsNewLineRecordResolver;

/**
 * Example that shows how to use {@link org.apache.ignite.igfs.mapreduce.IgfsTask} to find lines matching particular pattern in the file in pretty
 * the same way as {@code grep} command does.
 * <p>
 * Remote nodes should always be started with configuration file which includes
 * IGFS: {@code 'ignite.sh examples/config/filesystem/example-igfs.xml'}.
 * <p>
 * Alternatively you can run {@link IgfsNodeStartup} in another JVM which will start
 * node with {@code examples/config/filesystem/example-igfs.xml} configuration.
 */
public class IgfsMapReduceExample {
    /**
     * Executes example.
     *
     * @param args Command line arguments. First argument is file name, second argument is regex to look for.
     * @throws Exception If failed.
     */
    public static void main(String[] args) throws Exception {
        if (args.length == 0)
            System.out.println("Please provide file name and regular expression.");
        else if (args.length == 1)
            System.out.println("Please provide regular expression.");
        else {
            try (Ignite ignite = Ignition.start("examples/config/filesystem/example-igfs.xml")) {
                System.out.println();
                System.out.println(">>> IGFS map reduce example started.");

                // Prepare arguments.
                String fileName = args[0];

                File file = new File(fileName);

                String regexStr = args[1];

                // Get an instance of Ignite File System.
                IgniteFileSystem fs = ignite.fileSystem("igfs");

                // Working directory path.
                IgfsPath workDir = new IgfsPath("/examples/fs");

                // Write file to IGFS.
                IgfsPath fsPath = new IgfsPath(workDir, file.getName());

                writeFile(fs, fsPath, file);

                Collection<Line> lines = fs.execute(new GrepTask(), IgfsNewLineRecordResolver.NEW_LINE,
                    Collections.singleton(fsPath), regexStr);

                if (lines.isEmpty()) {
                    System.out.println();
                    System.out.println("No lines were found.");
                }
                else {
                    System.out.println();
                    System.out.println("Found lines:");

                    for (Line line : lines)
                        print(line.fileLine());
                }
            }
        }
    }

    /**
     * Write file to the Ignite file system.
     *
     * @param fs Ignite file system.
     * @param fsPath Ignite file system path.
     * @param file File to write.
     * @throws Exception In case of exception.
     */
    private static void writeFile(IgniteFileSystem fs, IgfsPath fsPath, File file) throws Exception {
        System.out.println();
        System.out.println("Copying file to IGFS: " + file);

        try (
            IgfsOutputStream os = fs.create(fsPath, true);
            FileInputStream fis = new FileInputStream(file)
        ) {
            byte[] buf = new byte[2048];

            int read = fis.read(buf);

            while (read != -1) {
                os.write(buf, 0, read);

                read = fis.read(buf);
            }
        }
    }

    /**
     * Print particular string.
     *
     * @param str String.
     */
    private static void print(String str) {
        System.out.println(">>> " + str);
    }

    /**
     * Grep task.
     */
    private static class GrepTask extends IgfsTask<String, Collection<Line>> {
        /** {@inheritDoc} */
        @Override public IgfsJob createJob(IgfsPath path, IgfsFileRange range,
            IgfsTaskArgs<String> args) {
            return new GrepJob(args.userArgument());
        }

        /** {@inheritDoc} */
        @Override public Collection<Line> reduce(List<ComputeJobResult> results) {
            Collection<Line> lines = new TreeSet<>(new Comparator<Line>() {
                @Override public int compare(Line line1, Line line2) {
                    return line1.rangePosition() < line2.rangePosition() ? -1 :
                        line1.rangePosition() > line2.rangePosition() ? 1 : line1.lineIndex() - line2.lineIndex();
                }
            });

            for (ComputeJobResult res : results) {
                if (res.getException() != null)
                    throw res.getException();

                Collection<Line> line = res.getData();

                if (line != null)
                    lines.addAll(line);
            }

            return lines;
        }
    }

    /**
     * Grep job.
     */
    private static class GrepJob extends IgfsInputStreamJobAdapter {
        /** Regex string. */
        private final String regex;

        /**
         * Constructor.
         *
         * @param regex Regex string.
         */
        private GrepJob(String regex) {
            this.regex = regex;
        }

        /**  {@inheritDoc} */
        @Override public Object execute(IgniteFileSystem igfs, IgfsRangeInputStream in) throws IgniteException, IOException {
            Collection<Line> res = null;

            long start = in.startOffset();

            try (BufferedReader br = new BufferedReader(new InputStreamReader(in))) {
                int ctr = 0;

                String line = br.readLine();

                while (line != null) {
                    if (line.matches(".*" + regex + ".*")) {
                        if (res == null)
                            res = new HashSet<>();

                        res.add(new Line(start, ctr++, line));
                    }

                    line = br.readLine();
                }
            }

            return res;
        }
    }

    /**
     * Single file line with it's position.
     */
    private static class Line {
        /** Line start position in the file. */
        private long rangePos;

        /** Matching line index within the range. */
        private final int lineIdx;

        /** File line. */
        private String line;

        /**
         * Constructor.
         *
         * @param rangePos Range position.
         * @param lineIdx Matching line index within the range.
         * @param line File line.
         */
        private Line(long rangePos, int lineIdx, String line) {
            this.rangePos = rangePos;
            this.lineIdx = lineIdx;
            this.line = line;
        }

        /**
         * @return Range position.
         */
        public long rangePosition() {
            return rangePos;
        }

        /**
         * @return Matching line index within the range.
         */
        public int lineIndex() {
            return lineIdx;
        }

        /**
         * @return File line.
         */
        public String fileLine() {
            return line;
        }
    }
}