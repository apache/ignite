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

package org.apache.ignite.internal.processors.rest.handlers.log;

import org.apache.ignite.*;
import org.apache.ignite.internal.*;
import org.apache.ignite.internal.processors.rest.*;
import org.apache.ignite.internal.processors.rest.handlers.*;
import org.apache.ignite.internal.processors.rest.request.*;
import org.apache.ignite.internal.util.future.*;
import org.apache.ignite.internal.util.io.*;
import org.apache.ignite.internal.util.typedef.*;
import org.apache.ignite.internal.util.typedef.internal.*;

import java.io.*;
import java.net.*;
import java.util.*;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.*;

/**
 * Handler for {@link GridRestCommand#LOG} command.
 */
public class GridLogCommandHandler extends GridRestCommandHandlerAdapter {
    /** Supported commands. */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(LOG);

    /** Default log path. */
    private static final String DFLT_PATH = "work/log/ignite.log";

    /** Approximate line length. */
    private static final int LINE_LENGTH = 120;

    /** Folders accessible for log reading. */
    private List<File> accessibleFolders;

    /** @param ctx Context. */
    public GridLogCommandHandler(GridKernalContext ctx) {
        super(ctx);

        assert ctx.config().getClientConnectionConfiguration() != null;

        String[] accessiblePaths = ctx.config().getClientConnectionConfiguration().getRestAccessibleFolders();

        if (accessiblePaths == null) {
            String ggHome = U.getGridGainHome();

            if (ggHome != null)
                accessiblePaths = new String[] {ggHome};
        }

        if (accessiblePaths != null) {
            accessibleFolders = new ArrayList<>();

            for (String accessiblePath : accessiblePaths)
                accessibleFolders.add(new File(accessiblePath));
        }
        else if (log.isDebugEnabled())
            log.debug("Neither restAccessibleFolders nor IGNITE_HOME properties are not set, will not restrict " +
                "log files access");
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req instanceof GridRestLogRequest : "Invalid command for topology handler: " + req;

        assert SUPPORTED_COMMANDS.contains(req.command());

        GridRestLogRequest req0 = (GridRestLogRequest) req;

        String path = req0.path();

        int from = req0.from();
        int to = req0.to();

        if (path == null)
            path = DFLT_PATH;

        try {
            return new GridFinishedFuture<>(ctx, new GridRestResponse(readLog(path, from, to)));
        }
        catch (IgniteCheckedException e) {
            return new GridFinishedFuture<>(ctx, e);
        }
        catch (IOException e) {
            return new GridFinishedFuture<>(ctx, e);
        }
    }

    /**
     * Reads log.
     *
     * @param path Path where read log located.
     * @param from Number of line to start from.
     * @param to Number tof line to finish on.
     * @return List of read lines.
     * @throws IgniteCheckedException If argumets are illegal.
     * @throws IOException If file couldn't be accessed or read failed.
     */
    private List<String> readLog(String path, int from, int to) throws IgniteCheckedException, IOException {
        URL url = U.resolveGridGainUrl(path);

        if (url == null)
            throw new IgniteCheckedException("Log file not found: " + path);

        if (!isAccessible(url))
            throw new IgniteCheckedException("File is not accessible through REST" +
                " (check restAccessibleFolders configuration property): " + path);

        if (from >= 0 && to >= 0)
            return readLinesForward(url, from, to);
        else if (from < 0 && to < 0)
            return readLinesBackward(url, from, to);
        else
            throw new IgniteCheckedException(
                "Illegal arguments (both should be positive or negative) [from=" + from + ", to=" + to + ']');
    }

    /**
     * Read lines from log backwards.
     *
     * @param url URL of the log.
     * @param from Number of line to start from. Should be negative, representing number of line from the end.
     * @param to Number tof line to finish on. Should be negative, representing number of line from the end.
     * @return List of read lines.
     * @throws IgniteCheckedException If arguments are illegal.
     * @throws IOException If file couldn't be accessed or read failed.
     */
    @SuppressWarnings("TooBroadScope")
    private List<String> readLinesBackward(URL url, final int from, final int to) throws IgniteCheckedException, IOException {
        File file = new File(url.getFile());

        if (!file.exists() || !file.isFile())
            throw new IgniteCheckedException("File doesn't exists: " + url);

        int linesToRead = to - from + 1;
        int linesRead = 0;

        if (linesToRead <= 0)
            return Collections.emptyList();

        LinkedList<String> lines = new LinkedList<>();

        RandomAccessFile raf = null;

        try {
            raf = new RandomAccessFile(file, "r");

            byte[] buf = new byte[Math.min(16 * 1024, linesToRead * LINE_LENGTH)];

            long endPos = raf.length();

            String lastLineEnding = null;

            do {
                long startPos = endPos - buf.length;

                if (startPos < 0)
                    startPos = 0;

                raf.seek(startPos);

                // Limiting number of bytes read to protect from line duplication near file start,
                int bytesRead = raf.read(buf, 0, (int)(endPos - startPos));

                Scanner rdr = new Scanner(new GridByteArrayInputStream(buf, 0, bytesRead));

                // Read lines into temporary, forward ordered collection.
                List<String> tmpLines = new LinkedList<>();

                boolean firstLine = true;

                // Temporary variable to keep a new lastLineEnding value
                // while old is still required.
                String fst = null;

                while (rdr.hasNextLine()) {
                    String line = rdr.nextLine();

                    // Skip the first line as it could be incomplete.
                    if (firstLine) {
                        firstLine = false;

                        // If we started from the beginning add it.
                        if (startPos > 0)
                            fst = lastLineEnding != null && !rdr.hasNextLine() ? line + lastLineEnding : line;
                        else
                            tmpLines.add(lastLineEnding != null ? line + lastLineEnding : line);
                    }
                    else if (rdr.hasNextLine())
                        // If it's a last line in buffer add previously read part.
                        tmpLines.add(line);
                    else
                        tmpLines.add(lastLineEnding != null ? line + lastLineEnding : line);
                }

                lastLineEnding = fst;

                // Limit next read to end of the first line.
                endPos = startPos;

                // Save lines, if they are requested, in backward order into result collection.
                for (ListIterator<String> it = tmpLines.listIterator(tmpLines.size()); it.hasPrevious(); ) {
                    linesRead++;

                    String prev = it.previous();

                    if ((linesRead >= -to) && (linesRead <= -from))
                        lines.addFirst(prev);
                }
            } while (linesRead < -from && endPos > 0);
        }
        finally {
            U.close(raf, log);
        }

        return lines;
    }

    /**
     * Reads log forward, using {@link Reader} API.
     *
     * @param url URL of the log file.
     * @param from Number of line to start from.
     * @param to Number tof line to finish on.
     * @return List of read lines.
     * @throws IOException If file couldn't be accessed or read failed.
     */
    private List<String> readLinesForward(URL url, int from, int to) throws IOException {
        BufferedReader reader = null;

        try {
            reader = new BufferedReader(new InputStreamReader(url.openStream()));

            List<String> lines = new LinkedList<>();

            String line;

            int i = 0;

            while ((line = reader.readLine()) != null) {
                i++;

                if (from != -1 && i - 1 < from)
                    continue;

                if (to != -1 && i - 1 > to)
                    break;

                lines.add(line);
            }

            return lines;
        }
        finally {
            U.close(reader, log);
        }
    }

    /**
     * Checks whether given url is accessible against configuration.
     *
     * @param url URL to check.
     * @return {@code True} if file is accessible (i.e. located in one of the sub-folders of
     *      {@code restAccessibleFolders} list.
     */
    private boolean isAccessible(URL url) throws IOException {
        // No check is made if configuration is undefined.
        if (accessibleFolders == null)
            return true;

        File f = new File(url.getFile());

        f = f.getCanonicalFile();

        do {
            if (F.contains(accessibleFolders, f))
                return true;

            f = f.getParentFile();
        }
        while (f != null);

        return false;
    }
}
