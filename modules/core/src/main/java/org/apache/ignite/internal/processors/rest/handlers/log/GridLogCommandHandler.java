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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.InvalidPathException;
import java.util.Collection;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.rest.GridRestCommand;
import org.apache.ignite.internal.processors.rest.GridRestResponse;
import org.apache.ignite.internal.processors.rest.handlers.GridRestCommandHandlerAdapter;
import org.apache.ignite.internal.processors.rest.request.GridRestLogRequest;
import org.apache.ignite.internal.processors.rest.request.GridRestRequest;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.typedef.internal.U;

import static org.apache.ignite.internal.processors.rest.GridRestCommand.LOG;

/**
 * Handler for {@link org.apache.ignite.internal.processors.rest.GridRestCommand#LOG} command.
 */
public class GridLogCommandHandler extends GridRestCommandHandlerAdapter {
    /**
     * Supported commands.
     */
    private static final Collection<GridRestCommand> SUPPORTED_COMMANDS = U.sealList(LOG);

    /**
     * Default log file start line number *
     */
    private static final int DEFAULT_FROM = 0;

    /**
     * Default log file end line number*
     */
    private static final int DEFAULT_TO = 1;

    /**
     * @param ctx Context.
     */
    public GridLogCommandHandler(GridKernalContext ctx) {
        super(ctx);
    }

    /** {@inheritDoc} */
    @Override public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req != null;

        if (req.command() == LOG) {
            if (log.isDebugEnabled())
                log.debug("Handling log REST request: " + req);

            GridRestLogRequest req0 = (GridRestLogRequest)req;

            if (req0.from() < -1 || req0.to() < -1)
                return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED,
                    "One of the request parameters is invalid [from=" + req0.from() + ", to=" + req0.to() + ']'));

            int from;

            if (req0.from() != -1) {
                if (req0.to() == -1)
                    return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED,
                        "Request parameter 'to' is not set."));

                from = req0.from();
            }
            else
                from = DEFAULT_FROM;

            int to;

            if (req0.to() != -1) {
                if (req0.from() == -1)
                    return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED,
                        "Request parameter 'from' is not set."));

                to = req0.to();
            }
            else
                to = DEFAULT_TO;

            if (from >= to)
                return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED,
                    "Request parameter 'from' must be less than 'to'."));

            File logFile;

            try {
                if (req0.path() != null) {
                    if (log.fileName() != null) {
                        if (!req0.path().equals(log.fileName())) {
                            return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED,
                                "Request parameter 'path' must contain a path to valid log file."));
                        }
                        else
                            logFile = new File(req0.path());
                    }
                    else if (req0.path().startsWith(ctx.config().getIgniteHome()))
                        logFile = new File(req0.path());
                    else {
                        return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED,
                            "Request parameter 'path' must contain a path to valid log file."));
                    }
                }
                else if (log.fileName() == null)
                    logFile = new File(ctx.config().getIgniteHome() + "/work/log/ignite.log");
                else
                    logFile = new File(log.fileName());
            }
            catch (InvalidPathException e) {
                return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED,
                    "Incorrect path to a log file [msg=" + e.getMessage() + ']'));
            }

            try {
                String content = readLog(from, to, logFile);

                return new GridFinishedFuture<>(new GridRestResponse(content));
            }
            catch (IgniteCheckedException e) {
                return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED, e.getMessage()));
            }
        }

        return new GridFinishedFuture<>();
    }

    /**
     * Reads content from a log file.
     *
     * @param from Start position.
     * @param to End position.
     * @param logFile Log file.
     * @return Content that is read.
     * @throws IgniteCheckedException If failed.
     */
    private String readLog(int from, int to, File logFile) throws IgniteCheckedException {
        StringBuilder content = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(new FileReader(logFile))) {
            String line;

            int start = 0;

            while (start <= to && (line = reader.readLine()) != null) {
                if (start >= from)
                    content.append(line);

                start++;
            }

            if (content.length() == 0)
                throw new IgniteCheckedException("Request parameter 'from' and 'to' are for lines that " +
                    "do not exist in log file.");
        }
        catch (IOException e) {
            throw new IgniteCheckedException(e);
        }

        return content.toString();
    }
}
