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
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
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
     * Default log file name *
     */
    private static final String DEFAULT_LOG_PATH = "work/log/ignite.log";

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

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<GridRestCommand> supportedCommands() {
        return SUPPORTED_COMMANDS;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IgniteInternalFuture<GridRestResponse> handleAsync(GridRestRequest req) {
        assert req != null;
        if (req.command() == LOG) {
            if (log.isDebugEnabled())
                log.debug("Handling log REST request: " + req);
            GridRestLogRequest req0 = (GridRestLogRequest) req;
            try {
                validateRange(req0);
            } catch (IgniteCheckedException e){
                return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED, e.getMessage()));
            }
            int from = DEFAULT_FROM;
            if (req0.from() != -1) {
                from = req0.from();
            }
            int to = DEFAULT_TO;
            if (req0.to() != -1) {
                to = req0.to();
            }
            String path = DEFAULT_LOG_PATH;
            Path filePath = getPath(req0, path);
            if (from >= to) {
                return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED,
                        "Log file start from cannot be greater than to"));
            }
            if(req0.command() == LOG) {
                try {
                   List content = getContent(from, to, filePath);
                   return new GridFinishedFuture<>(new GridRestResponse(content));
                } catch (IgniteCheckedException e) {
                   return new GridFinishedFuture<>(new GridRestResponse(GridRestResponse.STATUS_FAILED, e.getMessage()));
                }
            }
        }
        return new GridFinishedFuture<>();
    }

    private void validateRange(GridRestLogRequest req0) throws IgniteCheckedException {
        if((req0.from() != -1 && req0.to() == -1)||
                (req0.from() == -1 && req0.to() != -1)){
            throw new IgniteCheckedException("Both from and to need to be provided");
        }
    }

    private Path getPath(GridRestLogRequest req0, String path) {
        Path filePath;
        if (req0.path() != null) {
            path = req0.path();
            filePath = Paths.get(path);
        } else {
            filePath = Paths.get(ctx.config().getIgniteHome() + "/" + path);

        }
        return filePath;
    }

    private List getContent(int from, int to, Path filePath) throws IgniteCheckedException {
        List content = new ArrayList<>();
        try (BufferedReader reader = Files.newBufferedReader(filePath, Charset.defaultCharset())) {
            String line = null;
            int start = from;
            while ((line = reader.readLine()) != null
                    && from < to) {
                if (start >= from) {
                    content.add(line);
                    start++;
                }
                from++;
            }
        } catch (IOException e) {
            throw new IgniteCheckedException(e);
        }
        return content;
    }
}
