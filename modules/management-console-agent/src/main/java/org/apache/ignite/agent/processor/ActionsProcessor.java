/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 *
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.agent.processor;

import java.util.UUID;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.agent.action.ActionDispatcher;
import org.apache.ignite.agent.dto.action.InvalidRequest;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.agent.dto.action.Response;
import org.apache.ignite.agent.dto.action.ResponseError;
import org.apache.ignite.agent.ws.WebSocketManager;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.authentication.IgniteAccessControlException;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.lang.IgniteFuture;
import org.apache.ignite.plugin.security.SecurityException;

import static org.apache.ignite.agent.StompDestinationsUtils.buildActionResponseDest;
import static org.apache.ignite.agent.dto.action.ActionStatus.COMPLETED;
import static org.apache.ignite.agent.dto.action.ActionStatus.FAILED;
import static org.apache.ignite.agent.dto.action.ActionStatus.RUNNING;
import static org.apache.ignite.agent.dto.action.ResponseError.AUTHENTICATION_ERROR_CODE;
import static org.apache.ignite.agent.dto.action.ResponseError.AUTHORIZE_ERROR_CODE;
import static org.apache.ignite.agent.dto.action.ResponseError.INTERNAL_ERROR_CODE;
import static org.apache.ignite.agent.dto.action.ResponseError.PARSE_ERROR_CODE;
import static org.apache.ignite.agent.utils.AgentUtils.quiteStop;

/**
 * Actions processor.
 */
public class ActionsProcessor extends GridProcessorAdapter {
    /** Manager. */
    private final WebSocketManager mgr;

    /** Action dispatcher. */
    private final ActionDispatcher dispatcher;

    /**
     * @param ctx Context.
     * @param mgr Manager.
     */
    public ActionsProcessor(GridKernalContext ctx, WebSocketManager mgr) {
        super(ctx);

        this.mgr = mgr;
        this.dispatcher = new ActionDispatcher(ctx);
    }

    /**
     * Handle action requests from Management Console.
     *
     * @param req Request.
     */
    public void onActionRequest(Request req) {
        // Deserialization error occurred.
        if (req instanceof InvalidRequest) {
            Throwable ex = ((InvalidRequest) req).getCause();

            sendResponse(
                new Response()
                    .setId(req.getId())
                    .setStatus(FAILED)
                    .setError(new ResponseError(PARSE_ERROR_CODE, ex.getMessage(), ex.getStackTrace()))
            );

            return;
        }

        try {
            sendResponse(new Response().setId(req.getId()).setStatus(RUNNING));

            dispatcher.dispatch(req)
                    .thenApply(IgniteFuture::get)
                    .thenApply(r -> new Response().setId(req.getId()).setStatus(COMPLETED).setResult(r))
                    .exceptionally(e -> convertToErrorResponse(req.getId(), e.getCause()))
                    .thenAccept(this::sendResponse);
        }
        catch (Exception e) {
            sendResponse(convertToErrorResponse(req.getId(), e));
        }
    }

    /**
     * @param id Id.
     * @param e Throwable.
     */
    private Response convertToErrorResponse(UUID id, Throwable e) {
        log.error("Failed to execute action, send error response to Management Console: [reqId=" + id + ']', e);

        return new Response()
                .setId(id)
                .setStatus(FAILED)
                .setError(new ResponseError(getErrorCode(e), e.getMessage(), e.getStackTrace()));
    }

    /**
     * @param e Exception.
     * @return Integer error code.
     */
    private int getErrorCode(Throwable e) {
        if (e instanceof SecurityException || X.hasCause(e, SecurityException.class))
            return AUTHORIZE_ERROR_CODE;
        else if (e instanceof IgniteAuthenticationException || e instanceof IgniteAccessControlException
            || X.hasCause(e, IgniteAuthenticationException.class, IgniteAccessControlException.class)
        )
            return AUTHENTICATION_ERROR_CODE;

        return INTERNAL_ERROR_CODE;
    }

    /**
     * Send action response to Management Console.
     *
     * @param res Response.
     */
    private void sendResponse(Response res) {
        UUID clusterId = ctx.cluster().get().id();

        mgr.send(buildActionResponseDest(clusterId, res.getId()), res);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        quiteStop(dispatcher);
    }
}
