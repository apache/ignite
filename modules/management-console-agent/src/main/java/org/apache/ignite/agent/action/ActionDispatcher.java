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

package org.apache.ignite.agent.action;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.ignite.IgniteAuthenticationException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.agent.ManagementConsoleProcessor;
import org.apache.ignite.agent.dto.action.Request;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.util.future.IgniteFinishedFutureImpl;
import org.apache.ignite.internal.util.future.IgniteFutureImpl;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteFuture;

import static org.apache.ignite.agent.action.annotation.ActionControllerAnnotationReader.actions;

/**
 * Action dispatcher.
 */
public class ActionDispatcher extends GridProcessorAdapter {
    /** Session registry. */
    private SessionRegistry sesRegistry;

    /** Controllers. */
    private final Map<Class, Object> controllers = new ConcurrentHashMap<>();

    /** Thread pool. */
    private final ExecutorService pool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

    /**
     * @param ctx Context.
     */
    public ActionDispatcher(GridKernalContext ctx) {
        super(ctx);

        sesRegistry = ((ManagementConsoleProcessor)ctx.managementConsole()).sessionRegistry();
    }

    /**
     * Find the controller with appropriate method and invoke it.
     *
     * @param req Request.
     * @return Completable future with action result.
     */
    public CompletableFuture<IgniteFuture> dispatch(Request req) {
        String act = req.getAction();

        ActionMethod mtd = actions().get(act);

        if (mtd == null)
            throw new IgniteException("Failed to find action method");

        return CompletableFuture.supplyAsync(() -> handleRequest(mtd, req), pool);
    }

    /**
     *  Find appropriate action for request and invoke it.
     *
     * @param mtd Method.
     * @param req Request.
     */
    private IgniteFuture handleRequest(ActionMethod mtd, Request req) {
        try {
            Class<?> ctrlCls = mtd.controllerClass();

            boolean securityEnabled = ctx.security().enabled();

            boolean authenticationEnabled = ctx.authentication().enabled();

            if (!controllers.containsKey(ctrlCls))
                controllers.put(ctrlCls, ctrlCls.getConstructor(GridKernalContext.class).newInstance(ctx));

            boolean isAuthenticateAct = "SecurityActions.authenticate".equals(mtd.actionName());

            if ((authenticationEnabled || securityEnabled) && !isAuthenticateAct) {
                UUID sesId = req.getSessionId();

                Session ses = sesRegistry.getSession(sesId);

                if (ses == null) {
                    throw new IgniteAuthenticationException(
                        "Failed to authenticate, the session with provided sessionId: " + sesId
                    );
                }

                if (log.isDebugEnabled())
                    log.debug("Received request: [sessionId=" + sesId + ", reqId=" + req.getId() + ']');

                if (ses.securityContext() != null) {
                    try (OperationSecurityContext ignored = ctx.security().withContext(ses.securityContext())) {
                        return invoke(mtd.method(), controllers.get(ctrlCls), req.getArgument());
                    }
                }
            }

            return invoke(mtd.method(), controllers.get(ctrlCls), req.getArgument());
        }
        catch (InvocationTargetException e) {
            return new IgniteFinishedFutureImpl(e.getTargetException());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();

            return new IgniteFinishedFutureImpl(e);
        }
        catch (Exception e) {
            return new IgniteFinishedFutureImpl(e);
        }
    }

    /**
     * Invoke action method.
     *
     * @param mtd Method.
     * @param controller Controller.
     * @param arg Argument.
     */
    @SuppressWarnings("unchecked")
    private IgniteFuture invoke(Method mtd, Object controller, Object arg) throws Exception {
        Object res = arg == null ? mtd.invoke(controller) : mtd.invoke(controller, arg);

        if (res instanceof IgniteFuture)
            return (IgniteFuture) res;

        if (res instanceof Void)
            return new IgniteFinishedFutureImpl();

        if (res instanceof IgniteInternalFuture)
            return new IgniteFutureImpl((IgniteInternalFuture) res);

        return new IgniteFinishedFutureImpl(res);
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) {
        U.shutdownNow(getClass(), pool, log);
    }
}
