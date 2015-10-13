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

package org.apache.ignite.agent.remote;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.http.auth.AuthenticationException;

/**
 * Allow to execute methods remotely from NodeJS server by web-socket command.
 */
public class RemoteHandler implements AutoCloseable {
    /** */
    public static final Gson GSON = new Gson();
    /** */
    public static final Object[] EMPTY_OBJECTS = new Object[0];
    /** */
    private static final Logger log = Logger.getLogger(RemoteHandler.class.getName());
    /** */
    private static final String INTERNAL_EXCEPTION_TYPE = "org.apache.ignite.agent.AgentException";
    /** */
    private final WebSocketSender snd;

    /** */
    private final Map<String, MethodDescriptor> mtds = new HashMap<>();

    /** */
    private final ExecutorService executorSrvc = Executors.newFixedThreadPool(Runtime.getRuntime()
        .availableProcessors());

    /**
     * @param snd Session.
     * @param hnds Handlers.
     */
    private RemoteHandler(WebSocketSender snd, Object ... hnds) {
        this.snd = snd;

        for (Object hnd : hnds) {
            for (Method method : hnd.getClass().getMethods()) {
                Remote remoteAnn = method.getAnnotation(Remote.class);

                if (remoteAnn != null) {
                    MethodDescriptor old = mtds.put(method.getName(), new MethodDescriptor(method, hnd,
                        remoteAnn.async()));

                    if (old != null)
                        throw new IllegalArgumentException("Duplicated method: " + method.getName());
                }
            }
        }
    }

    /**
     * @param hnds Handler.
     * @param snd Sender.
     */
    public static RemoteHandler wrap(WebSocketSender snd, Object ... hnds) {
        return new RemoteHandler(snd, hnds);
    }

    /**
     * @param req Request.
     */
    public void onMessage(JsonObject req) {
        log.log(Level.FINE, "Message: " + req);

        JsonPrimitive reqIdJson = req.getAsJsonPrimitive("reqId");

        final Long reqId = reqIdJson == null ? null : reqIdJson.getAsLong();

        String mtdName = req.getAsJsonPrimitive("mtdName").getAsString();

        final MethodDescriptor desc = mtds.get(mtdName);

        if (desc == null) {
            sendException(reqId, INTERNAL_EXCEPTION_TYPE, "Unknown method: " + mtdName);

            return;
        }

        Type[] paramTypes = desc.mtd.getGenericParameterTypes();

        JsonArray argsJson = req.getAsJsonArray("args");

        final Object[] args;

        if (paramTypes.length > 0) {
            args = new Object[paramTypes.length];

            if (argsJson == null || argsJson.size() != paramTypes.length) {
                sendException(reqId, INTERNAL_EXCEPTION_TYPE, "Inconsistent parameters");

                return;
            }

            for (int i = 0; i < paramTypes.length; i++)
                args[i] = GSON.fromJson(argsJson.get(i), paramTypes[i]);
        }
        else {
            args = EMPTY_OBJECTS;

            if (argsJson != null && argsJson.size() > 0) {
                sendException(reqId, INTERNAL_EXCEPTION_TYPE, "Inconsistent parameters");

                return;
            }
        }

        Runnable run = new Runnable() {
            @Override public void run() {
                final Object res;

                try {
                    res = desc.mtd.invoke(desc.hnd, args);
                }
                catch (Throwable e) {
                    if (e instanceof AuthenticationException) {
                        close();

                        return;
                    }

                    if (e instanceof InvocationTargetException)
                        e = ((InvocationTargetException)e).getTargetException();

                    if (reqId != null)
                        sendException(reqId, e.getClass().getName(), e.getMessage());
                    else
                        log.log(Level.SEVERE, "Exception on execute remote method.", e);

                    return;
                }

                sendResponse(reqId, res, desc.returnType);
            }
        };

        if (desc.async)
            executorSrvc.submit(run);
        else
            run.run();
    }

    /**
     * @param reqId Request id.
     * @param exType Exception class name.
     * @param exMsg Exception message.
     */
    protected void sendException(Long reqId, String exType, String exMsg) {
        if (reqId == null)
            return;

        JsonObject res = new JsonObject();

        res.addProperty("type", "CallRes");
        res.addProperty("reqId", reqId);

        JsonObject exJson = new JsonObject();
        exJson.addProperty("type", exType);
        exJson.addProperty("message", exMsg);

        res.add("ex", exJson);

        snd.send(res);
    }

    /**
     * @param reqId Request id.
     * @param res Result.
     * @param type Type.
     */
    private void sendResponse(Long reqId, Object res, Type type) {
        if (reqId == null)
            return;

        JsonObject resp = new JsonObject();

        resp.addProperty("type", "CallRes");

        resp.addProperty("reqId", reqId);

        JsonElement resJson = type == void.class ? JsonNull.INSTANCE : GSON.toJsonTree(res, type);

        resp.add("res", resJson);

        snd.send(resp);
    }

    /** {@inheritDoc} */
    @Override public void close() {
        executorSrvc.shutdown();
    }

    /**
     *
     */
    private static class MethodDescriptor {
        /** */
        private final Method mtd;

        /** */
        private final Object hnd;

        /** */
        private final Type returnType;

        /** */
        private final boolean async;

        /**
         * @param mtd Method.
         * @param hnd Handler.
         * @param async Async.
         */
        MethodDescriptor(Method mtd, Object hnd, boolean async) {
            this.mtd = mtd;
            this.hnd = hnd;
            this.async = async;

            returnType = mtd.getGenericReturnType();
        }
    }
}
