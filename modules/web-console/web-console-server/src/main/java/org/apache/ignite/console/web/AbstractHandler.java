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

package org.apache.ignite.console.web;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.console.websocket.WebSocketResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import static org.apache.ignite.console.utils.Utils.extractErrorMessage;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.utils.Utils.toJson;

/**
 * Base class for web sockets handler.
 */
public abstract class AbstractHandler extends TextWebSocketHandler {
    /** */
    private static final Logger log = LoggerFactory.getLogger(AbstractHandler.class);

    /**
     * @param ws Websocket session.
     * @param evt Event.
     */
    protected abstract void handleEvent(WebSocketSession ws, WebSocketRequest evt) throws Exception;

    /** {@inheritDoc} */
    @Override protected void handleTextMessage(WebSocketSession ws, TextMessage msg) {
        try {
            WebSocketRequest evt = fromJson(msg.getPayload(), WebSocketRequest.class);

            handleEvent(ws, evt);
        }
        catch (NullPointerException | IOException e) {
            log.warn("Failed to process message [session=" + ws + ", msg=" + msg + "errMsg=" + e.toString() + "]");
        }
        catch (Throwable e) {
            log.error("Failed to process message [session=" + ws + ", msg=" + msg + "]", e);
        }
    }

    /**
     * @param c Collection of Objects.
     * @param mapper Mapper.
     */
    protected <T, R> Set<R> mapToSet(Collection<T> c, Function<? super T, ? extends R> mapper) {
        return c.stream().map(mapper).collect(Collectors.toSet());
    }

    /**
     * @param ws Session to send message.
     * @param evt Event.
     * @throws IOException If failed to send message.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private void sendMessage(WebSocketSession ws, WebSocketResponse evt) throws IOException {
        synchronized (ws) {
            ws.sendMessage(new TextMessage(toJson(evt)));
        }
    }

    /**
     * @param ws Session to send message.
     * @param reqEvt Event .
     * @throws IOException If failed to send message.
     */
    protected void sendResponse(WebSocketSession ws, WebSocketRequest reqEvt, Object payload) throws IOException {
        sendMessage(ws, reqEvt.withPayload(payload));
    }

    /**
     * @param ws Websocket session.
     * @param evt Event.
     * @param prefix Error message.
     * @param err Error.
     */
    protected void sendError(WebSocketSession ws, WebSocketRequest evt, String prefix, Throwable err) {
        try {
            sendMessage(ws, evt.withError(extractErrorMessage(prefix, err)));
        }
        catch (Throwable e) {
            log.error("Failed to send error in response [session=" + ws + ", event=" + evt + "]", e);
        }
    }
}
