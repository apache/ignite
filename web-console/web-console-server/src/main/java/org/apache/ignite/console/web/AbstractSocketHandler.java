

package org.apache.ignite.console.web;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.ignite.console.messages.WebConsoleMessageSource;
import org.apache.ignite.console.messages.WebConsoleMessageSourceAccessor;

import org.apache.ignite.console.websocket.WebSocketEvent;
import org.apache.ignite.console.websocket.WebSocketRequest;
import org.apache.ignite.console.websocket.WebSocketResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.socket.CloseStatus;
import org.springframework.web.socket.PingMessage;
import org.springframework.web.socket.TextMessage;
import org.springframework.web.socket.WebSocketSession;
import org.springframework.web.socket.handler.TextWebSocketHandler;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.ignite.console.utils.Utils.fromJson;
import static org.apache.ignite.console.utils.Utils.toJson;

/**
 * Base class for web sockets handler.
 */
public abstract class AbstractSocketHandler extends TextWebSocketHandler {
    /** */
    private static final Logger log = LoggerFactory.getLogger(AbstractSocketHandler.class);

    /** */
    private static final PingMessage PING = new PingMessage(UTF_8.encode("PING"));

    /** Messages accessor. */
    protected WebConsoleMessageSourceAccessor messages = WebConsoleMessageSource.getAccessor();

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
     * @param req Event .
     * @throws IOException If failed to send message.
     */
    protected void sendResponse(WebSocketSession ws, WebSocketRequest req, Object payload) throws IOException {
        sendMessage(ws, req.response(payload));
    }

    /**
     * @param ws Session to ping.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    protected void ping(WebSocketSession ws) {
        synchronized (ws) {
            try {
                ws.sendMessage(PING);
            }
            catch (Throwable e) {
                log.error("Failed to send PING request [session=" + ws + "]");

                try {
                    ws.close(CloseStatus.SESSION_NOT_RELIABLE);
                }
                catch (IOException ignored) {
                    // No-op.
                }
            }
        }
    }

    /**
     * @param ses Session to send message.
     * @param evt Event.
     * @throws IOException If failed to send message.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    public void sendMessage(WebSocketSession ses, WebSocketEvent evt) throws IOException {
        synchronized (ses) {
            ses.sendMessage(new TextMessage(toJson(evt)));
        }
    }

    /**
     * @param ses Session to send message.
     * @param evt Event.
     */
    public void sendMessageQuiet(WebSocketSession ses, WebSocketEvent evt) {
        try {
            sendMessage(ses, evt);
        }
        catch (Throwable e) {
            log.error("Failed to send event [session=" + ses + ", event=" + evt + "]", e);
        }
    }
}
