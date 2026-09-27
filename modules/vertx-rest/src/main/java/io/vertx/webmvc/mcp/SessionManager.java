package io.vertx.webmvc.mcp;

import io.vertx.ext.web.RoutingContext;
import lombok.Data;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

// 内部类实现
public class SessionManager {

    public static class McpSession {
        private final String sessionId;
        private final Map<String, Object> clientInfo;
        private final long createdAt;
        private boolean confirmed;

        public McpSession(String sessionId, Map<String, Object> clientInfo) {
            this.sessionId = sessionId;
            this.clientInfo = clientInfo;
            this.createdAt = System.currentTimeMillis();
            this.confirmed = false;
        }

        public void confirm() {
            this.confirmed = true;
        }

        public void notifyToolsChanged() {
            // 实现通知逻辑
        }

        public String getSessionId() {
            return sessionId;
        }

        public void close() {

        }
    }

    public static class StreamSession extends McpSession {

        private final RoutingContext ctx;
        private boolean active;

        private String channel;

        public StreamSession(String sessionId, Map<String, Object> clientInfo, RoutingContext ctx) {
            super(sessionId,clientInfo);
            this.ctx = ctx;
            this.active = true;
        }

        @Override
        public void close() {
            this.active = false;
            if (!ctx.response().ended()) {
                ctx.response().end();
            }
        }

        public boolean isActive() {
            return active && !ctx.response().ended();
        }

        public String getChannel() {
            return channel;
        }

        public void setChannel(String channel) {
            this.channel = channel;
        }
    }

    private final Map<String, McpSession> sessions = new ConcurrentHashMap<>();

    public McpSession createSession(String sessionId, Map<String, Object> clientInfo) {
        McpSession session = new McpSession(sessionId, clientInfo);
        sessions.put(sessionId, session);
        return session;
    }

    public void confirmSession(String sessionId) {
        McpSession session = sessions.get(sessionId);
        if (session != null) {
            session.confirm();
        }
    }

    public McpSession getSession(String sessionId) {
        if(sessionId==null) return null;
        return sessions.get(sessionId);
    }

    public StreamSession createStreamSession(String sessionId, Map<String, Object> clientInfo, RoutingContext ctx) {
        StreamSession session = (StreamSession)sessions.get(sessionId);
        if(session==null){
            session = new StreamSession(sessionId, clientInfo, ctx);
            sessions.put(sessionId, session);
        }
        return session;
    }

    public void removeSession(String sessionId) {
        sessions.remove(sessionId);
    }

    public void notifyToolsListChanged() {
        // 通知所有会话工具列表已变更
        for (McpSession session : sessions.values()) {
            session.notifyToolsChanged();
        }
    }

    public void closeAllSessions() {
        sessions.values().forEach(McpSession::close);
        sessions.clear();
    }

    public int getActiveSessionCount() {
        return sessions.size();
    }
}

