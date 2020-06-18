package de.bwaldvogel.mongo.wire;

import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.bwaldvogel.mongo.MongoBackend;
import de.bwaldvogel.mongo.backend.Utils;
import de.bwaldvogel.mongo.bson.Document;
import de.bwaldvogel.mongo.exception.MongoServerError;
import de.bwaldvogel.mongo.exception.MongoServerException;
import de.bwaldvogel.mongo.exception.NoSuchCommandException;
import de.bwaldvogel.mongo.wire.message.ClientRequest;
import de.bwaldvogel.mongo.wire.message.MessageHeader;
import de.bwaldvogel.mongo.wire.message.MongoDelete;
import de.bwaldvogel.mongo.wire.message.MongoInsert;
import de.bwaldvogel.mongo.wire.message.MongoQuery;
import de.bwaldvogel.mongo.wire.message.MongoReply;
import de.bwaldvogel.mongo.wire.message.MongoUpdate;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.group.ChannelGroup;

public class MongoDatabaseHandler extends SimpleChannelInboundHandler<ClientRequest> {

    private static final Logger log = LoggerFactory.getLogger(MongoWireProtocolHandler.class);

    private final AtomicInteger idSequence = new AtomicInteger();
    private final MongoBackend mongoBackend;

    private final ChannelGroup channelGroup;
    private final long started;

    public MongoDatabaseHandler(MongoBackend mongoBackend, ChannelGroup channelGroup) {
        this.channelGroup = channelGroup;
        this.mongoBackend = mongoBackend;
        this.started = System.nanoTime();
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        channelGroup.add(ctx.channel());
        log.info("client {} connected", ctx.channel());
        super.channelActive(ctx);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        log.info("channel {} closed", ctx.channel());
        channelGroup.remove(ctx.channel());
        mongoBackend.handleClose(ctx.channel());
        super.channelInactive(ctx);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, ClientRequest object) throws Exception {
        if (object instanceof MongoQuery) {
            MongoQuery mongoQuery = (MongoQuery) object;
            ctx.channel().writeAndFlush(handleQuery(mongoQuery));
        } else if (object instanceof MongoInsert) {
            MongoInsert insert = (MongoInsert) object;
            mongoBackend.handleInsert(insert);
        } else if (object instanceof MongoDelete) {
            MongoDelete delete = (MongoDelete) object;
            mongoBackend.handleDelete(delete);
        } else if (object instanceof MongoUpdate) {
            MongoUpdate update = (MongoUpdate) object;
            mongoBackend.handleUpdate(update);
        } else {
            throw new MongoServerException("unknown message: " + object);
        }
    }

    private MongoReply handleQuery(MongoQuery query) {
        MessageHeader header = new MessageHeader(idSequence.incrementAndGet(), query.getHeader().getRequestID());
        try {
            List<Document> documents = new ArrayList<>();
            if (query.getCollectionName().startsWith("$cmd")) {
                documents.add(handleCommand(query));
            } else {
                for (Document obj : mongoBackend.handleQuery(query)) {
                    documents.add(obj);
                }
            }
            return new MongoReply(header, documents);
        } catch (NoSuchCommandException e) {
            log.error("unknown command: {}", query, e);
            Map<String, ?> additionalInfo = Collections.singletonMap("bad cmd", query.getQuery());
            return queryFailure(header, e, additionalInfo);
        } catch (MongoServerException e) {
            if (e.isLogError()) {
                log.error("failed to handle query {}", query, e);
            }
            return queryFailure(header, e);
        }
    }

    private MongoReply queryFailure(MessageHeader header, MongoServerException exception) {
        Map<String, ?> additionalInfo = Collections.emptyMap();
        return queryFailure(header, exception, additionalInfo);
    }

    private MongoReply queryFailure(MessageHeader header, MongoServerException exception, Map<String, ?> additionalInfo) {
        Document obj = new Document();
        obj.put("$err", exception.getMessageWithoutErrorCode());
        obj.put("errmsg", exception.getMessageWithoutErrorCode());
        if (exception instanceof MongoServerError) {
            MongoServerError error = (MongoServerError) exception;
            obj.put("code", error.getCode());
            obj.putIfNotNull("codeName", error.getCodeName());
        }
        obj.putAll(additionalInfo);
        obj.put("ok", Integer.valueOf(0));
        return new MongoReply(header, obj, ReplyFlag.QUERY_FAILURE);
    }

    Document handleCommand(MongoQuery query) {
        String collectionName = query.getCollectionName();
        if (collectionName.equals("$cmd.sys.inprog")) {
            Collection<Document> currentOperations = mongoBackend.getCurrentOperations(query);
            return new Document("inprog", currentOperations);
        }

        if (collectionName.equals("$cmd")) {
            String command = query.getQuery().keySet().iterator().next();

            switch (command) {
                case "serverStatus":
                    return getServerStatus();
                case "ping":
                    Document response = new Document();
                    Utils.markOkay(response);
                    return response;
                default:
                    Document actualQuery = query.getQuery();

                    if (command.equals("$query")) {
                        command = ((Document)query.getQuery().get("$query")).keySet().iterator().next();
                        actualQuery = (Document)actualQuery.get("$query");
                    }

                    return mongoBackend.handleCommand(query.getChannel(), query.getDatabaseName(), command, actualQuery);
            }
        }

        throw new MongoServerException("unknown collection: " + collectionName);
    }

    private Document getServerStatus() {
        Document serverStatus = new Document();
        try {
            serverStatus.put("host", InetAddress.getLocalHost().getHostName());
        } catch (UnknownHostException e) {
            throw new MongoServerException("failed to get hostname", e);
        }
        serverStatus.put("version", Utils.join(mongoBackend.getVersion(), "."));
        serverStatus.put("process", "java");
        serverStatus.put("pid", getProcessId());

        serverStatus.put("uptime", Integer.valueOf((int) TimeUnit.NANOSECONDS.toSeconds(System.nanoTime() - started)));
        serverStatus.put("uptimeMillis", Long.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started)));
        serverStatus.put("localTime", Instant.now(mongoBackend.getClock()));

        Document connections = new Document();
        connections.put("current", Integer.valueOf(channelGroup.size()));

        serverStatus.put("connections", connections);

        Document cursors = new Document();
        cursors.put("totalOpen", Integer.valueOf(0)); // TODO

        serverStatus.put("cursors", cursors);

        Utils.markOkay(serverStatus);

        return serverStatus;
    }

    private Integer getProcessId() {
        String runtimeName = ManagementFactory.getRuntimeMXBean().getName();
        if (runtimeName.contains("@")) {
            return Integer.valueOf(runtimeName.substring(0, runtimeName.indexOf('@')));
        }
        return Integer.valueOf(0);
    }
}
