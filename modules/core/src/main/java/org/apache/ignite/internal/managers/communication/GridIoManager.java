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

package org.apache.ignite.internal.managers.communication;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.channels.Channel;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.WritableByteChannel;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BooleanSupplier;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteMessaging;
import org.apache.ignite.cluster.ClusterGroup;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridTopic;
import org.apache.ignite.internal.IgniteClientDisconnectedCheckedException;
import org.apache.ignite.internal.IgniteComponentType;
import org.apache.ignite.internal.IgniteDeploymentCheckedException;
import org.apache.ignite.internal.IgniteFeatures;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.NodeStoppingException;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.direct.DirectMessageReader;
import org.apache.ignite.internal.direct.DirectMessageWriter;
import org.apache.ignite.internal.managers.GridManagerAdapter;
import org.apache.ignite.internal.managers.deployment.GridDeployment;
import org.apache.ignite.internal.managers.discovery.CustomEventListener;
import org.apache.ignite.internal.managers.eventstorage.GridEventStorageManager;
import org.apache.ignite.internal.managers.eventstorage.GridLocalEventListener;
import org.apache.ignite.internal.processors.cache.mvcc.msg.MvccMessage;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.FileIOFactory;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIO;
import org.apache.ignite.internal.processors.cache.persistence.file.RandomAccessFileIOFactory;
import org.apache.ignite.internal.processors.metric.MetricRegistry;
import org.apache.ignite.internal.processors.platform.message.PlatformMessageFilter;
import org.apache.ignite.internal.processors.pool.PoolProcessor;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.internal.processors.timeout.GridTimeoutObject;
import org.apache.ignite.internal.processors.tracing.MTC;
import org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;
import org.apache.ignite.internal.processors.tracing.Span;
import org.apache.ignite.internal.processors.tracing.SpanTags;
import org.apache.ignite.internal.util.GridBoundedConcurrentLinkedHashSet;
import org.apache.ignite.internal.util.StripedCompositeReadWriteLock;
import org.apache.ignite.internal.util.future.GridFinishedFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;
import org.apache.ignite.internal.util.lang.IgnitePair;
import org.apache.ignite.internal.util.tostring.GridToStringExclude;
import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.internal.util.typedef.internal.LT;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.lang.IgniteInClosure;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.lang.IgniteUuid;
import org.apache.ignite.marshaller.Marshaller;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageFactory;
import org.apache.ignite.plugin.extensions.communication.MessageFormatter;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.apache.ignite.spi.IgniteSpiException;
import org.apache.ignite.spi.communication.CommunicationListener;
import org.apache.ignite.spi.communication.CommunicationSpi;
import org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi;
import org.apache.ignite.spi.communication.tcp.internal.CommunicationListenerEx;
import org.apache.ignite.spi.communication.tcp.internal.ConnectionRequestor;
import org.apache.ignite.spi.communication.tcp.internal.TcpConnectionRequestDiscoveryMessage;
import org.apache.ignite.spi.communication.tcp.internal.TcpInverseConnectionResponseMessage;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.GridTopic.TOPIC_CACHE_COORDINATOR;
import static org.apache.ignite.internal.GridTopic.TOPIC_COMM_SYSTEM;
import static org.apache.ignite.internal.GridTopic.TOPIC_COMM_USER;
import static org.apache.ignite.internal.GridTopic.TOPIC_IO_TEST;
import static org.apache.ignite.internal.IgniteFeatures.CHANNEL_COMMUNICATION;
import static org.apache.ignite.internal.IgniteFeatures.nodeSupports;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.AFFINITY_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.DATA_STREAMER_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.IDX_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.MANAGEMENT_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.P2P_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.PUBLIC_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.QUERY_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SCHEMA_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SERVICE_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.SYSTEM_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.UTILITY_CACHE_POOL;
import static org.apache.ignite.internal.managers.communication.GridIoPolicy.isReservedGridIoPolicy;
import static org.apache.ignite.internal.processors.metric.impl.MetricUtils.metricName;
import static org.apache.ignite.internal.processors.tracing.MTC.support;
import static org.apache.ignite.internal.processors.tracing.SpanType.COMMUNICATION_ORDERED_PROCESS;
import static org.apache.ignite.internal.processors.tracing.SpanType.COMMUNICATION_REGULAR_PROCESS;
import static org.apache.ignite.internal.processors.tracing.messages.TraceableMessagesTable.traceName;
import static org.apache.ignite.internal.util.nio.GridNioBackPressureControl.threadProcessingMessage;
import static org.apache.ignite.spi.communication.tcp.TcpCommunicationSpi.ATTR_PAIRED_CONN;
import static org.jsr166.ConcurrentLinkedHashMap.QueuePolicy.PER_SEGMENT_Q_OPTIMIZED_RMV;

/**
 * This class represents the internal grid communication (<em>input</em> and <em>output</em>) manager
 * which is placed as a layer of indirection between the {@link IgniteKernal} and {@link CommunicationSpi}.
 * The IO manager is responsible for controlling CommunicationSPI which in turn is responsible
 * for exchanging data between Ignite nodes.
 *
 * <h2>Data exchanging</h2>
 * <p>
 * Communication manager provides a rich API for data exchanging between a pair of cluster nodes. Two types
 * of communication <em>Message-based communication</em> and <em>File-based communication</em> are available.
 * Each of them support sending data to an arbitrary topic on the remote node (see {@link GridTopic} for an
 * additional information about Ignite topics).
 *
 * <h3>Message-based communication</h3>
 * <p>
 * {@link Message} and {@link GridTopic} are used to provide a topic-based messaging protocol
 * between cluster nodes. All of messages used for data exchanging can be devided into two general types:
 * <em>internal</em> and <em>user</em> messages.
 * <p>
 * <em>Internal message</em> communication is used by Ignite Kernal. Please, refer to appropriate methods below:
 * <ul>
 * <li>{@link #sendToGridTopic(ClusterNode, GridTopic, Message, byte)}</li>
 * <li>{@link #sendOrderedMessage(ClusterNode, Object, Message, byte, long, boolean)}</li>
 * <li>{@link #addMessageListener(Object, GridMessageListener)}</li>
 * </ul>
 * <p>
 * <em>User message</em> communication is directly exposed to the {@link IgniteMessaging} API and provides
 * for user functionality for topic-based message exchanging among nodes within the cluser defined
 * by {@link ClusterGroup}. Please, refer to appropriate methods below:
 * <ul>
 * <li>{@link #sendToCustomTopic(ClusterNode, Object, Message, byte)}</li>
 * <li>{@link #addUserMessageListener(Object, IgniteBiPredicate, UUID)}</li>
 * </ul>
 *
 * <h3>File-based communication</h3>
 * <p>
 * Sending or receiving binary data (represented by a <em>File</em>) over a <em>SocketChannel</em> is only
 * possible when the build-in {@link TcpCommunicationSpi} implementation of Communication SPI is used and
 * both local and remote nodes are {@link IgniteFeatures#CHANNEL_COMMUNICATION CHANNEL_COMMUNICATION} feature
 * support. To ensue that the remote node satisfies all conditions the {@link #fileTransmissionSupported(ClusterNode)}
 * method must be called prior to data sending.
 * <p>
 * It is possible to receive a set of files on a particular topic (any of {@link GridTopic}) on the remote node.
 * A transmission handler for desired topic must be registered prior to opening transmission sender to it.
 * Methods below are used to register handlers and open new transmissions:
 * <ul>
 * <li>{@link #addTransmissionHandler(Object, TransmissionHandler)}</li>
 * <li>{@link #removeTransmissionHandler(Object)}</li>
 * <li>{@link #openTransmissionSender(UUID, Object)}</li>
 * </ul>
 * <p>
 * Each transmission sender opens a new transmission session to remote node prior to sending files over it.
 * (see description of {@link TransmissionSender} for details). The {@link TransmissionSender}
 * will send all files within single session syncronously one by one.
 * <p>
 * <em>NOTE.</em> It is important to call <em>close()</em> method or use <em>try-with-resource</em>
 * statement to release all resources once you've done with the transmission session. This ensures that all
 * resources are released on remote node in a proper way (i.e. transmission handlers are closed).
 * <p>
 *
 * @see TcpCommunicationSpi
 * @see IgniteMessaging
 * @see TransmissionHandler
 */
public class GridIoManager extends GridManagerAdapter<CommunicationSpi<Serializable>> {
    /** Io communication metrics registry name. */
    public static final String COMM_METRICS = metricName("io", "communication");

    /** Outbound message queue size metric name. */
    public static final String OUTBOUND_MSG_QUEUE_CNT = "OutboundMessagesQueueSize";

    /** Sent messages count metric name. */
    public static final String SENT_MSG_CNT = "SentMessagesCount";

    /** Sent bytes count metric name. */
    public static final String SENT_BYTES_CNT = "SentBytesCount";

    /** Received messages count metric name. */
    public static final String RCVD_MSGS_CNT = "ReceivedMessagesCount";

    /** Received bytes count metric name. */
    public static final String RCVD_BYTES_CNT = "ReceivedBytesCount";

    /** Empty array of message factories. */
    public static final MessageFactory[] EMPTY = {};

    /** Max closed topics to store. */
    public static final int MAX_CLOSED_TOPICS = 10240;

    /** Direct protocol version attribute name. */
    public static final String DIRECT_PROTO_VER_ATTR = "comm.direct.proto.ver";

    /** Direct protocol version. */
    public static final byte DIRECT_PROTO_VER = 3;

    /** Current IO policy. */
    private static final ThreadLocal<Byte> CUR_PLC = new ThreadLocal<>();

    /**
     * Default chunk size in bytes used for sending\receiving files over a {@link SocketChannel}.
     * Setting the transfer chunk size more than <tt>1 MB</tt> is meaningless because there is
     * no asymptotic benefit. What you're trying to achieve with larger transfer chunk sizes is
     * fewer thread context switches, and every time we double the transfer size you have
     * the context switch cost.
     * <p>
     * Default value is {@code 256Kb}.
     */
    private static final int DFLT_CHUNK_SIZE_BYTES = 256 * 1024;

    /** Mutex to achieve consistency of transmission handlers and receiver contexts. */
    private final Object rcvMux = new Object();

    /** Map of registered handlers per each IO topic. */
    private final ConcurrentMap<Object, TransmissionHandler> topicTransmissionHnds = new ConcurrentHashMap<>();

    /** The map of already known channel read contexts by its registered topics. */
    private final ConcurrentMap<Object, ReceiverContext> rcvCtxs = new ConcurrentHashMap<>();

    /** The map of sessions which are currently writing files and their corresponding interruption flags. */
    private final ConcurrentMap<T2<UUID, IgniteUuid>, AtomicBoolean> senderStopFlags = new ConcurrentHashMap<>();

    /**
     * Default factory to provide IO operation interface over files for further transmission them between nodes.
     * Some implementations of file senders\receivers are using the zero-copy approach to transfer bytes
     * from a file to the given {@link SocketChannel} and vice-versa. So, it is necessary to produce an {@link FileIO}
     * implementation based on {@link FileChannel} which is reflected in Ignite project as {@link RandomAccessFileIO}.
     *
     * @see FileChannel#transferTo(long, long, WritableByteChannel)
     */
    private final FileIOFactory fileIoFactory = new RandomAccessFileIOFactory();

    /** The maximum number of retry attempts (read or write attempts). */
    private final int retryCnt;

    /** Network timeout in milliseconds. */
    private final int netTimeoutMs;

    /** Listeners by topic. */
    private final ConcurrentMap<Object, GridMessageListener> lsnrMap = new ConcurrentHashMap<>();

    /** System listeners. */
    private volatile GridMessageListener[] sysLsnrs;

    /** Mutex for system listeners. */
    private final Object sysLsnrsMux = new Object();

    /** Disconnect listeners. */
    private final Collection<GridDisconnectListener> disconnectLsnrs = new ConcurrentLinkedQueue<>();

    /** Pool processor. */
    private final PoolProcessor pools;

    /** Discovery listener. */
    private GridLocalEventListener discoLsnr;

    /** */
    private final ConcurrentMap<Object, ConcurrentMap<UUID, GridCommunicationMessageSet>> msgSetMap =
        new ConcurrentHashMap<>();

    /** Local node ID. */
    private volatile UUID locNodeId;

    /** Cache for messages that were received prior to discovery. */
    private final ConcurrentMap<UUID, Deque<DelayedMessage>> waitMap = new ConcurrentHashMap<>();

    /** Communication message listener. */
    private CommunicationListenerEx<Serializable> commLsnr;

    /** Grid marshaller. */
    private final Marshaller marsh;

    /** Busy lock. */
    private final ReadWriteLock busyLock =
        new StripedCompositeReadWriteLock(Runtime.getRuntime().availableProcessors());

    /** Lock to sync maps access. */
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    /** Fully started flag. When set to true, can send and receive messages. */
    private volatile boolean started;

    /** Closed topics. */
    private final GridBoundedConcurrentLinkedHashSet<Object> closedTopics =
        new GridBoundedConcurrentLinkedHashSet<>(MAX_CLOSED_TOPICS, MAX_CLOSED_TOPICS, 0.75f, 256,
            PER_SEGMENT_Q_OPTIMIZED_RMV);

    /** */
    private MessageFactory msgFactory;

    /** */
    private MessageFormatter formatter;

    /** Stopping flag. */
    private volatile boolean stopping;

    /** */
    private final AtomicReference<ConcurrentHashMap<Long, IoTestFuture>> ioTestMap = new AtomicReference<>();

    /** */
    private final AtomicLong ioTestId = new AtomicLong();

    /** */
    private final TcpCommunicationInverseConnectionHandler invConnHandler = new TcpCommunicationInverseConnectionHandler();

    /** No-op runnable. */
    private static final IgniteRunnable NOOP = () -> {};

    /**
     * @param ctx Grid kernal context.
     */
    @SuppressWarnings("deprecation")
    public GridIoManager(GridKernalContext ctx) {
        super(ctx, ctx.config().getCommunicationSpi());

        pools = ctx.pools();

        assert pools != null;

        locNodeId = ctx.localNodeId();

        marsh = ctx.config().getMarshaller();

        synchronized (sysLsnrsMux) {
            sysLsnrs = new GridMessageListener[GridTopic.values().length];
        }

        retryCnt = ctx.config().getNetworkSendRetryCount();
        netTimeoutMs = (int)ctx.config().getNetworkTimeout();
    }

    /**
     * @return Message factory.
     */
    public MessageFactory messageFactory() {
        assert msgFactory != null;

        return msgFactory;
    }

    /**
     * @return Message writer factory.
     */
    public MessageFormatter formatter() {
        assert formatter != null;

        return formatter;
    }

    /**
     * Resets metrics for this manager.
     */
    public void resetMetrics() {
        getSpi().resetMetrics();
    }

    /** {@inheritDoc} */
    @Override public void start() throws IgniteCheckedException {
        ctx.addNodeAttribute(DIRECT_PROTO_VER_ATTR, DIRECT_PROTO_VER);

        MessageFormatter[] formatterExt = ctx.plugins().extensions(MessageFormatter.class);

        if (formatterExt != null && formatterExt.length > 0) {
            if (formatterExt.length > 1)
                throw new IgniteCheckedException("More than one MessageFormatter extension is defined. Check your " +
                    "plugins configuration and make sure that only one of them provides custom message format.");

            formatter = formatterExt[0];
        }
        else {
            formatter = new MessageFormatter() {
                @Override public MessageWriter writer(UUID rmtNodeId) throws IgniteCheckedException {
                    assert rmtNodeId != null;

                    return new DirectMessageWriter(U.directProtocolVersion(ctx, rmtNodeId));
                }

                @Override public MessageReader reader(UUID rmtNodeId, MessageFactory msgFactory)
                    throws IgniteCheckedException {

                    return new DirectMessageReader(msgFactory,
                        rmtNodeId != null ? U.directProtocolVersion(ctx, rmtNodeId) : DIRECT_PROTO_VER);
                }
            };
        }

        MessageFactory[] msgs = ctx.plugins().extensions(MessageFactory.class);

        if (msgs == null)
            msgs = EMPTY;

        List<MessageFactory> compMsgs = new ArrayList<>();

        compMsgs.add(new GridIoMessageFactory());

        for (IgniteComponentType compType : IgniteComponentType.values()) {
            MessageFactory f = compType.messageFactory();

            if (f != null)
                compMsgs.add(f);
        }

        if (!compMsgs.isEmpty())
            msgs = F.concat(msgs, compMsgs.toArray(new MessageFactory[compMsgs.size()]));

        msgFactory = new IgniteMessageFactoryImpl(msgs);

        CommunicationSpi<Serializable> spi = getSpi();

        if ((CommunicationSpi<?>)spi instanceof TcpCommunicationSpi)
            getTcpCommunicationSpi().setConnectionRequestor(invConnHandler);

        startSpi();

        MetricRegistry ioMetric = ctx.metric().registry(COMM_METRICS);

        ioMetric.register(OUTBOUND_MSG_QUEUE_CNT, spi::getOutboundMessagesQueueSize,
                "Outbound messages queue size.");

        ioMetric.register(SENT_MSG_CNT, spi::getSentMessagesCount, "Sent messages count.");

        ioMetric.register(SENT_BYTES_CNT, spi::getSentBytesCount, "Sent bytes count.");

        ioMetric.register(RCVD_MSGS_CNT, spi::getReceivedMessagesCount,
                "Received messages count.");

        ioMetric.register(RCVD_BYTES_CNT, spi::getReceivedBytesCount, "Received bytes count.");

        getSpi().setListener(commLsnr = new CommunicationListenerEx<Serializable>() {
            @Override public void onMessage(UUID nodeId, Serializable msg, IgniteRunnable msgC) {
                try {
                    onMessage0(nodeId, (GridIoMessage)msg, msgC);
                }
                catch (ClassCastException ignored) {
                    U.error(log, "Communication manager received message of unknown type (will ignore): " +
                            msg.getClass().getName() + ". Most likely GridCommunicationSpi is being used directly, " +
                            "which is illegal - make sure to send messages only via GridProjection API.");
                }
            }

            @Override public void onDisconnected(UUID nodeId) {
                for (GridDisconnectListener lsnr : disconnectLsnrs)
                    lsnr.onNodeDisconnected(nodeId);
            }

            @Override public void onChannelOpened(UUID rmtNodeId, Serializable initMsg, Channel channel) {
                try {
                    onChannelOpened0(rmtNodeId, (GridIoMessage)initMsg, channel);
                }
                catch (ClassCastException ignored) {
                    U.error(log, "Communication manager received message of unknown type (will ignore): " +
                            initMsg.getClass().getName() + ". Most likely GridCommunicationSpi is being used directly, " +
                            "which is illegal - make sure to send messages only via GridProjection API.");
                }
            }
        });

        if (log.isDebugEnabled())
            log.debug(startInfo());

        addMessageListener(GridTopic.TOPIC_IO_TEST, new GridMessageListener() {
            @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
                ClusterNode node = ctx.discovery().node(nodeId);

                if (node == null)
                    return;

                IgniteIoTestMessage msg0 = (IgniteIoTestMessage)msg;

                msg0.senderNodeId(nodeId);

                if (msg0.request()) {
                    IgniteIoTestMessage res = new IgniteIoTestMessage(msg0.id(), false, null);

                    res.flags(msg0.flags());
                    res.onRequestProcessed();

                    res.copyDataFromRequest(msg0);

                    try {
                        sendToGridTopic(node, GridTopic.TOPIC_IO_TEST, res, GridIoPolicy.SYSTEM_POOL);
                    }
                    catch (IgniteCheckedException e) {
                        U.error(log, "Failed to send IO test response [msg=" + msg0 + "]", e);
                    }
                }
                else {
                    IoTestFuture fut = ioTestMap().get(msg0.id());

                    msg0.onResponseProcessed();

                    if (fut == null)
                        U.warn(log, "Failed to find IO test future [msg=" + msg0 + ']');
                    else
                        fut.onResponse(msg0);
                }
            }
        });
    }

    /** {@inheritDoc} */
    @Override public IgniteInternalFuture<?> onReconnected(boolean clusterRestarted) throws IgniteCheckedException {
        locNodeId = ctx.localNodeId();

        return super.onReconnected(clusterRestarted);
    }

    /**
     * @param nodes Nodes.
     * @param payload Payload.
     * @param procFromNioThread If {@code true} message is processed from NIO thread.
     * @return Response future.
     */
    public IgniteInternalFuture sendIoTest(List<ClusterNode> nodes, byte[] payload, boolean procFromNioThread) {
        long id = ioTestId.getAndIncrement();

        IoTestFuture fut = new IoTestFuture(id, nodes.size());

        IgniteIoTestMessage msg = new IgniteIoTestMessage(id, true, payload);

        msg.processFromNioThread(procFromNioThread);

        ioTestMap().put(id, fut);

        for (int i = 0; i < nodes.size(); i++) {
            ClusterNode node = nodes.get(i);

            try {
                sendToGridTopic(node, GridTopic.TOPIC_IO_TEST, msg, GridIoPolicy.SYSTEM_POOL);
            }
            catch (IgniteCheckedException e) {
                ioTestMap().remove(msg.id());

                return new GridFinishedFuture(e);
            }
        }

        return fut;
    }

    /**
     * @param node Node.
     * @param payload Payload.
     * @param procFromNioThread If {@code true} message is processed from NIO thread.
     * @return Response future.
     */
    public IgniteInternalFuture<List<IgniteIoTestMessage>> sendIoTest(
        ClusterNode node,
        byte[] payload,
        boolean procFromNioThread
    ) {
        long id = ioTestId.getAndIncrement();

        IoTestFuture fut = new IoTestFuture(id, 1);

        IgniteIoTestMessage msg = new IgniteIoTestMessage(id, true, payload);

        msg.processFromNioThread(procFromNioThread);

        ioTestMap().put(id, fut);

        try {
            sendToGridTopic(node, GridTopic.TOPIC_IO_TEST, msg, GridIoPolicy.SYSTEM_POOL);
        }
        catch (IgniteCheckedException e) {
            ioTestMap().remove(msg.id());

            return new GridFinishedFuture(e);
        }

        return fut;
    }

    /**
     * @return IO test futures map.
     */
    private ConcurrentHashMap<Long, IoTestFuture> ioTestMap() {
        ConcurrentHashMap<Long, IoTestFuture> map = ioTestMap.get();

        if (map == null) {
            if (!ioTestMap.compareAndSet(null, map = new ConcurrentHashMap<>()))
                map = ioTestMap.get();
        }

        return map;
    }

    /**
     * @param warmup Warmup duration in milliseconds.
     * @param duration Test duration in milliseconds.
     * @param threads Thread count.
     * @param latencyLimit Max latency in nanoseconds.
     * @param rangesCnt Ranges count in resulting histogram.
     * @param payLoadSize Payload size in bytes.
     * @param procFromNioThread {@code True} to process requests in NIO threads.
     * @param nodes Nodes participating in test.
     */
    public void runIoTest(
        final long warmup,
        final long duration,
        final int threads,
        final long latencyLimit,
        final int rangesCnt,
        final int payLoadSize,
        final boolean procFromNioThread,
        final List<ClusterNode> nodes
    ) {
        ExecutorService svc = Executors.newFixedThreadPool(threads + 1);

        final AtomicBoolean warmupFinished = new AtomicBoolean();
        final AtomicBoolean done = new AtomicBoolean();
        final CyclicBarrier bar = new CyclicBarrier(threads + 1);
        final LongAdder cnt = new LongAdder();
        final long sleepDuration = 5000;
        final byte[] payLoad = new byte[payLoadSize];
        final Map<UUID, IoTestThreadLocalNodeResults>[] res = new Map[threads];

        boolean failed = true;

        try {
            svc.execute(new Runnable() {
                @Override public void run() {
                    boolean failed = true;

                    try {
                        bar.await();

                        long start = System.currentTimeMillis();

                        if (log.isInfoEnabled())
                            log.info("IO test started " +
                                "[warmup=" + warmup +
                                ", duration=" + duration +
                                ", threads=" + threads +
                                ", latencyLimit=" + latencyLimit +
                                ", rangesCnt=" + rangesCnt +
                                ", payLoadSize=" + payLoadSize +
                                ", procFromNioThreads=" + procFromNioThread + ']'
                            );

                        for (;;) {
                            if (!warmupFinished.get() && System.currentTimeMillis() - start > warmup) {
                                if (log.isInfoEnabled())
                                    log.info("IO test warmup finished.");

                                warmupFinished.set(true);

                                start = System.currentTimeMillis();
                            }

                            if (warmupFinished.get() && System.currentTimeMillis() - start > duration) {
                                if (log.isInfoEnabled())
                                    log.info("IO test finished, will wait for all threads to finish.");

                                done.set(true);

                                bar.await();

                                failed = false;

                                break;
                            }

                            if (log.isInfoEnabled())
                                log.info("IO test [opsCnt/sec=" + (cnt.sumThenReset() * 1000 / sleepDuration) +
                                    ", warmup=" + !warmupFinished.get() +
                                    ", elapsed=" + (System.currentTimeMillis() - start) + ']');

                            Thread.sleep(sleepDuration);
                        }

                        // At this point all threads have finished the test and
                        // stored data to the resulting array of maps.
                        // Need to iterate it over and sum values for all threads.
                        printIoTestResults(res);
                    }
                    catch (InterruptedException | BrokenBarrierException e) {
                        U.error(log, "IO test failed.", e);
                    }
                    finally {
                        if (failed)
                            bar.reset();
                    }
                }
            });

            for (int i = 0; i < threads; i++) {
                final int i0 = i;

                res[i] = U.newHashMap(nodes.size());

                svc.execute(new Runnable() {
                    @Override public void run() {
                        boolean failed = true;
                        ThreadLocalRandom rnd = ThreadLocalRandom.current();
                        int size = nodes.size();
                        Map<UUID, IoTestThreadLocalNodeResults> res0 = res[i0];

                        try {
                            boolean warmupFinished0 = false;

                            bar.await();

                            for (;;) {
                                if (done.get())
                                    break;

                                if (!warmupFinished0)
                                    warmupFinished0 = warmupFinished.get();

                                ClusterNode node = nodes.get(rnd.nextInt(size));

                                List<IgniteIoTestMessage> msgs = sendIoTest(node, payLoad, procFromNioThread).get();

                                cnt.increment();

                                for (IgniteIoTestMessage msg : msgs) {
                                    UUID nodeId = msg.senderNodeId();

                                    assert nodeId != null;

                                    IoTestThreadLocalNodeResults nodeRes = res0.get(nodeId);

                                    if (nodeRes == null)
                                        res0.put(nodeId,
                                            nodeRes = new IoTestThreadLocalNodeResults(rangesCnt, latencyLimit));

                                    nodeRes.onResult(msg);
                                }
                            }

                            bar.await();

                            failed = false;
                        }
                        catch (Exception e) {
                            U.error(log, "IO test worker thread failed.", e);
                        }
                        finally {
                            if (failed)
                                bar.reset();
                        }
                    }
                });
            }

            failed = false;
        }
        finally {
            if (failed)
                U.shutdownNow(GridIoManager.class, svc, log);
        }
    }

    /**
     * @param rawRes Resulting map.
     */
    private void printIoTestResults(
        Map<UUID, IoTestThreadLocalNodeResults>[] rawRes
    ) {
        Map<UUID, IoTestNodeResults> res = new HashMap<>();

        for (Map<UUID, IoTestThreadLocalNodeResults> r : rawRes) {
            for (Entry<UUID, IoTestThreadLocalNodeResults> e : r.entrySet()) {
                IoTestNodeResults r0 = res.get(e.getKey());

                if (r0 == null)
                    res.put(e.getKey(), r0 = new IoTestNodeResults());

                r0.add(e.getValue());
            }
        }

        SimpleDateFormat dateFmt = new SimpleDateFormat("HH:mm:ss,SSS");

        StringBuilder b = new StringBuilder(U.nl())
            .append("IO test results (round-trip count per each latency bin).")
            .append(U.nl());

        for (Entry<UUID, IoTestNodeResults> e : res.entrySet()) {
            ClusterNode node = ctx.discovery().node(e.getKey());

            long binLatencyMcs = e.getValue().binLatencyMcs();

            b.append("Node ID: ").append(e.getKey()).append(" (addrs=")
                .append(node != null ? node.addresses().toString() : "n/a")
                .append(", binLatency=").append(binLatencyMcs).append("mcs")
                .append(')').append(U.nl());

            b.append("Latency bin, mcs | Count exclusive | Percentage exclusive | " +
                "Count inclusive | Percentage inclusive ").append(U.nl());

            long[] nodeRes = e.getValue().resLatency;

            long sum = 0;

            for (int i = 0; i < nodeRes.length; i++)
                sum += nodeRes[i];

            long curSum = 0;

            for (int i = 0; i < nodeRes.length; i++) {
                curSum += nodeRes[i];

                if (i < nodeRes.length - 1)
                    b.append(String.format("<%11d mcs | %15d | %19.6f%% | %15d | %19.6f%%\n",
                        (i + 1) * binLatencyMcs,
                        nodeRes[i], (100.0 * nodeRes[i]) / sum,
                        curSum, (100.0 * curSum) / sum));
                else
                    b.append(String.format(">%11d mcs | %15d | %19.6f%% | %15d | %19.6f%%\n",
                        i * binLatencyMcs,
                        nodeRes[i], (100.0 * nodeRes[i]) / sum,
                        curSum, (100.0 * curSum) / sum));
            }

            b.append(U.nl()).append("Total latency (ns): ").append(U.nl())
                .append(String.format("%15d", e.getValue().totalLatency)).append(U.nl());

            b.append(U.nl()).append("Max latencies (ns):").append(U.nl());
            format(b, e.getValue().maxLatency, dateFmt);

            b.append(U.nl()).append("Max request send queue times (ns):").append(U.nl());
            format(b, e.getValue().maxReqSendQueueTime, dateFmt);

            b.append(U.nl()).append("Max request receive queue times (ns):").append(U.nl());
            format(b, e.getValue().maxReqRcvQueueTime, dateFmt);

            b.append(U.nl()).append("Max response send queue times (ns):").append(U.nl());
            format(b, e.getValue().maxResSendQueueTime, dateFmt);

            b.append(U.nl()).append("Max response receive queue times (ns):").append(U.nl());
            format(b, e.getValue().maxResRcvQueueTime, dateFmt);

            b.append(U.nl()).append("Max request wire times (millis):").append(U.nl());
            format(b, e.getValue().maxReqWireTimeMillis, dateFmt);

            b.append(U.nl()).append("Max response wire times (millis):").append(U.nl());
            format(b, e.getValue().maxResWireTimeMillis, dateFmt);

            b.append(U.nl());
        }

        if (log.isInfoEnabled())
            log.info(b.toString());
    }

    /**
     * @param b Builder.
     * @param pairs Pairs to format.
     * @param dateFmt Formatter.
     */
    private void format(StringBuilder b, Collection<IgnitePair<Long>> pairs, SimpleDateFormat dateFmt) {
        for (IgnitePair<Long> p : pairs) {
            b.append(String.format("%15d", p.get1())).append(" ")
                .append(dateFmt.format(new Date(p.get2()))).append(U.nl());
        }
    }

    /** {@inheritDoc} */
    @SuppressWarnings({"SynchronizationOnLocalVariableOrMethodParameter"})
    @Override public void onKernalStart0() throws IgniteCheckedException {
        discoLsnr = new GridLocalEventListener() {
            @Override public void onEvent(Event evt) {
                assert evt instanceof DiscoveryEvent : "Invalid event: " + evt;

                DiscoveryEvent discoEvt = (DiscoveryEvent)evt;

                UUID nodeId = discoEvt.eventNode().id();

                switch (evt.type()) {
                    case EVT_NODE_JOINED:
                        assert waitMap.get(nodeId) == null; // We can't receive messages from undiscovered nodes.

                        break;

                    case EVT_NODE_LEFT:
                    case EVT_NODE_FAILED:
                        busyLock.readLock().lock();

                        try {
                            // Stop all writer sessions.
                            for (Map.Entry<T2<UUID, IgniteUuid>, AtomicBoolean> writeSesEntry: senderStopFlags.entrySet()) {
                                if (writeSesEntry.getKey().get1().equals(nodeId))
                                    writeSesEntry.getValue().set(true);
                            }

                            synchronized (rcvMux) {
                                // Clear the context on the uploader node left.
                                Iterator<Map.Entry<Object, ReceiverContext>> it = rcvCtxs.entrySet().iterator();

                                while (it.hasNext()) {
                                    Map.Entry<Object, ReceiverContext> e = it.next();

                                    if (nodeId.equals(e.getValue().rmtNodeId)) {
                                        it.remove();

                                        interruptRecevier(e.getValue(),
                                            new ClusterTopologyCheckedException("Remote node left the grid. " +
                                                "Receiver has been stopped : " + nodeId));
                                    }
                                }
                            }
                        }
                        finally {
                            busyLock.readLock().unlock();
                        }

                        for (Map.Entry<Object, ConcurrentMap<UUID, GridCommunicationMessageSet>> e :
                            msgSetMap.entrySet()) {
                            ConcurrentMap<UUID, GridCommunicationMessageSet> map = e.getValue();

                            GridCommunicationMessageSet set;

                            boolean empty;

                            synchronized (map) {
                                set = map.remove(nodeId);

                                empty = map.isEmpty();
                            }

                            if (set != null) {
                                if (log.isDebugEnabled())
                                    log.debug("Removed message set due to node leaving grid: " + set);

                                // Unregister timeout listener.
                                ctx.timeout().removeTimeoutObject(set);

                                // Node may still send stale messages for this topic
                                // even after discovery notification is done.
                                closedTopics.add(set.topic());
                            }

                            if (empty)
                                msgSetMap.remove(e.getKey(), map);
                        }

                        // Clean up delayed and ordered messages (need exclusive lock).
                        lock.writeLock().lock();

                        try {
                            Deque<DelayedMessage> waitList = waitMap.remove(nodeId);

                            if (log.isDebugEnabled())
                                log.debug("Removed messages from discovery startup delay list " +
                                    "(sender node left topology): " + waitList);
                        }
                        finally {
                            lock.writeLock().unlock();
                        }

                        break;

                    default:
                        assert false : "Unexpected event: " + evt;
                }
            }
        };

        ctx.event().addLocalEventListener(discoLsnr, EVT_NODE_JOINED, EVT_NODE_LEFT, EVT_NODE_FAILED);

        invConnHandler.onStart();

        // Make sure that there are no stale messages due to window between communication
        // manager start and kernal start.
        // 1. Process wait list.
        Collection<Collection<DelayedMessage>> delayedMsgs = new ArrayList<>();

        lock.writeLock().lock();

        try {
            started = true;

            for (Entry<UUID, Deque<DelayedMessage>> e : waitMap.entrySet()) {
                if (ctx.discovery().node(e.getKey()) != null) {
                    Deque<DelayedMessage> waitList = waitMap.remove(e.getKey());

                    if (log.isDebugEnabled())
                        log.debug("Processing messages from discovery startup delay list: " + waitList);

                    if (waitList != null)
                        delayedMsgs.add(waitList);
                }
            }
        }
        finally {
            lock.writeLock().unlock();
        }

        // After write lock released.
        if (!delayedMsgs.isEmpty()) {
            for (Collection<DelayedMessage> col : delayedMsgs)
                for (DelayedMessage msg : col)
                    commLsnr.onMessage(msg.nodeId(), msg.message(), msg.callback());
        }

        // 2. Process messages sets.
        for (Map.Entry<Object, ConcurrentMap<UUID, GridCommunicationMessageSet>> e : msgSetMap.entrySet()) {
            ConcurrentMap<UUID, GridCommunicationMessageSet> map = e.getValue();

            for (GridCommunicationMessageSet set : map.values()) {
                if (ctx.discovery().node(set.nodeId()) == null) {
                    // All map modifications should be synced for consistency.
                    boolean rmv;

                    synchronized (map) {
                        rmv = map.remove(set.nodeId(), set);
                    }

                    if (rmv) {
                        if (log.isDebugEnabled())
                            log.debug("Removed message set due to node leaving grid: " + set);

                        // Unregister timeout listener.
                        ctx.timeout().removeTimeoutObject(set);
                    }

                }
            }

            boolean rmv;

            synchronized (map) {
                rmv = map.isEmpty();
            }

            if (rmv) {
                msgSetMap.remove(e.getKey(), map);

                // Node may still send stale messages for this topic
                // even after discovery notification is done.
                closedTopics.add(e.getKey());
            }
        }
    }

    /**
     * Checks that both local and remote nodes are configured to use paired connections.
     *
     * @param node Remote node.
     * @param tcpCommSpi TcpCommunicationSpi.
     * @return {@code True} if both local and remote nodes are configured to use paired connections.
     */
    private boolean isPairedConnection(ClusterNode node, TcpCommunicationSpi tcpCommSpi) {
        return tcpCommSpi.isUsePairedConnections() &&
            Boolean.TRUE.equals(node.attribute(U.spiAttribute(tcpCommSpi, ATTR_PAIRED_CONN)));
    }

    /**
     * @return Instance of {@link TcpCommunicationSpi}. Will throw {@link AssertionError} or {@link ClassCastException}
     *      if another SPI type is configured. Must be called only if type of SPI has been explicilty asserted earlier.
     */
    private TcpCommunicationSpi getTcpCommunicationSpi() {
        CommunicationSpi<?> spi = getSpi();

        assert spi instanceof TcpCommunicationSpi;

        return (TcpCommunicationSpi)spi;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("BusyWait")
    @Override public void onKernalStop0(boolean cancel) {
        // No more communication messages.
        getSpi().setListener(null);

        boolean interrupted = false;

        // Busy wait is intentional.
        while (true) {
            try {
                if (busyLock.writeLock().tryLock(200, TimeUnit.MILLISECONDS))
                    break;
                else
                    Thread.sleep(200);
            }
            catch (InterruptedException ignore) {
                // Preserve interrupt status & ignore.
                // Note that interrupted flag is cleared.
                interrupted = true;
            }
        }

        try {
            if (interrupted)
                Thread.currentThread().interrupt();

            GridEventStorageManager evtMgr = ctx.event();

            if (evtMgr != null && discoLsnr != null)
                evtMgr.removeLocalEventListener(discoLsnr);

            stopping = true;

            Set<ReceiverContext> rcvs;

            synchronized (rcvMux) {
                topicTransmissionHnds.clear();

                rcvs = new HashSet<>(rcvCtxs.values());

                rcvCtxs.clear();
            }

            for (ReceiverContext rctx : rcvs) {
                interruptRecevier(rctx, new NodeStoppingException("Local node io manager requested to be stopped: "
                    + ctx.localNodeId()));
            }
        }
        finally {
            busyLock.writeLock().unlock();
        }
    }

    /** {@inheritDoc} */
    @Override public void stop(boolean cancel) throws IgniteCheckedException {
        stopSpi();

        invConnHandler.onStop();

        if (log.isDebugEnabled())
            log.debug(stopInfo());
    }

    /**
     * @param rmtNodeId The remote node id.
     * @param initMsg Message with additional channel params.
     * @param channel The channel to notify listeners with.
     */
    private void onChannelOpened0(UUID rmtNodeId, GridIoMessage initMsg, Channel channel) {
        Lock busyLock0 = busyLock.readLock();

        busyLock0.lock();

        try {
            if (stopping) {
                if (log.isDebugEnabled()) {
                    log.debug("Received communication channel create event while node stopping (will ignore) " +
                        "[rmtNodeId=" + rmtNodeId + ", initMsg=" + initMsg + ']');
                }

                return;
            }

            if (initMsg.topic() == null) {
                int topicOrd = initMsg.topicOrdinal();

                initMsg.topic(topicOrd >= 0 ? GridTopic.fromOrdinal(topicOrd) :
                    U.unmarshal(marsh, initMsg.topicBytes(), U.resolveClassLoader(ctx.config())));
            }

            byte plc = initMsg.policy();

            pools.poolForPolicy(plc).execute(new Runnable() {
                @Override public void run() {
                    processOpenedChannel(initMsg.topic(), rmtNodeId, (SessionChannelMessage)initMsg.message(),
                        (SocketChannel)channel);
                }
            });
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to process channel creation event due to exception " +
                "[rmtNodeId=" + rmtNodeId + ", initMsg=" + initMsg + ']', e);
        }
        finally {
            busyLock0.unlock();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message bytes.
     * @param msgC Closure to call when message processing finished.
     */
    private void onMessage0(UUID nodeId, GridIoMessage msg, IgniteRunnable msgC) {
        assert nodeId != null;
        assert msg != null;

        Lock busyLock0 = busyLock.readLock();

        busyLock0.lock();

        try {
            if (stopping) {
                if (log.isDebugEnabled())
                    log.debug("Received communication message while stopping (will ignore) [nodeId=" +
                        nodeId + ", msg=" + msg + ']');

                return;
            }

            if (msg.topic() == null) {
                int topicOrd = msg.topicOrdinal();

                msg.topic(topicOrd >= 0 ? GridTopic.fromOrdinal(topicOrd) :
                    U.unmarshal(marsh, msg.topicBytes(), U.resolveClassLoader(ctx.config())));
            }

            if (!started) {
                lock.readLock().lock();

                try {
                    if (!started) { // Sets to true in write lock, so double checking.
                        // Received message before valid context is set to manager.
                        if (log.isDebugEnabled())
                            log.debug("Adding message to waiting list [senderId=" + nodeId +
                                ", msg=" + msg + ']');

                        Deque<DelayedMessage> list = F.<UUID, Deque<DelayedMessage>>addIfAbsent(
                            waitMap,
                            nodeId,
                            ConcurrentLinkedDeque::new
                        );

                        assert list != null;

                        list.add(new DelayedMessage(nodeId, msg, msgC));

                        return;
                    }
                }
                finally {
                    lock.readLock().unlock();
                }
            }

            // If message is P2P, then process in P2P service.
            // This is done to avoid extra waiting and potential deadlocks
            // as thread pool may not have any available threads to give.
            byte plc = msg.policy();

            switch (plc) {
                case P2P_POOL: {
                    processP2PMessage(nodeId, msg, msgC);

                    break;
                }

                case PUBLIC_POOL:
                case SYSTEM_POOL:
                case MANAGEMENT_POOL:
                case AFFINITY_POOL:
                case UTILITY_CACHE_POOL:
                case IDX_POOL:
                case DATA_STREAMER_POOL:
                case QUERY_POOL:
                case SCHEMA_POOL:
                case SERVICE_POOL:
                {
                    if (msg.isOrdered())
                        processOrderedMessage(nodeId, msg, plc, msgC);
                    else
                        processRegularMessage(nodeId, msg, plc, msgC);

                    break;
                }

                default:
                    assert plc >= 0 : "Negative policy [plc=" + plc + ", msg=" + msg + ']';

                    if (isReservedGridIoPolicy(plc))
                        throw new IgniteCheckedException("Failed to process message with policy of reserved range. " +
                            "[policy=" + plc + ']');

                    if (msg.isOrdered())
                        processOrderedMessage(nodeId, msg, plc, msgC);
                    else
                        processRegularMessage(nodeId, msg, plc, msgC);
            }
        }
        catch (IgniteCheckedException e) {
            U.error(log, "Failed to process message (will ignore): " + msg, e);
        }
        finally {
            busyLock0.unlock();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     * @param msgC Closure to call when message processing finished.
     */
    private void processP2PMessage(
        final UUID nodeId,
        final GridIoMessage msg,
        final IgniteRunnable msgC
    ) {
        Runnable c = new Runnable() {
            @Override public void run() {
                try {
                    threadProcessingMessage(true, msgC);

                    GridMessageListener lsnr = listenerGet0(msg.topic());

                    if (lsnr == null)
                        return;

                    Object obj = msg.message();

                    assert obj != null;

                    invokeListener(msg.policy(), lsnr, nodeId, obj, secSubjId(msg));
                }
                finally {
                    threadProcessingMessage(false, null);

                    msgC.run();
                }
            }
        };

        try {
            pools.p2pPool().execute(c);
        }
        catch (RejectedExecutionException e) {
            U.error(log, "Failed to process P2P message due to execution rejection. Increase the upper bound " +
                "on 'ExecutorService' provided by 'IgniteConfiguration.getPeerClassLoadingThreadPoolSize()'. " +
                "Will attempt to process message in the listener thread instead.", e);

            c.run();
        }
    }

    /**
     * @param nodeId Node ID.
     * @param msg Message.
     * @param plc Execution policy.
     * @param msgC Closure to call when message processing finished.
     * @throws IgniteCheckedException If failed.
     */
    private void processRegularMessage(
        final UUID nodeId,
        final GridIoMessage msg,
        final byte plc,
        final IgniteRunnable msgC
    ) throws IgniteCheckedException {
        Runnable c = new TraceRunnable(ctx.tracing(), COMMUNICATION_REGULAR_PROCESS) {
            @Override public void execute() {
                try {
                    MTC.span().addTag(SpanTags.MESSAGE, () -> traceName(msg));

                    threadProcessingMessage(true, msgC);

                    processRegularMessage0(msg, nodeId);
                }
                finally {
                    threadProcessingMessage(false, null);

                    msgC.run();
                }
            }

            @Override public String toString() {
                return "Message closure [msg=" + msg + ']';
            }
        };

        MTC.span().addLog(() -> "Regular process queued");

        if (msg.topicOrdinal() == TOPIC_IO_TEST.ordinal()) {
            IgniteIoTestMessage msg0 = (IgniteIoTestMessage)msg.message();

            if (msg0.processFromNioThread())
                c.run();
            else
                ctx.getStripedExecutorService().execute(-1, c);

            return;
        }
        if (msg.topicOrdinal() == TOPIC_CACHE_COORDINATOR.ordinal()) {
            MvccMessage msg0 = (MvccMessage)msg.message();

            // see IGNITE-8609
            /*if (msg0.processedFromNioThread())
                c.run();
            else*/
                ctx.getStripedExecutorService().execute(-1, c);

            return;
        }

        final int part = msg.partition(); // Store partition to avoid possible recalculation.

        if (plc == GridIoPolicy.SYSTEM_POOL && part != GridIoMessage.STRIPE_DISABLED_PART) {
            ctx.getStripedExecutorService().execute(part, c);

            return;
        }

        if (plc == GridIoPolicy.DATA_STREAMER_POOL && part != GridIoMessage.STRIPE_DISABLED_PART) {
            ctx.getDataStreamerExecutorService().execute(part, c);

            return;
        }

        if (msg.topicOrdinal() == TOPIC_IO_TEST.ordinal()) {
            IgniteIoTestMessage msg0 = (IgniteIoTestMessage)msg.message();

            if (msg0.processFromNioThread()) {
                c.run();

                return;
            }
        }

        try {
            String execName = msg.executorName();

            if (execName != null) {
                Executor exec = pools.customExecutor(execName);

                if (exec != null) {
                    exec.execute(c);

                    return;
                }
                else {
                    LT.warn(log, "Custom executor doesn't exist (message will be processed in default " +
                        "thread pool): " + execName);
                }
            }

            pools.poolForPolicy(plc).execute(c);
        }
        catch (RejectedExecutionException e) {
            if (!ctx.isStopping()) {
                U.error(log, "Failed to process regular message due to execution rejection. Will attempt to process " +
                        "message in the listener thread instead.", e);

                c.run();
            }
            else if (log.isDebugEnabled())
                log.debug("Failed to process regular message due to execution rejection: " + msg);
        }
    }

    /**
     * @param msg Message.
     * @param nodeId Node ID.
     */
    private void processRegularMessage0(GridIoMessage msg, UUID nodeId) {
        GridMessageListener lsnr = listenerGet0(msg.topic());

        if (lsnr == null)
            return;

        Object obj = msg.message();

        assert obj != null;

        invokeListener(msg.policy(), lsnr, nodeId, obj, secSubjId(msg));
    }

    /**
     * Get listener.
     *
     * @param topic Topic.
     * @return Listener.
     */
    @Nullable private GridMessageListener listenerGet0(Object topic) {
        if (topic instanceof GridTopic)
            return sysLsnrs[systemListenerIndex(topic)];
        else
            return lsnrMap.get(topic);
    }

    /**
     * Put listener if it is absent.
     *
     * @param topic Topic.
     * @param lsnr Listener.
     * @return Old listener (if any).
     */
    @Nullable private GridMessageListener listenerPutIfAbsent0(Object topic, GridMessageListener lsnr) {
        if (topic instanceof GridTopic) {
            synchronized (sysLsnrsMux) {
                int idx = systemListenerIndex(topic);

                GridMessageListener old = sysLsnrs[idx];

                if (old == null)
                    changeSystemListener(idx, lsnr);

                return old;
            }
        }
        else
            return lsnrMap.putIfAbsent(topic, lsnr);
    }

    /**
     * Remove listener.
     *
     * @param topic Topic.
     * @return Removed listener (if any).
     */
    @Nullable private GridMessageListener listenerRemove0(Object topic) {
        if (topic instanceof GridTopic) {
            synchronized (sysLsnrsMux) {
                int idx = systemListenerIndex(topic);

                GridMessageListener old = sysLsnrs[idx];

                if (old != null)
                    changeSystemListener(idx, null);

                return old;
            }
        }
        else
            return lsnrMap.remove(topic);
    }

    /**
     * Remove listener if it matches expected value.
     *
     * @param topic Topic.
     * @param exp Listener.
     * @return Result.
     */
    private boolean listenerRemove0(Object topic, GridMessageListener exp) {
        if (topic instanceof GridTopic) {
            synchronized (sysLsnrsMux) {
                return systemListenerChange(topic, exp, null);
            }
        }
        else
            return lsnrMap.remove(topic, exp);
    }

    /**
     * Replace listener.
     *
     * @param topic Topic.
     * @param exp Old value.
     * @param newVal New value.
     * @return Result.
     */
    private boolean listenerReplace0(Object topic, GridMessageListener exp, GridMessageListener newVal) {
        if (topic instanceof GridTopic) {
            synchronized (sysLsnrsMux) {
                return systemListenerChange(topic, exp, newVal);
            }
        }
        else
            return lsnrMap.replace(topic, exp, newVal);
    }

    /**
     * Change system listener.
     *
     * @param topic Topic.
     * @param exp Expected value.
     * @param newVal New value.
     * @return Result.
     */
    private boolean systemListenerChange(Object topic, GridMessageListener exp, GridMessageListener newVal) {
        assert Thread.holdsLock(sysLsnrsMux);
        assert topic instanceof GridTopic;

        int idx = systemListenerIndex(topic);

        GridMessageListener old = sysLsnrs[idx];

        if (old != null && old.equals(exp)) {
            changeSystemListener(idx, newVal);

            return true;
        }

        return false;
    }

    /**
     * Change systme listener at the given index.
     *
     * @param idx Index.
     * @param lsnr Listener.
     */
    private void changeSystemListener(int idx, @Nullable GridMessageListener lsnr) {
        assert Thread.holdsLock(sysLsnrsMux);

        GridMessageListener[] res = new GridMessageListener[sysLsnrs.length];

        System.arraycopy(sysLsnrs, 0, res, 0, sysLsnrs.length);

        res[idx] = lsnr;

        sysLsnrs = res;
    }

    /**
     * Get index of a system listener.
     *
     * @param topic Topic.
     * @return Index.
     */
    private int systemListenerIndex(Object topic) {
        assert topic instanceof GridTopic;

        return ((GridTopic)topic).ordinal();
    }

    /**
     * @param nodeId Node ID.
     * @param msg Ordered message.
     * @param plc Execution policy.
     * @param msgC Closure to call when message processing finished ({@code null} for sync processing).
     * @throws IgniteCheckedException If failed.
     */
    @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
    private void processOrderedMessage(
        final UUID nodeId,
        final GridIoMessage msg,
        final byte plc,
        @Nullable final IgniteRunnable msgC
    ) throws IgniteCheckedException {
        assert msg != null;

        long timeout = msg.timeout();
        boolean skipOnTimeout = msg.skipOnTimeout();

        boolean isNew = false;

        ConcurrentMap<UUID, GridCommunicationMessageSet> map;

        GridCommunicationMessageSet set = null;

        while (true) {
            map = msgSetMap.get(msg.topic());

            if (map == null) {
                set = new GridCommunicationMessageSet(plc, msg.topic(), nodeId, timeout, skipOnTimeout, msg, msgC);

                map = new ConcurrentHashMap0<>();

                map.put(nodeId, set);

                ConcurrentMap<UUID, GridCommunicationMessageSet> old = msgSetMap.putIfAbsent(
                    msg.topic(), map);

                if (old != null)
                    map = old;
                else {
                    isNew = true;

                    // Put succeeded.
                    break;
                }
            }

            boolean rmv = false;

            synchronized (map) {
                if (map.isEmpty())
                    rmv = true;
                else {
                    set = map.get(nodeId);

                    if (set == null) {
                        GridCommunicationMessageSet old = map.putIfAbsent(nodeId,
                            set = new GridCommunicationMessageSet(plc, msg.topic(),
                                nodeId, timeout, skipOnTimeout, msg, msgC));

                        assert old == null;

                        isNew = true;

                        // Put succeeded.
                        break;
                    }
                }
            }

            if (rmv)
                msgSetMap.remove(msg.topic(), map);
            else {
                assert set != null;
                assert !isNew;

                set.add(msg, msgC);

                break;
            }
        }

        if (isNew && ctx.discovery().node(nodeId) == null) {
            if (log.isDebugEnabled())
                log.debug("Message is ignored as sender has left the grid: " + msg);

            assert map != null;

            boolean rmv;

            synchronized (map) {
                map.remove(nodeId);

                rmv = map.isEmpty();
            }

            if (rmv)
                msgSetMap.remove(msg.topic(), map);

            return;
        }

        if (isNew && set.endTime() != Long.MAX_VALUE)
            ctx.timeout().addTimeoutObject(set);

        final GridMessageListener lsnr = listenerGet0(msg.topic());

        if (lsnr == null) {
            if (closedTopics.contains(msg.topic())) {
                if (log.isDebugEnabled())
                    log.debug("Message is ignored as it came for the closed topic: " + msg);

                assert map != null;

                msgSetMap.remove(msg.topic(), map);
            }
            else if (log.isDebugEnabled()) {
                // Note that we simply keep messages if listener is not
                // registered yet, until one will be registered.
                log.debug("Received message for unknown listener (messages will be kept until a " +
                    "listener is registered): " + msg);
            }

            // Mark the message as processed, otherwise reading from the connection
            // may stop.
            if (msgC != null)
                msgC.run();

            return;
        }

        if (msgC == null) {
            // Message from local node can be processed in sync manner.
            assert locNodeId.equals(nodeId);

            unwindMessageSet(set, lsnr);

            return;
        }

        final GridCommunicationMessageSet msgSet0 = set;

        Runnable c = new Runnable() {
            @Override public void run() {
                try {
                    threadProcessingMessage(true, msgC);

                    unwindMessageSet(msgSet0, lsnr);
                }
                finally {
                    threadProcessingMessage(false, null);
                }
            }
        };

        try {
            MTC.span().addLog(() -> "Ordered process queued");

            pools.poolForPolicy(plc).execute(c);
        }
        catch (RejectedExecutionException e) {
            U.error(log, "Failed to process ordered message due to execution rejection. " +
                "Increase the upper bound on executor service provided by corresponding " +
                "configuration property. Will attempt to process message in the listener " +
                "thread instead [msgPlc=" + plc + ']', e);

            c.run();
        }
    }

    /**
     * @param msgSet Message set to unwind.
     * @param lsnr Listener to notify.
     */
    private void unwindMessageSet(GridCommunicationMessageSet msgSet, GridMessageListener lsnr) {
        // Loop until message set is empty or
        // another thread owns the reservation.
        while (true) {
            if (msgSet.reserve()) {
                try {
                    msgSet.unwind(lsnr);
                }
                finally {
                    msgSet.release();
                }

                // Check outside of reservation block.
                if (!msgSet.changed()) {
                    if (log.isDebugEnabled())
                        log.debug("Message set has not been changed: " + msgSet);

                    break;
                }
            }
            else {
                if (log.isDebugEnabled())
                    log.debug("Another thread owns reservation: " + msgSet);

                return;
            }
        }
    }

    /**
     * Invoke message listener.
     *
     * @param plc Policy.
     * @param lsnr Listener.
     * @param nodeId Node ID.
     * @param msg Message.
     * @param secSubjId Security subject that will be used to open a security session.
     */
    private void invokeListener(Byte plc, GridMessageListener lsnr, UUID nodeId, Object msg, UUID secSubjId) {
        MTC.span().addLog(() -> "Invoke listener");

        Byte oldPlc = CUR_PLC.get();

        boolean change = !F.eq(oldPlc, plc);

        if (change)
            CUR_PLC.set(plc);

        UUID newSecSubjId = secSubjId != null ? secSubjId : nodeId;

        try (OperationSecurityContext s = ctx.security().withContext(newSecSubjId)) {
            lsnr.onMessage(nodeId, msg, plc);
        }
        finally {
            if (change)
                CUR_PLC.set(oldPlc);
        }
    }

    /**
     * @return Current IO policy
     */
    @Nullable public static Byte currentPolicy() {
        return CUR_PLC.get();
    }

    /**
     * @param nodeId Node ID.
     * @param sndErr Send error.
     * @param ping {@code True} if try ping node.
     * @return {@code True} if node left.
     * @throws IgniteClientDisconnectedCheckedException If ping failed.
     */
    public boolean checkNodeLeft(UUID nodeId, IgniteCheckedException sndErr, boolean ping)
        throws IgniteClientDisconnectedCheckedException
    {
        return sndErr instanceof ClusterTopologyCheckedException ||
            ctx.discovery().node(nodeId) == null ||
            (ping && !ctx.discovery().pingNode(nodeId));
    }

    /**
     * @param remoteId The remote node to connect to.
     * @param topic The remote topic to connect to.
     * @return The channel instance to communicate with remote.
     */
    public TransmissionSender openTransmissionSender(UUID remoteId, Object topic) {
        return new TransmissionSender(remoteId, topic);
    }

    /**
     * @param topic The {@link GridTopic} to register handler to.
     * @param hnd Handler which will handle file upload requests.
     */
    public void addTransmissionHandler(Object topic, TransmissionHandler hnd) {
        TransmissionHandler hnd0 = topicTransmissionHnds.putIfAbsent(topic, hnd);

        assert hnd0 == null : "The topic already have an appropriate session handler [topic=" + topic + ']';
    }

    /**
     * @param topic The topic to erase handler from.
     */
    public void removeTransmissionHandler(Object topic) {
        ReceiverContext rcvCtx0;

        synchronized (rcvMux) {
            topicTransmissionHnds.remove(topic);

            rcvCtx0 = rcvCtxs.remove(topic);
        }

        interruptRecevier(rcvCtx0,
            new IgniteCheckedException("Receiver has been closed due to removing corresponding transmission handler " +
                "on local node [nodeId=" + ctx.localNodeId() + ']'));
    }

    /**
     * This method must be used prior to opening a {@link TransmissionSender} by calling
     * {@link #openTransmissionSender(UUID, Object)} to ensure that remote and local nodes
     * are fully support direct {@link SocketChannel} connection to transfer data.
     *
     * @param node Remote node to check.
     * @return {@code true} if a file can be sent over socket channel directly.
     */
    public boolean fileTransmissionSupported(ClusterNode node) {
        return ((CommunicationSpi)getSpi() instanceof TcpCommunicationSpi) &&
            nodeSupports(node, CHANNEL_COMMUNICATION);
    }

    /**
     * @param nodeId Destination node to connect to.
     * @param topic Topic to send the request to.
     * @param initMsg Channel initialization message.
     * @return Established {@link Channel} to use.
     * @throws IgniteCheckedException If fails.
     */
    private IgniteInternalFuture<Channel> openChannel(
        UUID nodeId,
        Object topic,
        Message initMsg
    ) throws IgniteCheckedException {
        assert nodeId != null;
        assert topic != null;
        assert !locNodeId.equals(nodeId) : "Channel cannot be opened to the local node itself: " + nodeId;
        assert (CommunicationSpi)getSpi() instanceof TcpCommunicationSpi : "Only TcpCommunicationSpi supports direct " +
            "connections between nodes: " + getSpi().getClass();

        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null)
            throw new ClusterTopologyCheckedException("Failed to open a new channel to remote node (node left): " + nodeId);

        int topicOrd = topic instanceof GridTopic ? ((Enum<GridTopic>)topic).ordinal() : -1;

        GridIoMessage ioMsg = createGridIoMessage(topic,
            topicOrd,
            initMsg,
            PUBLIC_POOL,
            false,
            0,
            false
        );

        try {
            if (topicOrd < 0)
                ioMsg.topicBytes(U.marshal(marsh, topic));

            return ((TcpCommunicationSpi)(CommunicationSpi)getSpi()).openChannel(node, ioMsg);
        }
        catch (IgniteSpiException e) {
            if (e.getCause() instanceof ClusterTopologyCheckedException)
                throw (ClusterTopologyCheckedException)e.getCause();

            if (!ctx.discovery().alive(node))
                throw new ClusterTopologyCheckedException("Failed to create channel (node left): " + node.id(), e);

            throw new IgniteCheckedException("Failed to create channel (node may have left the grid or " +
                "TCP connection cannot be established due to unknown issues) " +
                "[node=" + node + ", topic=" + topic + ']', e);
        }
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param topicOrd GridTopic enumeration ordinal.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param ordered Ordered flag.
     * @param timeout Timeout.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     * @param ackC Ack closure.
     * @param async If {@code true} message for local node will be processed in pool, otherwise in current thread.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    private void send(
        ClusterNode node,
        Object topic,
        int topicOrd,
        Message msg,
        byte plc,
        boolean ordered,
        long timeout,
        boolean skipOnTimeout,
        IgniteInClosure<IgniteException> ackC,
        boolean async
    ) throws IgniteCheckedException {
        assert node != null;
        assert topic != null;
        assert msg != null;
        assert !async || msg instanceof GridIoUserMessage : msg; // Async execution was added only for IgniteMessaging.
        assert topicOrd >= 0 || !(topic instanceof GridTopic) : msg;

        try (TraceSurroundings ignored = support(null)) {
            MTC.span().addLog(() -> "Create communication msg - " + traceName(msg));

            GridIoMessage ioMsg = createGridIoMessage(topic, topicOrd, msg, plc, ordered, timeout, skipOnTimeout);

            if (locNodeId.equals(node.id())) {

                assert plc != P2P_POOL;

                CommunicationListener commLsnr = this.commLsnr;

                if (commLsnr == null)
                    throw new IgniteCheckedException("Trying to send message when grid is not fully started.");

                if (ordered)
                    processOrderedMessage(locNodeId, ioMsg, plc, null);
                else if (async)
                    processRegularMessage(locNodeId, ioMsg, plc, NOOP);
                else
                    processRegularMessage0(ioMsg, locNodeId);

                if (ackC != null)
                    ackC.apply(null);
            }
            else {
                if (topicOrd < 0)
                    ioMsg.topicBytes(U.marshal(marsh, topic));

                try {
                    if ((CommunicationSpi<?>)getSpi() instanceof TcpCommunicationSpi)
                        getTcpCommunicationSpi().sendMessage(node, ioMsg, ackC);
                    else
                        getSpi().sendMessage(node, ioMsg);
                }
                catch (IgniteSpiException e) {
                    if (e.getCause() instanceof ClusterTopologyCheckedException)
                        throw (ClusterTopologyCheckedException)e.getCause();

                    if (!ctx.discovery().alive(node))
                        throw new ClusterTopologyCheckedException("Failed to send message, node left: " + node.id(), e);

                    throw new IgniteCheckedException("Failed to send message (node may have left the grid or " +
                        "TCP connection cannot be established due to firewall issues) " +
                        "[node=" + node + ", topic=" + topic +
                        ", msg=" + msg + ", policy=" + plc + ']', e);
                }
            }
        }
    }

    /** */
    private long getInverseConnectionWaitTimeout() {
        return ctx.config().getFailureDetectionTimeout();
    }

    /**
     * @return One of two message wrappers. The first is {@link GridIoMessage}, the second is secured version {@link
     * GridIoSecurityAwareMessage}.
     */
    private @NotNull GridIoMessage createGridIoMessage(
        Object topic,
        int topicOrd,
        Message msg,
        byte plc,
        boolean ordered,
        long timeout,
        boolean skipOnTimeout
    ) {
        if (ctx.security().enabled()) {
            UUID secSubjId = null;

            UUID curSecSubjId = ctx.security().securityContext().subject().id();

            if (!locNodeId.equals(curSecSubjId))
                secSubjId = curSecSubjId;

            return new GridIoSecurityAwareMessage(secSubjId, plc, topic, topicOrd, msg, ordered, timeout, skipOnTimeout);
        }

        return new GridIoMessage(plc, topic, topicOrd, msg, ordered, timeout, skipOnTimeout);
    }

    /**
     * @param nodeId Id of destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendToCustomTopic(UUID nodeId, Object topic, Message msg, byte plc)
        throws IgniteCheckedException {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null)
            throw new ClusterTopologyCheckedException("Failed to send message to node (has node left grid?): " + nodeId);

        sendToCustomTopic(node, topic, msg, plc);
    }

    /**
     * @param nodeId Id of destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendToGridTopic(UUID nodeId, GridTopic topic, Message msg, byte plc)
        throws IgniteCheckedException {
        ClusterNode node = ctx.discovery().node(nodeId);

        if (node == null)
            throw new ClusterTopologyCheckedException("Failed to send message to node (has node left grid?): " + nodeId);

        send(node, topic, topic.ordinal(), msg, plc, false, 0, false, null, false);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendToGridTopic(ClusterNode node, GridTopic topic, Message msg, byte plc)
        throws IgniteCheckedException {
        send(node, topic, topic.ordinal(), msg, plc, false, 0, false, null, false);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendToCustomTopic(ClusterNode node, Object topic, Message msg, byte plc)
        throws IgniteCheckedException {
        send(node, topic, -1, msg, plc, false, 0, false, null, false);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param span Current span for tracing.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendToGridTopic(ClusterNode node, GridTopic topic, Message msg, byte plc, Span span)
        throws IgniteCheckedException {
        send(node, topic, topic.ordinal(), msg, plc, false, 0, false, null, false);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param topicOrd GridTopic enumeration ordinal.
     * @param msg Message to send.
     * @param plc Type of processing.     *
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendGeneric(ClusterNode node, Object topic, int topicOrd, Message msg, byte plc)
        throws IgniteCheckedException {
        send(node, topic, topicOrd, msg, plc, false, 0, false, null, false);
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param timeout Timeout to keep a message on receiving queue.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendOrderedMessage(
        ClusterNode node,
        Object topic,
        Message msg,
        byte plc,
        long timeout,
        boolean skipOnTimeout
    ) throws IgniteCheckedException {
        assert timeout > 0 || skipOnTimeout;

        send(node, topic, (byte)-1, msg, plc, true, timeout, skipOnTimeout, null, false);
    }

    /**
     * @param node Destination nodes.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param ackC Ack closure.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendToGridTopic(ClusterNode node,
        GridTopic topic,
        Message msg,
        byte plc,
        IgniteInClosure<IgniteException> ackC) throws IgniteCheckedException
    {
        send(node, topic, topic.ordinal(), msg, plc, false, 0, false, ackC, false);
    }

    /**
     * @param nodes Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param timeout Timeout to keep a message on receiving queue.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    void sendOrderedMessageToGridTopic(
        Collection<? extends ClusterNode> nodes,
        GridTopic topic,
        Message msg,
        byte plc,
        long timeout,
        boolean skipOnTimeout
    )
        throws IgniteCheckedException {
        assert timeout > 0 || skipOnTimeout;

        IgniteCheckedException err = null;

        for (ClusterNode node : nodes) {
            try {
                send(node, topic, topic.ordinal(), msg, plc, true, timeout, skipOnTimeout, null, false);
            }
            catch (IgniteCheckedException e) {
                if (err == null)
                    err = e;
                else
                    err.addSuppressed(e);
            }
        }

        if (err != null)
            throw err;
    }

    /**
     * @param nodes Destination nodes.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendToGridTopic(
        Collection<? extends ClusterNode> nodes,
        GridTopic topic,
        Message msg,
        byte plc
    ) throws IgniteCheckedException {
        IgniteCheckedException err = null;

        for (ClusterNode node : nodes) {
            try {
                send(node, topic, topic.ordinal(), msg, plc, false, 0, false, null, false);
            }
            catch (IgniteCheckedException e) {
                if (err == null)
                    err = e;
                else
                    err.addSuppressed(e);
            }
        }

        if (err != null)
            throw err;
    }

    /**
     * @param node Destination node.
     * @param topic Topic to send the message to.
     * @param msg Message to send.
     * @param plc Type of processing.
     * @param timeout Timeout to keep a message on receiving queue.
     * @param skipOnTimeout Whether message can be skipped on timeout.
     * @param ackC Ack closure.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    public void sendOrderedMessage(
        ClusterNode node,
        Object topic,
        Message msg,
        byte plc,
        long timeout,
        boolean skipOnTimeout,
        IgniteInClosure<IgniteException> ackC
    ) throws IgniteCheckedException {
        assert timeout > 0 || skipOnTimeout;

        send(node, topic, (byte)-1, msg, plc, true, timeout, skipOnTimeout, ackC, false);
    }

    /**
     * Sends a peer deployable user message.
     *
     * @param nodes Destination nodes.
     * @param msg Message to send.
     * @param topic Message topic to use.
     * @param ordered Is message ordered?
     * @param timeout Message timeout in milliseconds for ordered messages.
     * @param async Async flag.
     * @throws IgniteCheckedException Thrown in case of any errors.
     */
    @SuppressWarnings("ConstantConditions")
    public void sendUserMessage(Collection<? extends ClusterNode> nodes,
        Object msg,
        @Nullable Object topic,
        boolean ordered,
        long timeout,
        boolean async) throws IgniteCheckedException
    {
        boolean loc = nodes.size() == 1 && F.first(nodes).id().equals(locNodeId);

        byte[] serMsg = null;
        byte[] serTopic = null;

        if (!loc) {
            serMsg = U.marshal(marsh, msg);

            if (topic != null)
                serTopic = U.marshal(marsh, topic);
        }

        GridDeployment dep = null;

        String depClsName = null;

        if (ctx.config().isPeerClassLoadingEnabled()) {
            Class<?> cls0 = U.detectClass(msg);

            if (U.isJdk(cls0) && topic != null)
                cls0 = U.detectClass(topic);

            dep = ctx.deploy().deploy(cls0, U.detectClassLoader(cls0));

            if (dep == null)
                throw new IgniteDeploymentCheckedException("Failed to deploy user message: " + msg);

            depClsName = cls0.getName();
        }

        Message ioMsg = new GridIoUserMessage(
            msg,
            serMsg,
            depClsName,
            topic,
            serTopic,
            dep != null ? dep.classLoaderId() : null,
            dep != null ? dep.deployMode() : null,
            dep != null ? dep.userVersion() : null,
            dep != null ? dep.participants() : null);

        if (ordered)
            sendOrderedMessageToGridTopic(nodes, TOPIC_COMM_USER, ioMsg, PUBLIC_POOL, timeout, true);
        else if (loc) {
            send(F.first(nodes),
                TOPIC_COMM_USER,
                TOPIC_COMM_USER.ordinal(),
                ioMsg,
                PUBLIC_POOL,
                false,
                0,
                false,
                null,
                async
            );
        }
        else {
            ClusterNode locNode = F.find(nodes, null, F.localNode(locNodeId));

            Collection<? extends ClusterNode> rmtNodes = F.view(nodes, F.remoteNodes(locNodeId));

            if (!rmtNodes.isEmpty())
                sendToGridTopic(rmtNodes, TOPIC_COMM_USER, ioMsg, PUBLIC_POOL);

            // Will call local listeners in current thread synchronously or through pool,
            // depending async flag, so must go the last
            // to allow remote nodes execute the requested operation in parallel.
            if (locNode != null) {
                send(locNode,
                    TOPIC_COMM_USER,
                    TOPIC_COMM_USER.ordinal(),
                    ioMsg,
                    PUBLIC_POOL,
                    false,
                    0,
                    false,
                    null,
                    async
                );
            }
        }
    }

    public void addUserMessageListener(final @Nullable Object topic, final @Nullable IgniteBiPredicate<UUID, ?> p) {
        addUserMessageListener(topic, p, ctx.localNodeId());
    }

    /**
     * @param topic Topic to subscribe to.
     * @param p Message predicate.
     */
    public void addUserMessageListener(
        final @Nullable Object topic,
        final @Nullable IgniteBiPredicate<UUID, ?> p,
        final UUID nodeId
    ) {
        if (p != null) {
            try {
                if (p instanceof PlatformMessageFilter)
                    ((PlatformMessageFilter)p).initialize(ctx);
                else
                    ctx.resource().injectGeneric(p);

                addMessageListener(TOPIC_COMM_USER,
                    new GridUserMessageListener(topic, (IgniteBiPredicate<UUID, Object>)p, nodeId));
            }
            catch (IgniteCheckedException e) {
                throw new IgniteException(e);
            }
        }
    }

    /**
     * @param topic Topic to unsubscribe from.
     * @param p Message predicate.
     */
    public void removeUserMessageListener(@Nullable Object topic, IgniteBiPredicate<UUID, ?> p) {
        removeMessageListener(TOPIC_COMM_USER,
            new GridUserMessageListener(topic, (IgniteBiPredicate<UUID, Object>)p));
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to add.
     */
    public void addMessageListener(GridTopic topic, GridMessageListener lsnr) {
        addMessageListener((Object)topic, lsnr);
    }

    /**
     * @param lsnr Listener to add.
     */
    public void addDisconnectListener(GridDisconnectListener lsnr) {
        disconnectLsnrs.add(lsnr);
    }

    /**
     * @param lsnr Listener to remove.
     */
    public void removeDisconnectListener(GridDisconnectListener lsnr) {
        disconnectLsnrs.remove(lsnr);
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to add.
     */
    public void addMessageListener(Object topic, final GridMessageListener lsnr) {
        assert lsnr != null;
        assert topic != null;

        // Make sure that new topic is not in the list of closed topics.
        closedTopics.remove(topic);

        GridMessageListener lsnrs;

        for (;;) {
            lsnrs = listenerPutIfAbsent0(topic, lsnr);

            if (lsnrs == null) {
                lsnrs = lsnr;

                break;
            }

            assert lsnrs != null;

            if (!(lsnrs instanceof ArrayListener)) { // We are putting the second listener, creating array.
                GridMessageListener arrLsnr = new ArrayListener(lsnrs, lsnr);

                if (listenerReplace0(topic, lsnrs, arrLsnr)) {
                    lsnrs = arrLsnr;

                    break;
                }
            }
            else {
                if (((ArrayListener)lsnrs).add(lsnr))
                    break;

                // Add operation failed because array is already empty and is about to be removed, helping and retrying.
                listenerRemove0(topic, lsnrs);
            }
        }

        Map<UUID, GridCommunicationMessageSet> map = msgSetMap.get(topic);

        Collection<GridCommunicationMessageSet> msgSets = map != null ? map.values() : null;

        if (msgSets != null) {
            final GridMessageListener lsnrs0 = lsnrs;

            try {
                for (final GridCommunicationMessageSet msgSet : msgSets) {
                    pools.poolForPolicy(msgSet.policy()).execute(
                        new Runnable() {
                            @Override public void run() {
                                unwindMessageSet(msgSet, lsnrs0);
                            }
                        });
                }
            }
            catch (RejectedExecutionException e) {
                U.error(log, "Failed to process delayed message due to execution rejection. Increase the upper bound " +
                    "on executor service provided in 'IgniteConfiguration.getPublicThreadPoolSize()'). Will attempt to " +
                    "process message in the listener thread instead.", e);

                for (GridCommunicationMessageSet msgSet : msgSets)
                    unwindMessageSet(msgSet, lsnr);
            }
            catch (IgniteCheckedException ice) {
                throw new IgniteException(ice);
            }
        }
    }

    /**
     * @param topic Message topic.
     * @return Whether or not listener was indeed removed.
     */
    public boolean removeMessageListener(GridTopic topic) {
        return removeMessageListener((Object)topic);
    }

    /**
     * @param topic Message topic.
     * @return Whether or not listener was indeed removed.
     */
    public boolean removeMessageListener(Object topic) {
        return removeMessageListener(topic, null);
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to remove.
     * @return Whether or not the lsnr was removed.
     */
    public boolean removeMessageListener(GridTopic topic, @Nullable GridMessageListener lsnr) {
        return removeMessageListener((Object)topic, lsnr);
    }

    /**
     * @param topic Listener's topic.
     * @param lsnr Listener to remove.
     * @return Whether or not the lsnr was removed.
     */
    public boolean removeMessageListener(Object topic, @Nullable GridMessageListener lsnr) {
        assert topic != null;

        boolean rmv = true;

        Collection<GridCommunicationMessageSet> msgSets = null;

        // If listener is null, then remove all listeners.
        if (lsnr == null) {
            closedTopics.add(topic);

            lsnr = listenerRemove0(topic);

            rmv = lsnr != null;

            Map<UUID, GridCommunicationMessageSet> map = msgSetMap.remove(topic);

            if (map != null)
                msgSets = map.values();
        }
        else {
            for (;;) {
                GridMessageListener lsnrs = listenerGet0(topic);

                // If removing listener before subscription happened.
                if (lsnrs == null) {
                    closedTopics.add(topic);

                    Map<UUID, GridCommunicationMessageSet> map = msgSetMap.remove(topic);

                    if (map != null)
                        msgSets = map.values();

                    rmv = false;

                    break;
                }
                else {
                    boolean empty = false;

                    if (!(lsnrs instanceof ArrayListener)) {
                        if (lsnrs.equals(lsnr)) {
                            if (!listenerRemove0(topic, lsnrs))
                                continue; // Retry because it can be packed to array listener.

                            empty = true;
                        }
                        else
                            rmv = false;
                    }
                    else {
                        ArrayListener arrLsnr = (ArrayListener)lsnrs;

                        if (arrLsnr.remove(lsnr))
                            empty = arrLsnr.isEmpty();
                        else
                            // Listener was not found.
                            rmv = false;

                        if (empty)
                            listenerRemove0(topic, lsnrs);
                    }

                    // If removing last subscribed listener.
                    if (empty) {
                        closedTopics.add(topic);

                        Map<UUID, GridCommunicationMessageSet> map = msgSetMap.remove(topic);

                        if (map != null)
                            msgSets = map.values();
                    }

                    break;
                }
            }
        }

        if (msgSets != null)
            for (GridCommunicationMessageSet msgSet : msgSets)
                ctx.timeout().removeTimeoutObject(msgSet);

        if (rmv && log.isDebugEnabled())
            log.debug("Removed message listener [topic=" + topic + ", lsnr=" + lsnr + ']');

        if (lsnr instanceof ArrayListener) {
            for (GridMessageListener childLsnr : ((ArrayListener)lsnr).arr)
                closeListener(childLsnr);
        }
        else
            closeListener(lsnr);

        return rmv;
    }

    /**
     * Closes a listener, if applicable.
     *
     * @param lsnr Listener.
     */
    private void closeListener(GridMessageListener lsnr) {
        if (lsnr instanceof GridUserMessageListener) {
            GridUserMessageListener userLsnr = (GridUserMessageListener)lsnr;

            if (userLsnr.predLsnr instanceof PlatformMessageFilter)
                ((PlatformMessageFilter)userLsnr.predLsnr).onClose();
        }
    }

    /**
     * Gets sent messages count.
     *
     * @return Sent messages count.
     */
    public int getSentMessagesCount() {
        return getSpi().getSentMessagesCount();
    }

    /**
     * Gets sent bytes count.
     *
     * @return Sent bytes count.
     */
    public long getSentBytesCount() {
        return getSpi().getSentBytesCount();
    }

    /**
     * Gets received messages count.
     *
     * @return Received messages count.
     */
    public int getReceivedMessagesCount() {
        return getSpi().getReceivedMessagesCount();
    }

    /**
     * Gets received bytes count.
     *
     * @return Received bytes count.
     */
    public long getReceivedBytesCount() {
        return getSpi().getReceivedBytesCount();
    }

    /**
     * Gets outbound messages queue size.
     *
     * @return Outbound messages queue size.
     */
    public int getOutboundMessagesQueueSize() {
        return getSpi().getOutboundMessagesQueueSize();
    }

    /**
     * @param rctx Receiver context to use.
     * @param ex Exception to close receiver with.
     */
    private void interruptRecevier(ReceiverContext rctx, Exception ex) {
        if (rctx == null)
            return;

        if (rctx.interrupted.compareAndSet(false, true)) {
            if (rctx.timeoutObj != null)
                ctx.timeout().removeTimeoutObject(rctx.timeoutObj);

            U.closeQuiet(rctx.rcv);

            rctx.lastState = rctx.lastState == null ?
                new TransmissionMeta(ex) : rctx.lastState.error(ex);

            rctx.hnd.onException(rctx.rmtNodeId, ex);

            U.error(log, "Receiver has been interrupted due to an exception occurred [ctx=" + rctx + ']', ex);
        }
    }

    /**
     * @param topic Topic to which the channel is created.
     * @param rmtNodeId Remote node id.
     * @param initMsg Channel initialization message with additional params.
     * @param ch Channel instance.
     */
    private void processOpenedChannel(Object topic, UUID rmtNodeId, SessionChannelMessage initMsg, SocketChannel ch) {
        ReceiverContext rcvCtx = null;
        ObjectInputStream in = null;
        ObjectOutputStream out = null;

        try {
            if (stopping) {
                throw new NodeStoppingException("Local node is stopping. Channel will be closed [topic=" + topic +
                    ", channel=" + ch + ']');
            }

            if (initMsg == null || initMsg.sesId() == null) {
                U.warn(log, "There is no initial message provied for given topic. Opened channel will be closed " +
                    "[rmtNodeId=" + rmtNodeId + ", topic=" + topic + ", initMsg=" + initMsg + ']');

                return;
            }

            configureChannel(ch, netTimeoutMs);

            in = new ObjectInputStream(ch.socket().getInputStream());
            out = new ObjectOutputStream(ch.socket().getOutputStream());

            IgniteUuid newSesId = initMsg.sesId();

            synchronized (rcvMux) {
                TransmissionHandler hnd = topicTransmissionHnds.get(topic);

                if (hnd == null) {
                    U.warn(log, "There is no handler for a given topic. Channel will be closed [rmtNodeId=" + rmtNodeId +
                        ", topic=" + topic + ']');

                    return;
                }

                rcvCtx = rcvCtxs.computeIfAbsent(topic, t -> new ReceiverContext(rmtNodeId, hnd, newSesId));
            }

            // Do not allow multiple connections for the same session.
            if (!newSesId.equals(rcvCtx.sesId)) {
                IgniteCheckedException err = new IgniteCheckedException("Requested topic is busy by another transmission. " +
                    "It's not allowed to process different sessions over the same topic simultaneously. " +
                    "Channel will be closed [initMsg=" + initMsg + ", channel=" + ch + ", nodeId=" + rmtNodeId + ']');

                U.error(log, err);

                out.writeObject(new TransmissionMeta(err));

                return;
            }

            if (log.isDebugEnabled()) {
                log.debug("Trasmission open a new channel [rmtNodeId=" + rmtNodeId + ", topic=" + topic +
                    ", initMsg=" + initMsg + ']');
            }

            if (!rcvCtx.lock.tryLock(netTimeoutMs, TimeUnit.MILLISECONDS))
                throw new IgniteException("Wait for the previous receiver finished its work timeouted: " + rcvCtx);

            try {
                if (rcvCtx.timeoutObj != null)
                    ctx.timeout().removeTimeoutObject(rcvCtx.timeoutObj);

                // Send previous context state to sync remote and local node.
                out.writeObject(rcvCtx.lastState == null ? new TransmissionMeta() : rcvCtx.lastState);

                if (rcvCtx.lastState == null || rcvCtx.lastState.error() == null)
                    receiveFromChannel(topic, rcvCtx, in, out, ch);
                else
                    interruptRecevier(rcvCtxs.remove(topic), rcvCtx.lastState.error());
            }
            finally {
                rcvCtx.lock.unlock();
            }
        }
        catch (Throwable t) {
            U.error(log, "Download session cannot be finished due to an unexpected error [ctx=" + rcvCtx + ']', t);

            // Do not remove receiver context here, since sender will recconect to get this error.
            interruptRecevier(rcvCtx, new IgniteCheckedException("Channel processing error [nodeId=" + rmtNodeId + ']', t));
        }
        finally {
            U.closeQuiet(in);
            U.closeQuiet(out);
            U.closeQuiet(ch);
        }
    }

    /**
     * @param topic Topic handler related to.
     * @param rcvCtx Receiver read context.
     * @throws NodeStoppingException If processing fails.
     * @throws InterruptedException If thread interrupted.
     */
    private void receiveFromChannel(
        Object topic,
        ReceiverContext rcvCtx,
        ObjectInputStream in,
        ObjectOutputStream out,
        ReadableByteChannel ch
    ) throws NodeStoppingException, InterruptedException {
        try {
            while (true) {
                if (Thread.currentThread().isInterrupted())
                    throw new InterruptedException("The thread has been interrupted. Stop downloading file.");

                if (stopping)
                    throw new NodeStoppingException("Operation has been cancelled (node is stopping)");

                boolean exit = in.readBoolean();

                if (exit) {
                    ReceiverContext rcv = rcvCtxs.remove(topic);

                    assert rcv != null;

                    rcv.hnd.onEnd(rcv.rmtNodeId);

                    break;
                }

                TransmissionMeta meta = (TransmissionMeta)in.readObject();

                if (rcvCtx.rcv == null) {
                    rcvCtx.rcv = createReceiver(rcvCtx.rmtNodeId,
                        rcvCtx.hnd,
                        meta,
                        () -> stopping || rcvCtx.interrupted.get());

                    rcvCtx.lastState = meta;
                }

                validate(rcvCtx.lastState, meta);

                try {
                    long startTime = U.currentTimeMillis();

                    rcvCtx.rcv.receive(ch);

                    // Write processing ack.
                    out.writeBoolean(true);
                    out.flush();

                    rcvCtx.rcv.close();

                    U.log(log, "File has been received " +
                        "[name=" + rcvCtx.rcv.state().name() + ", transferred=" + rcvCtx.rcv.transferred() +
                        ", time=" + (double)((U.currentTimeMillis() - startTime) / 1000) + " sec" +
                        ", rmtId=" + rcvCtx.rmtNodeId + ']');

                    rcvCtx.rcv = null;
                }
                catch (Throwable e) {
                    rcvCtx.lastState = rcvCtx.rcv.state();

                    throw e;
                }
            }
        }
        catch (ClassNotFoundException e) {
            throw new IgniteException(e);
        }
        catch (IOException e) {
            // Waiting for re-establishing connection.
            U.warn(log, "Сonnection from the remote node lost. Will wait for the new one to continue file " +
                "receive [nodeId=" + rcvCtx.rmtNodeId + ", sesKey=" + rcvCtx.sesId + ']', e);

            long startTs = U.currentTimeMillis();

            boolean added = ctx.timeout().addTimeoutObject(rcvCtx.timeoutObj = new GridTimeoutObject() {
                @Override public IgniteUuid timeoutId() {
                    return rcvCtx.sesId;
                }

                @Override public long endTime() {
                    return startTs + netTimeoutMs;
                }

                @Override public void onTimeout() {
                    interruptRecevier(rcvCtxs.remove(topic), new IgniteCheckedException("Receiver is closed due to " +
                        "waiting for the reconnect has been timeouted"));
                }
            });

            assert added;
        }
    }

    /**
     * @param prev Previous available transmission meta.
     * @param next Next transmission meta.
     */
    private void validate(TransmissionMeta prev, TransmissionMeta next) {
        A.ensure(prev.name().equals(next.name()), "Attempt to load different file " +
            "[prev=" + prev + ", next=" + next + ']');

        A.ensure(prev.offset() == next.offset(),
            "The next chunk offest is incorrect [prev=" + prev + ", meta=" + next + ']');

        A.ensure(prev.count() == next.count(), " The count of bytes to transfer for " +
            "the next chunk is incorrect [prev=" + prev + ", next=" + next + ']');

        A.ensure(prev.policy() == next.policy(), "Attemt to continue file upload with" +
            " different transmission policy [prev=" + prev + ", next=" + next + ']');
    }

    /**
     * @param nodeId Remote node id.
     * @param hnd Current handler instance which produces file handlers.
     * @param meta Meta information about file pending to receive to create appropriate receiver.
     * @param stopChecker Process interrupt checker.
     * @return Chunk data recevier.
     */
    private TransmissionReceiver createReceiver(
        UUID nodeId,
        TransmissionHandler hnd,
        TransmissionMeta meta,
        BooleanSupplier stopChecker
    ) {
        switch (meta.policy()) {
            case FILE:
                return new FileReceiver(
                    meta,
                    DFLT_CHUNK_SIZE_BYTES,
                    stopChecker,
                    fileIoFactory,
                    hnd.fileHandler(nodeId, meta),
                    hnd.filePath(nodeId, meta),
                    log);

            case CHUNK:
                return new ChunkReceiver(
                    meta,
                    ctx.config()
                        .getDataStorageConfiguration()
                        .getPageSize(),
                    stopChecker,
                    hnd.chunkHandler(nodeId, meta),
                    log);

            default:
                throw new IllegalStateException("The type of transmission policy is unknown. An implementation " +
                    "required: " + meta.policy());
        }
    }

    /**
     * @param channel Socket channel to configure blocking mode.
     * @param timeout Ignite network configuration timeout.
     * @throws IOException If fails.
     */
    private static void configureChannel(SocketChannel channel, int timeout) throws IOException {
        // Timeout must be enabled prior to entering the blocking mode to have effect.
        channel.socket().setSoTimeout(timeout);
        channel.configureBlocking(true);
    }

    /**
     * Dumps SPI stats to diagnostic logs in case TcpCommunicationSpi is used, no-op otherwise.
     */
    public void dumpStats() {
        CommunicationSpi spi = getSpi();

        if (spi instanceof TcpCommunicationSpi)
            ((TcpCommunicationSpi)spi).dumpStats();
    }

    /** {@inheritDoc} */
    @Override public void printMemoryStats() {
        X.println(">>>");
        X.println(">>> IO manager memory stats [igniteInstanceName=" + ctx.igniteInstanceName() + ']');
        X.println(">>>  lsnrMapSize: " + lsnrMap.size());
        X.println(">>>  msgSetMapSize: " + msgSetMap.size());
        X.println(">>>  closedTopicsSize: " + closedTopics.sizex());
        X.println(">>>  discoWaitMapSize: " + waitMap.size());
    }

    /**
     * Read context holds all the information about current transfer read from channel process.
     */
    private static class ReceiverContext {
        /** The remote node input channel came from. */
        private final UUID rmtNodeId;

        /** Current sesssion handler. */
        @GridToStringExclude
        private final TransmissionHandler hnd;

        /** Unique session request id. */
        private final IgniteUuid sesId;

        /** Flag indicates that current file handling process must be interrupted. */
        private final AtomicBoolean interrupted = new AtomicBoolean();

        /** Only one thread can handle receiver context. */
        private final Lock lock = new ReentrantLock();

        /** Last infinished downloading object. */
        private TransmissionReceiver rcv;

        /** Last saved state about file data processing. */
        private TransmissionMeta lastState;

        /** Close receiver timeout object. */
        private GridTimeoutObject timeoutObj;

        /**
         * @param rmtNodeId Remote node id.
         * @param hnd Channel handler of current topic.
         * @param sesId Unique session request id.
         */
        public ReceiverContext(UUID rmtNodeId, TransmissionHandler hnd, IgniteUuid sesId) {
            this.rmtNodeId = rmtNodeId;
            this.hnd = hnd;
            this.sesId = sesId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(ReceiverContext.class, this);
        }
    }

    /**
     * Сlass represents an implementation of transmission file writer. Each new instance of transmission sender
     * will establish a new connection with unique transmission session identifier to the remote node and given
     * topic (an arbitraty {@link GridTopic} can be used).
     *
     * <h2>Zero-copy approach</h2>
     * <p>
     * Current implementation of transmission sender is based on file zero-copy approach (the {@link FileSender}
     * is used under the hood). It is much more efficient than a simple loop that reads data from
     * given file and writes it to the target socket channel. But if operating system does not support zero-copy
     * file transfer, sending a file with {@link TransmissionSender} might fail or yield worse performance.
     * <p>
     * Please, refer to <a href="http://en.wikipedia.org/wiki/Zero-copy">http://en.wikipedia.org/wiki/Zero-copy</a>
     * or {@link FileChannel#transferTo(long, long, WritableByteChannel)} for details of such approach.
     *
     * <h2>File and Chunk handlers</h2>
     * <p>
     * It is possible to choose a file handler prior to sendig the file to remote node  within opened transmission
     * session. There are two types of handlers available:
     * {@link TransmissionHandler#chunkHandler(UUID, TransmissionMeta)} and
     * {@link TransmissionHandler#fileHandler(UUID, TransmissionMeta)}. You can use an appropriate
     * {@link TransmissionPolicy} for {@link #send(File, long, long, Map, TransmissionPolicy)} method to switch
     * between them.
     *
     * <h2>Exceptions handling</h2>
     * <p>
     * Each transmission can have two different high-level types of exception which are handled differently:
     * <ul>
     * <li><em>transport</em> exception(e.g. some network issues)</li>
     * <li><em>application</em>\<em>handler</em> level exception</li>
     * </ul>
     *
     * <h3><em>Application</em> exceptions</h3>
     * <p>
     * The transmission will be stopped immediately and wrapping <em>IgniteExcpetion</em> thrown in case of
     * any <em>application</em> exception occured.
     *
     * <h3><em>Transport</em> exceptions</h3>
     * <p>
     * All transport level exceptions of transmission file sender will require transmission to be reconnected.
     * For instance, when the local node closes the socket connection in orderly way, but the file is not fully
     * handled by remote node, the read operation over the same socket endpoint will return <tt>-1</tt>. Such
     * result will be consideread as an {@link IOException} by handler and it will wait for reestablishing connection
     * to continue file loading.
     * <p>
     * Another example, the transmission sender gets the <em>Connection reset by peer</em> IOException message.
     * This means that the remote node you are connected to has to reset the connection. This is usually caused by a
     * high amount of traffic on the host, but may be caused by a server error or the remote node has exhausted
     * system resources as well. Such {@link IOException} will be considered as <em>reconnection required</em>.
     *
     * <h3>Timeout exceptions</h3>
     * <p>
     * For read operations over the {@link InputStream} or write operation through the {@link OutputStream}
     * the {@link Socket#setSoTimeout(int)} will be used and an {@link SocketTimeoutException} will be
     * thrown when the timeout occured. The default value is taken from {@link IgniteConfiguration#getNetworkTimeout()}.
     * <p>
     * If reconnection is not occurred withing configured timeout interval the timeout object will be fired which
     * clears corresponding to the used topic the {@link GridIoManager.ReceiverContext}.
     *
     * <h2>Release resources</h2>
     * <p>
     * It is important to call <em>close()</em> method or use <em>try-with-resource</em> statement to release
     * all resources once you've done with sending files.
     *
     * @see FileChannel#transferTo(long, long, WritableByteChannel)
     */
    public class TransmissionSender implements Closeable {
        /** Remote node id to connect to. */
        private final UUID rmtId;

        /** Remote topic to connect to. */
        private final Object topic;

        /** Current unique session identifier to transfer files to remote node. */
        private T2<UUID, IgniteUuid> sesKey;

        /** Instance of opened writable channel to work with. */
        private WritableByteChannel channel;

        /** Decorated with data operations socket of output channel. */
        private ObjectOutput out;

        /** Decorated with data operations socket of input channel. */
        private ObjectInput in;

        /**
         * @param rmtId The remote node to connect to.
         * @param topic The remote topic to connect to.
         */
        public TransmissionSender(
            UUID rmtId,
            Object topic
        ) {
            this.rmtId = rmtId;
            this.topic = topic;
            sesKey = new T2<>(rmtId, IgniteUuid.randomUuid());
        }

        /**
         * @return The synchronization meta if case connection has been reset.
         * @throws IgniteCheckedException If fails.
         * @throws IOException If fails.
         */
        private TransmissionMeta connect() throws IgniteCheckedException, IOException {
            SocketChannel channel = (SocketChannel)openChannel(rmtId,
                topic,
                new SessionChannelMessage(sesKey.get2()))
                .get();

            configureChannel(channel, netTimeoutMs);

            this.channel = (WritableByteChannel)channel;
            out = new ObjectOutputStream(channel.socket().getOutputStream());
            in = new ObjectInputStream(channel.socket().getInputStream());

            TransmissionMeta syncMeta;

            try {
                // Synchronize state between remote and local nodes.
                syncMeta = (TransmissionMeta)in.readObject();
            }
            catch (ClassNotFoundException e) {
                throw new IgniteException(e);
            }

            return syncMeta;
        }

        /**
         * @param file Source file to send to remote.
         * @param params Additional file params.
         * @param plc The policy of handling data on remote.
         * @throws IgniteCheckedException If fails.
         */
        public void send(
            File file,
            Map<String, Serializable> params,
            TransmissionPolicy plc
        ) throws IgniteCheckedException, InterruptedException, IOException {
            send(file, 0, file.length(), params, plc);
        }

        /**
         * @param file Source file to send to remote.
         * @param plc The policy of handling data on remote.
         * @throws IgniteCheckedException If fails.
         */
        public void send(
            File file,
            TransmissionPolicy plc
        ) throws IgniteCheckedException, InterruptedException, IOException {
            send(file, 0, file.length(), new HashMap<>(), plc);
        }

        /**
         * @param file Source file to send to remote.
         * @param offset Position to start trasfer at.
         * @param cnt Number of bytes to transfer.
         * @param params Additional file params.
         * @param plc The policy of handling data on remote.
         * @throws IgniteCheckedException If fails.
         */
        public void send(
            File file,
            long offset,
            long cnt,
            Map<String, Serializable> params,
            TransmissionPolicy plc
        ) throws IgniteCheckedException, InterruptedException, IOException {
            long startTime = U.currentTimeMillis();
            int retries = 0;

            senderStopFlags.putIfAbsent(sesKey, new AtomicBoolean());

            try (FileSender snd = new FileSender(file,
                offset,
                cnt,
                params,
                plc,
                () -> stopping || senderStopFlags.get(sesKey).get(),
                log,
                fileIoFactory,
                DFLT_CHUNK_SIZE_BYTES)
            ) {
                if (log.isDebugEnabled()) {
                    log.debug("Start writing file to remote node [file=" + file.getName() +
                        ", rmtNodeId=" + rmtId + ", topic=" + topic + ']');
                }

                while (true) {
                    if (Thread.currentThread().isInterrupted())
                        throw new InterruptedException("The thread has been interrupted. Stop uploading file.");

                    if (stopping)
                        throw new NodeStoppingException("Operation has been cancelled (node is stopping)");

                    try {
                        TransmissionMeta rcvMeta = null;

                        // In/out streams are not null if file has been sent successfully.
                        if (out == null && in == null) {
                            rcvMeta = connect();

                            assert rcvMeta != null : "Remote receiver has not sent its meta";

                            // Stop in case of any error occurred on remote node during file processing.
                            if (rcvMeta.error() != null)
                                throw rcvMeta.error();
                        }

                        snd.send(channel, out, rcvMeta);

                        // Read file received acknowledge.
                        boolean written = in.readBoolean();

                        assert written : "File is not fully written: " + file.getAbsolutePath();

                        break;
                    }
                    catch (IOException e) {
                        closeChannelQuiet();

                        retries++;

                        if (retries >= retryCnt) {
                            throw new IgniteException("The number of retry attempts to upload file exceeded " +
                                "the limit: " + retryCnt, e);
                        }

                        // Re-establish the new connection to continue upload.
                        U.warn(log, "Connection lost while writing a file to remote node and " +
                            "will be reestablished [rmtId=" + rmtId + ", file=" + file.getName() +
                            ", sesKey=" + sesKey + ", retries=" + retries +
                            ", transferred=" + snd.transferred() +
                            ", total=" + snd.state().count() + ']', e);
                    }
                }

                U.log(log, "File has been sent to remote node [name=" + file.getName() +
                    ", uploadTime=" + (double)((U.currentTimeMillis() - startTime) / 1000) + " sec, retries=" + retries +
                    ", transferred=" + snd.transferred() + ", rmtId=" + rmtId + ']');

            }
            catch (InterruptedException e) {
                closeChannelQuiet();

                throw e;
            }
            catch (IgniteCheckedException e) {
                closeChannelQuiet();

                if (X.hasCause(e, TransmissionCancelledException.class)) {
                    throw new TransmissionCancelledException("File transmission has been cancelled on the remote node " +
                        "[rmtId=" + rmtId + ", file=" + file.getName() + ", sesKey=" + sesKey + ", retries=" + retries +
                        ", cause='" + e.getCause(TransmissionCancelledException.class).getMessage() + "']");
                }

                throw new IgniteCheckedException("Exception while sending file [rmtId=" + rmtId +
                    ", file=" + file.getName() + ", sesKey=" + sesKey + ", retries=" + retries + ']', e);
            }
            catch (Throwable e) {
                closeChannelQuiet();

                if (stopping)
                    throw new NodeStoppingException("Operation has been cancelled (node is stopping)");

                if (senderStopFlags.get(sesKey).get())
                    throw new ClusterTopologyCheckedException("Remote node left the cluster: " + rmtId, e);

                throw new IgniteException("Unexpected exception while sending file to the remote node. The process stopped " +
                    "[rmtId=" + rmtId + ", file=" + file.getName() + ", sesKey=" + sesKey + ']', e);
            }
        }

        /** {@inheritDoc} */
        @Override public void close() throws IOException {
            try {
                senderStopFlags.remove(sesKey);

                ObjectOutput out0 = out;

                if (out0 == null)
                    return;

                U.log(log, "Close file writer session: " + sesKey);

                // Send transmission close flag.
                out0.writeBoolean(true);
                out0.flush();
            }
            catch (IOException e) {
                U.warn(log, "An exception while writing close session flag occured. " +
                    " Session close operation has been ignored", e);
            }
            finally {
                closeChannelQuiet();
            }
        }

        /** Close channel and relese resources. */
        private void closeChannelQuiet() {
            U.closeQuiet(out);
            U.closeQuiet(in);
            U.closeQuiet(channel);

            out = null;
            in = null;
            channel = null;
        }
    }

    /**
     * Linked chain of listeners.
     */
    private static class ArrayListener implements GridMessageListener {
        /** */
        private volatile GridMessageListener[] arr;

        /**
         * @param arr Array of listeners.
         */
        ArrayListener(GridMessageListener... arr) {
            this.arr = arr;
        }

        /**
         * Passes message to the whole chain.
         *
         * @param nodeId Node ID.
         * @param msg Message.
         */
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            GridMessageListener[] arr0 = arr;

            if (arr0 == null)
                return;

            for (GridMessageListener l : arr0)
                l.onMessage(nodeId, msg, plc);
        }

        /**
         * @return {@code true} If this instance is empty.
         */
        boolean isEmpty() {
            return arr == null;
        }

        /**
         * @param l Listener.
         * @return {@code true} If listener was removed.
         */
        synchronized boolean remove(GridMessageListener l) {
            GridMessageListener[] arr0 = arr;

            if (arr0 == null)
                return false;

            if (arr0.length == 1) {
                if (!arr0[0].equals(l))
                    return false;

                arr = null;

                return true;
            }

            for (int i = 0; i < arr0.length; i++) {
                if (arr0[i].equals(l)) {
                    int newLen = arr0.length - 1;

                    if (i == newLen) // Remove last.
                        arr = Arrays.copyOf(arr0, newLen);
                    else {
                        GridMessageListener[] arr1 = new GridMessageListener[newLen];

                        if (i != 0) // Not remove first.
                            System.arraycopy(arr0, 0, arr1, 0, i);

                        System.arraycopy(arr0, i + 1, arr1, i, newLen - i);

                        arr = arr1;
                    }

                    return true;
                }
            }

            return false;
        }

        /**
         * @param l Listener.
         * @return {@code true} if listener was added. Add can fail if this instance is empty and is about to be removed
         *         from map.
         */
        synchronized boolean add(GridMessageListener l) {
            GridMessageListener[] arr0 = arr;

            if (arr0 == null)
                return false;

            int oldLen = arr0.length;

            arr0 = Arrays.copyOf(arr0, oldLen + 1);

            arr0[oldLen] = l;

            arr = arr0;

            return true;
        }
    }

    /**
     * This class represents a message listener wrapper that knows about peer deployment.
     */
    private class GridUserMessageListener implements GridMessageListener {
        /** Predicate listeners. */
        private final IgniteBiPredicate<UUID, Object> predLsnr;

        /** User message topic. */
        private final Object topic;

        /** Initial node id. */
        private final UUID initNodeId;

        /**
         * @param topic User topic.
         * @param predLsnr Predicate listener.
         * @param initNodeId Node id that registered given listener.
         */
        GridUserMessageListener(@Nullable Object topic, @Nullable IgniteBiPredicate<UUID, Object> predLsnr,
            @Nullable UUID initNodeId) {
            this.topic = topic;
            this.predLsnr = predLsnr;
            this.initNodeId = initNodeId;
        }

        /**
         * @param topic User topic.
         * @param predLsnr Predicate listener.
         */
        GridUserMessageListener(@Nullable Object topic, @Nullable IgniteBiPredicate<UUID, Object> predLsnr) {
            this(topic, predLsnr, null);
        }

        /** {@inheritDoc} */
        @SuppressWarnings({"ConstantConditions"
        })
        @Override public void onMessage(UUID nodeId, Object msg, byte plc) {
            if (!(msg instanceof GridIoUserMessage)) {
                U.error(log, "Received unknown message (potentially fatal problem): " + msg);

                return;
            }

            GridIoUserMessage ioMsg = (GridIoUserMessage)msg;

            ClusterNode node = ctx.discovery().node(nodeId);

            if (node == null) {
                U.warn(log, "Failed to resolve sender node (did the node left grid?): " + nodeId);

                return;
            }

            Lock lock = busyLock.readLock();

            lock.lock();

            try {
                if (stopping) {
                    if (log.isDebugEnabled())
                        log.debug("Received user message while stopping (will ignore) [nodeId=" +
                            nodeId + ", msg=" + msg + ']');

                    return;
                }

                Object msgBody = ioMsg.body();

                assert msgBody != null || ioMsg.bodyBytes() != null;

                try {
                    byte[] msgTopicBytes = ioMsg.topicBytes();

                    Object msgTopic = ioMsg.topic();

                    GridDeployment dep = ioMsg.deployment();

                    if (dep == null && ctx.config().isPeerClassLoadingEnabled() &&
                        ioMsg.deploymentClassName() != null) {
                        dep = ctx.deploy().getGlobalDeployment(
                            ioMsg.deploymentMode(),
                            ioMsg.deploymentClassName(),
                            ioMsg.deploymentClassName(),
                            ioMsg.userVersion(),
                            nodeId,
                            ioMsg.classLoaderId(),
                            ioMsg.loaderParticipants(),
                            null);

                        if (dep == null)
                            throw new IgniteDeploymentCheckedException(
                                "Failed to obtain deployment information for user message. " +
                                    "If you are using custom message or topic class, try implementing " +
                                    "GridPeerDeployAware interface. [msg=" + ioMsg + ']');

                        ioMsg.deployment(dep); // Cache deployment.
                    }

                    // Unmarshall message topic if needed.
                    if (msgTopic == null && msgTopicBytes != null) {
                        msgTopic = U.unmarshal(marsh, msgTopicBytes,
                            U.resolveClassLoader(dep != null ? dep.classLoader() : null, ctx.config()));

                        ioMsg.topic(msgTopic); // Save topic to avoid future unmarshallings.
                    }

                    if (!F.eq(topic, msgTopic))
                        return;

                    if (msgBody == null) {
                        msgBody = U.unmarshal(marsh, ioMsg.bodyBytes(),
                            U.resolveClassLoader(dep != null ? dep.classLoader() : null, ctx.config()));

                        ioMsg.body(msgBody); // Save body to avoid future unmarshallings.
                    }

                    // Resource injection.
                    if (dep != null)
                        ctx.resource().inject(dep, dep.deployedClass(ioMsg.deploymentClassName()), msgBody);
                }
                catch (IgniteCheckedException e) {
                    U.error(log, "Failed to unmarshal user message [node=" + nodeId + ", message=" +
                        msg + ']', e);
                }

                if (msgBody != null) {
                    if (predLsnr != null) {
                        try (OperationSecurityContext s = ctx.security().withContext(initNodeId)) {
                            if (!predLsnr.apply(nodeId, msgBody))
                                removeMessageListener(TOPIC_COMM_USER, this);
                        }
                    }
                }
            }
            finally {
                lock.unlock();
            }
        }

        /** {@inheritDoc} */
        @Override public boolean equals(Object o) {
            if (this == o)
                return true;

            if (o == null || getClass() != o.getClass())
                return false;

            GridUserMessageListener l = (GridUserMessageListener)o;

            return F.eq(predLsnr, l.predLsnr) && F.eq(topic, l.topic);
        }

        /** {@inheritDoc} */
        @Override public int hashCode() {
            int res = predLsnr != null ? predLsnr.hashCode() : 0;

            res = 31 * res + (topic != null ? topic.hashCode() : 0);

            return res;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GridUserMessageListener.class, this);
        }
    }

    /**
     * Ordered communication message set.
     */
    private class GridCommunicationMessageSet implements GridTimeoutObject {
        /** */
        private final UUID nodeId;

        /** */
        private long endTime;

        /** */
        private final IgniteUuid timeoutId;

        /** */
        @GridToStringInclude
        private final Object topic;

        /** */
        private final byte plc;

        /** */
        @GridToStringInclude
        private final Queue<OrderedMessageContainer> msgs = new ConcurrentLinkedDeque<>();

        /** */
        private final AtomicBoolean reserved = new AtomicBoolean();

        /** */
        private final long timeout;

        /** */
        private final boolean skipOnTimeout;

        /** */
        private long lastTs;

        /**
         * @param plc Communication policy.
         * @param topic Communication topic.
         * @param nodeId Node ID.
         * @param timeout Timeout.
         * @param skipOnTimeout Whether message can be skipped on timeout.
         * @param msg Message to add immediately.
         * @param msgC Message closure (may be {@code null}).
         */
        GridCommunicationMessageSet(
            byte plc,
            Object topic,
            UUID nodeId,
            long timeout,
            boolean skipOnTimeout,
            GridIoMessage msg,
            @Nullable IgniteRunnable msgC
        ) {
            assert nodeId != null;
            assert topic != null;
            assert msg != null;

            this.plc = plc;
            this.nodeId = nodeId;
            this.topic = topic;
            this.timeout = timeout == 0 ? ctx.config().getNetworkTimeout() : timeout;
            this.skipOnTimeout = skipOnTimeout;

            endTime = endTime(timeout);

            timeoutId = IgniteUuid.randomUuid();

            lastTs = U.currentTimeMillis();

            msgs.add(new OrderedMessageContainer(msg, lastTs, msgC, MTC.span()));
        }

        /** {@inheritDoc} */
        @Override public IgniteUuid timeoutId() {
            return timeoutId;
        }

        /** {@inheritDoc} */
        @Override public long endTime() {
            return endTime;
        }

        /** {@inheritDoc} */
        @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
        @Override public void onTimeout() {
            GridMessageListener lsnr = listenerGet0(topic);

            if (lsnr != null) {
                long delta = 0;

                if (skipOnTimeout) {
                    while (true) {
                        delta = 0;

                        boolean unwind = false;

                        synchronized (this) {
                            if (!msgs.isEmpty()) {
                                delta = U.currentTimeMillis() - lastTs;

                                if (delta >= timeout)
                                    unwind = true;
                            }
                        }

                        if (unwind)
                            unwindMessageSet(this, lsnr);
                        else
                            break;
                    }
                }

                // Someone is still listening to messages, so delay set removal.
                endTime = endTime(timeout - delta);

                ctx.timeout().addTimeoutObject(this);

                return;
            }

            if (log.isDebugEnabled())
                log.debug("Removing message set due to timeout: " + this);

            ConcurrentMap<UUID, GridCommunicationMessageSet> map = msgSetMap.get(topic);

            if (map != null) {
                boolean rmv;

                synchronized (map) {
                    rmv = map.remove(nodeId, this) && map.isEmpty();
                }

                if (rmv)
                    msgSetMap.remove(topic, map);
            }
        }

        /**
         * @return ID of node that sent the messages in the set.
         */
        UUID nodeId() {
            return nodeId;
        }

        /**
         * @return Communication policy.
         */
        byte policy() {
            return plc;
        }

        /**
         * @return Message topic.
         */
        Object topic() {
            return topic;
        }

        /**
         * @return {@code True} if successful.
         */
        boolean reserve() {
            return reserved.compareAndSet(false, true);
        }

        /**
         * @return {@code True} if set is reserved.
         */
        boolean reserved() {
            return reserved.get();
        }

        /**
         * Releases reservation.
         */
        void release() {
            assert reserved.get() : "Message set was not reserved: " + this;

            reserved.set(false);
        }

        /**
         * @param lsnr Listener to notify.
         */
        void unwind(GridMessageListener lsnr) {
            assert reserved.get();

            for (OrderedMessageContainer mc = msgs.poll(); mc != null; mc = msgs.poll()) {
                try (TraceSurroundings ignore = support(ctx.tracing().create(
                    COMMUNICATION_ORDERED_PROCESS, mc.parentSpan))) {
                    try {
                        OrderedMessageContainer fmc = mc;

                        MTC.span().addTag(SpanTags.MESSAGE, () -> traceName(fmc.message));

                        invokeListener(plc, lsnr, nodeId, mc.message.message(), secSubjId(mc.message));
                    }
                    finally {
                        if (mc.closure != null)
                            mc.closure.run();
                    }
                }
            }
        }

        /**
         * @param msg Message to add.
         * @param msgC Message closure (may be {@code null}).
         */
        void add(
            GridIoMessage msg,
            @Nullable IgniteRunnable msgC
        ) {
            msgs.add(new OrderedMessageContainer(msg, U.currentTimeMillis(), msgC, MTC.span()));
        }

        /**
         * @return {@code True} if set has messages to unwind.
         */
        boolean changed() {
            return !msgs.isEmpty();
        }

        /**
         * Calculates end time with overflow check.
         *
         * @param timeout Timeout in milliseconds.
         * @return End time in milliseconds.
         */
        private long endTime(long timeout) {
            long endTime = U.currentTimeMillis() + timeout;

            // Account for overflow.
            if (endTime < 0)
                endTime = Long.MAX_VALUE;

            return endTime;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(GridCommunicationMessageSet.class, this);
        }
    }

    /**
     * DTO for handling of communication message.
     */
    private static class OrderedMessageContainer {
        /** */
        GridIoMessage message;

        /** */
        long addedTime;

        /** */
        IgniteRunnable closure;

        /** */
        Span parentSpan;

        /**
         *
         * @param msg Received message.
         * @param addedTime Time of added to queue.
         * @param c Message closure.
         * @param parentSpan Span of process which added this message.
         */
        private OrderedMessageContainer(GridIoMessage msg, Long addedTime, IgniteRunnable c, Span parentSpan) {
            this.message = msg;
            this.addedTime = addedTime;
            this.closure = c;
            this.parentSpan = parentSpan;
        }
    }

    /**
     *
     */
    private static class ConcurrentHashMap0<K, V> extends ConcurrentHashMap<K, V> {
        /** */
        private static final long serialVersionUID = 0L;

        /** */
        private int hash;

        /**
         * @param o Object to be compared for equality with this map.
         * @return {@code True} only for {@code this}.
         */
        @Override public boolean equals(Object o) {
            return o == this;
        }

        /**
         * @return Identity hash code.
         */
        @Override public int hashCode() {
            if (hash == 0) {
                int hash0 = System.identityHashCode(this);

                hash = hash0 != 0 ? hash0 : -1;
            }

            return hash;
        }
    }

    /**
     *
     */
    private static class DelayedMessage {
        /** */
        private final UUID nodeId;

        /** */
        private final GridIoMessage msg;

        /** */
        private final IgniteRunnable msgC;

        /**
         * @param nodeId Node ID.
         * @param msg Message.
         * @param msgC Callback.
         */
        private DelayedMessage(UUID nodeId, GridIoMessage msg, IgniteRunnable msgC) {
            this.nodeId = nodeId;
            this.msg = msg;
            this.msgC = msgC;
        }

        /**
         * @return Message char.
         */
        public IgniteRunnable callback() {
            return msgC;
        }

        /**
         * @return Message.
         */
        public GridIoMessage message() {
            return msg;
        }

        /**
         * @return Node id.
         */
        public UUID nodeId() {
            return nodeId;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(DelayedMessage.class, this, super.toString());
        }
    }

    /**
     *
     */
    private class IoTestFuture extends GridFutureAdapter<List<IgniteIoTestMessage>> {
        /** */
        private final long id;

        /** */
        private final int cntr;

        /** */
        private final List<IgniteIoTestMessage> ress;

        /**
         * @param id ID.
         * @param cntr Counter.
         */
        IoTestFuture(long id, int cntr) {
            assert cntr > 0 : cntr;

            this.id = id;
            this.cntr = cntr;

            ress = new ArrayList<>(cntr);
        }

        /**
         *
         */
        void onResponse(IgniteIoTestMessage res) {
            boolean complete;

            synchronized (this) {
                ress.add(res);

                complete = cntr == ress.size();
            }

            if (complete)
                onDone(ress);
        }

        /** {@inheritDoc} */
        @Override public boolean onDone(List<IgniteIoTestMessage> res, @Nullable Throwable err) {
            if (super.onDone(res, err)) {
                ioTestMap().remove(id);

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return S.toString(IoTestFuture.class, this);
        }
    }

    /**
     *
     */
    private static class IoTestThreadLocalNodeResults {
        /** */
        private final long[] resLatency;

        /** */
        private final int rangesCnt;

        /** */
        private long totalLatency;

        /** */
        private long maxLatency;

        /** */
        private long maxLatencyTs;

        /** */
        private long maxReqSendQueueTime;

        /** */
        private long maxReqSendQueueTimeTs;

        /** */
        private long maxReqRcvQueueTime;

        /** */
        private long maxReqRcvQueueTimeTs;

        /** */
        private long maxResSendQueueTime;

        /** */
        private long maxResSendQueueTimeTs;

        /** */
        private long maxResRcvQueueTime;

        /** */
        private long maxResRcvQueueTimeTs;

        /** */
        private long maxReqWireTimeMillis;

        /** */
        private long maxReqWireTimeTs;

        /** */
        private long maxResWireTimeMillis;

        /** */
        private long maxResWireTimeTs;

        /** */
        private final long latencyLimit;

        /**
         * @param rangesCnt Ranges count.
         * @param latencyLimit
         */
        public IoTestThreadLocalNodeResults(int rangesCnt, long latencyLimit) {
            this.rangesCnt = rangesCnt;
            this.latencyLimit = latencyLimit;

            resLatency = new long[rangesCnt + 1];
        }

        /**
         * @param msg
         */
        public void onResult(IgniteIoTestMessage msg) {
            long now = System.currentTimeMillis();

            long latency = msg.responseProcessedTs() - msg.requestCreateTs();

            int idx = latency >= latencyLimit ?
                rangesCnt /* Timed out. */ :
                (int)Math.floor((1.0 * latency) / ((1.0 * latencyLimit) / rangesCnt));

            resLatency[idx]++;

            totalLatency += latency;

            if (maxLatency < latency) {
                maxLatency = latency;
                maxLatencyTs = now;
            }

            long reqSndQueueTime = msg.requestSendTs() - msg.requestCreateTs();

            if (maxReqSendQueueTime < reqSndQueueTime) {
                maxReqSendQueueTime = reqSndQueueTime;
                maxReqSendQueueTimeTs = now;
            }

            long reqRcvQueueTime = msg.requestProcessTs() - msg.requestReceiveTs();

            if (maxReqRcvQueueTime < reqRcvQueueTime) {
                maxReqRcvQueueTime = reqRcvQueueTime;
                maxReqRcvQueueTimeTs = now;
            }

            long resSndQueueTime = msg.responseSendTs() - msg.requestProcessTs();

            if (maxResSendQueueTime < resSndQueueTime) {
                maxResSendQueueTime = resSndQueueTime;
                maxResSendQueueTimeTs = now;
            }

            long resRcvQueueTime = msg.responseProcessedTs() - msg.responseReceiveTs();

            if (maxResRcvQueueTime < resRcvQueueTime) {
                maxResRcvQueueTime = resRcvQueueTime;
                maxResRcvQueueTimeTs = now;
            }

            long reqWireTimeMillis = msg.requestReceivedTsMillis() - msg.requestSendTsMillis();

            if (maxReqWireTimeMillis < reqWireTimeMillis) {
                maxReqWireTimeMillis = reqWireTimeMillis;
                maxReqWireTimeTs = now;
            }

            long resWireTimeMillis = msg.responseReceivedTsMillis() - msg.requestSendTsMillis();

            if (maxResWireTimeMillis < resWireTimeMillis) {
                maxResWireTimeMillis = resWireTimeMillis;
                maxResWireTimeTs = now;
            }
        }
    }

    /**
     *
     */
    private static class IoTestNodeResults {
        /** */
        private long latencyLimit;

        /** */
        private long[] resLatency;

        /** */
        private long totalLatency;

        /** */
        private Collection<IgnitePair<Long>> maxLatency = new ArrayList<>();

        /** */
        private Collection<IgnitePair<Long>> maxReqSendQueueTime = new ArrayList<>();

        /** */
        private Collection<IgnitePair<Long>> maxReqRcvQueueTime = new ArrayList<>();

        /** */
        private Collection<IgnitePair<Long>> maxResSendQueueTime = new ArrayList<>();

        /** */
        private Collection<IgnitePair<Long>> maxResRcvQueueTime = new ArrayList<>();

        /** */
        private Collection<IgnitePair<Long>> maxReqWireTimeMillis = new ArrayList<>();

        /** */
        private Collection<IgnitePair<Long>> maxResWireTimeMillis = new ArrayList<>();

        /**
         * @param res Node results to add.
         */
        public void add(IoTestThreadLocalNodeResults res) {
            if (resLatency == null) {
                resLatency = res.resLatency.clone();
                latencyLimit = res.latencyLimit;
            }
            else {
                assert latencyLimit == res.latencyLimit;
                assert resLatency.length == res.resLatency.length;

                for (int i = 0; i < resLatency.length; i++)
                    resLatency[i] += res.resLatency[i];
            }

            totalLatency += res.totalLatency;

            maxLatency.add(F.pair(res.maxLatency, res.maxLatencyTs));
            maxReqSendQueueTime.add(F.pair(res.maxReqSendQueueTime, res.maxReqSendQueueTimeTs));
            maxReqRcvQueueTime.add(F.pair(res.maxReqRcvQueueTime, res.maxReqRcvQueueTimeTs));
            maxResSendQueueTime.add(F.pair(res.maxResSendQueueTime, res.maxResSendQueueTimeTs));
            maxResRcvQueueTime.add(F.pair(res.maxResRcvQueueTime, res.maxResRcvQueueTimeTs));
            maxReqWireTimeMillis.add(F.pair(res.maxReqWireTimeMillis, res.maxReqWireTimeTs));
            maxResWireTimeMillis.add(F.pair(res.maxResWireTimeMillis, res.maxResWireTimeTs));
        }

        /**
         * @return Bin latency in microseconds.
         */
        public long binLatencyMcs() {
            if (resLatency == null)
                throw new IllegalStateException();

            return latencyLimit / (1000 * (resLatency.length - 1));
        }
    }

    /**
     * @return Security subject id.
     */
    private UUID secSubjId(GridIoMessage msg) {
        if (ctx.security().enabled()) {
            assert msg instanceof GridIoSecurityAwareMessage;

            return ((GridIoSecurityAwareMessage) msg).secSubjId();
        }

        return null;
    }

    /**
     * Responsible for handling network situation where server cannot open connection to client and
     * has to ask client to establish a connection to specific server.
     *
     * This includes the following steps:
     * <ol>
     *     <li>
     *         Server tries to send regular communication message to unreachable client,
     *         detects that client is unreachagle and directs special discovery message to it.
     *         After that it wait for client to reply.
     *     </li>
     *     <li>
     *         Client receives discovery message and sends special communication message in response.
     *         This action opens communication channel between client and server that can be used by both sides.
     *     </li>
     *     <li>
     *         Server on receiving comm message sends original communication message to the client.
     *     </li>
     * </ol>
     */
    private final class TcpCommunicationInverseConnectionHandler implements ConnectionRequestor {
        /**
         * Executor service to send special communication message.
         */
        private ExecutorService responseSendService = Executors.newCachedThreadPool();

        /**
         * Discovery event listener (works only on client nodes for now) notified when
         * inverse connection request arrives.
         */
        private CustomEventListener<TcpConnectionRequestDiscoveryMessage> discoConnReqLsnr = (topVer, snd, msg) -> {
            if (!locNodeId.equals(msg.receiverNodeId()))
                return;

            int connIdx = msg.connectionIndex();

            if (log.isInfoEnabled())
                log.info("Received inverse communication request from " + snd + " for connection index " + connIdx);

            TcpCommunicationSpi tcpCommSpi = getTcpCommunicationSpi();

            assert !isPairedConnection(snd, tcpCommSpi);

            responseSendService.submit(() -> {
                try {
                    send(snd,
                        TOPIC_COMM_SYSTEM,
                        TOPIC_COMM_SYSTEM.ordinal(),
                        new TcpInverseConnectionResponseMessage(connIdx),
                        SYSTEM_POOL,
                        false,
                        0,
                        false,
                        null,
                        false
                    );
                }
                catch (IgniteCheckedException e) {
                    log.error("Failed to send response to inverse communication connection request from node: " + snd.id(), e);
                }
            });
        };

        /** */
        public void onStart() {
            if (ctx.clientNode())
                ctx.discovery().setCustomEventListener(TcpConnectionRequestDiscoveryMessage.class, invConnHandler.discoConnReqLsnr);

            addMessageListener(TOPIC_COMM_SYSTEM, (nodeId, msg, plc) -> {
                if (msg instanceof TcpInverseConnectionResponseMessage) {
                    if (log.isInfoEnabled())
                        log.info("Response for inverse connection received from node " + nodeId +
                            ", connection index is " + ((TcpInverseConnectionResponseMessage)msg).connectionIndex());
                }
            });
        }

        /**
         * Executes inverse connection protocol by sending discovery request and then waiting on future
         * completed when response arrives or timeout is reached.
         *
         * @param node Unreachable node.
         * @param connIdx Connection index.
         */
        @Override public void request(ClusterNode node, int connIdx) {
            TcpCommunicationSpi tcpCommSpi = getTcpCommunicationSpi();

            if (isPairedConnection(node, tcpCommSpi))
                throw new IgniteSpiException("Inverse connection protocol doesn't support paired connections");

            try {
                if (log.isInfoEnabled())
                    log.info("TCP connection failed, node " + node.id() + " is unreachable," +
                        " will attempt to request inverse connection via discovery SPI.");

                TcpConnectionRequestDiscoveryMessage msg = new TcpConnectionRequestDiscoveryMessage(
                    node.id(), connIdx
                );

                ctx.discovery().sendCustomEvent(msg);
            }
            catch (IgniteCheckedException ex) {
                throw new IgniteSpiException(ex);
            }
        }

        /** */
        public void onStop() {
            U.shutdownNow(
                TcpCommunicationInverseConnectionHandler.class,
                responseSendService,
                log
            );
        }
    }
}
