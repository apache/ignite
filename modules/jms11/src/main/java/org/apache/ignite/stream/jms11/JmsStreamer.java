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

package org.apache.ignite.stream.jms11;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.ignite.IgniteDataStreamer;
import org.apache.ignite.IgniteException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.internal.util.typedef.internal.A;
import org.apache.ignite.stream.StreamAdapter;

/**
 * Streamer that consumes from a JMS destination and feeds key-value pairs into an {@link IgniteDataStreamer} instance.
 * <p>
 * This Streamer uses purely JMS semantics and it is not coupled with any JMS implementation. It uses {@link
 * MessageListener} to receive messages. You must provide your broker's {@link javax.jms.ConnectionFactory} when
 * creating a {@link JmsStreamer}.
 * <p>
 * You must also provide a {@link MessageTransformer} to convert the incoming message into cache entries.
 * <p>
 * This Streamer has many features:
 *
 * <ul>
 *     <li>Consumes from queues or topics.</li>
 *     <li>For topics, it supports durable subscriptions.</li>
 *     <li>Concurrent consumers are supported via the <tt>threads</tt> parameter. When consuming from queues,
 *     this component will start as many {@link Session} objects with separate {@link MessageListener} instances each,
 *     therefore achieving <i>native</i> concurrency (in terms of the JMS standard).<br>
 *     When consuming from topics, obviously we cannot start multiple threads as that would lead us to consume
 *     duplicate messages. Therefore, we achieve concurrency in a <i>virtualized</i> manner through an internal
 *     thread pool.</li>
 *     <li>Transacted sessions are supported through the <tt>transacted</tt> parameter.</li>
 *     <li>Batched consumption is possible via the <tt>batched</tt> parameter. Depending on the broker, this
 *     technique can provide a higher throughput as it decreases the amount of message acknowledgement round trips
 *     that are necessary, albeit at the expense possible duplicate messages (especially if an incident
 *     occurs in the middle of a transaction).<br>
 *     Batches are committed when the <tt>batchClosureMillis</tt> time has elapsed, or when a Session has received
 *     at least <tt>batchClosureSize</tt> messages. Time-based closure fires with the specified frequency and applies to
 *     all {@link Session}s in parallel. Size-based closure applies individually to each <tt>Session</tt> (as transactions
 *     are <tt>Session-bound</tt> in JMS, so it will fire when that {@link Session} has processed that many messages.
 *     Both options are compatible with each other, or you can disable either (see setter documentation),
 *     but not both.</li>
 *     <li>Can specify the destination with implementation-specific {@link Destination} objects or with names.</li>
 * </ul>

 *
 * @author Raul Kripalani
 */
public class JmsStreamer<T extends Message, K, V> extends StreamAdapter<T, K, V> {

    /** Logger. */
    private IgniteLogger log;

    /**
     * <i>Compulsory.</i> The message transformer that converts an incoming JMS {@link Message} (or subclass) into one
     * or multiple cache entries.
     */
    private MessageTransformer<T, K, V> transformer;

    /** The JMS {@link ConnectionFactory} to use. */
    private ConnectionFactory connectionFactory;

    /** Whether to register or not as a durable subscription (for topic consumption). */
    private boolean durableSubscription;

    /** Name of the durable subscription, as required by the JMS specification. */
    private String durableSubscriptionName;

    /** Client ID in case we're using durable subscribers. */
    private String clientId;

    /** The JMS {@link Destination}; takes precedence over destinationName if both are set. */
    private Destination destination;

    /** Name of the destination. */
    private String destinationName;

    /** Whether to consume in a transacted manner. */
    private boolean transacted;

    /** Whether to consume messages in batches. May lead to duplicate consumption. Value <tt>true</tt> implies
     * <tt>transacted = true</tt>. */
    private boolean batched;

    /** When using batched consumers, the amount of messages after the batch (transaction) will be committed. */
    private int batchClosureSize = 50;

    /**
     * When using batched consumers, the amount of time to wait before the batch (transaction) will be committed. A
     * value of 0 or -1 disables timed-based session commits.
     */
    private long batchClosureMillis = 1000;

    /** Destination type. */
    private Class<? extends Destination> destinationType = Queue.class;

    /**
     * Number of threads to concurrently consume JMS messages. When working with queues, we will start as many {@link
     * javax.jms.Session} objects as indicated by this field, i.e. you will get native concurrency. On the other hand,
     * when consuming from a topic, for obvious reason we will only start 1 message consumer but we will distribute the
     * processing of received messages to as many concurrent threads as indicated.
     */
    private int threads = 1;

    /** Whether we are stopped or not. */
    private volatile boolean stopped = true;

    /** JMS Connection. */
    private Connection connection;

    /** Stores the current JMS Sessions. */
    private Set<Session> sessions = Collections.newSetFromMap(new ConcurrentHashMap<Session, Boolean>());

    /** Message consumers. */
    private Set<MessageConsumer> consumers = Collections.newSetFromMap(new ConcurrentHashMap<MessageConsumer, Boolean>());

    /** Message listeners. */
    private Set<IgniteJmsMessageListener> listeners = Collections.newSetFromMap(new ConcurrentHashMap<IgniteJmsMessageListener, Boolean>());

    /** Scheduler for handling {@link #batchClosureMillis}. */
    private ScheduledExecutorService scheduler;

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    public void start() throws IgniteException {
        if (!stopped)
            throw new IgniteException("Attempted to start an already started JMS Streamer");

        try {
            A.notNull(getStreamer(), "streamer");
            A.notNull(getIgnite(), "ignite");

            log = getIgnite().log();

            A.notNull(transformer, "message transformer");
            A.notNull(connectionFactory, "connection factory");
            A.ensure(threads > 0, "threads > 0");

            // handle batched && transacted parameter interaction
            if (batched && !transacted) {
                log.warning("Starting a Batched JMS Streamer without transacted flag = true. Setting it automatically.");
                transacted = true;
            }

            // handle batch completion criteria
            if (batched) {
                A.ensure(batchClosureMillis > 0 || batchClosureSize > 0, "at least one of batch closure size or " +
                    "batch closure frequency must be specified when using batch consumption");
            }

            // check the parameters needed for durable subscriptions, if enabled
            if (durableSubscription) {
                A.notNullOrEmpty(clientId, "client id is compulsory when using durable subscriptions");
                A.notNullOrEmpty(durableSubscriptionName, "durable subscription name is compulsory when using " +
                    "durable subscriptions");
            }

            // validate the destination; if we have an explicit destination, make sure it's of type Queue or Topic;
            // else make sure that the destinationName and the destinationType are valid
            if (destination == null) {
                A.notNull(destinationType, "destination type");
                A.ensure(destinationType.isAssignableFrom(Queue.class) || destinationType.isAssignableFrom(Topic.class),
                    "this streamer can only handle Queues or Topics.");
                A.notNullOrEmpty(destinationName, "destination or destination name");
            }
            else if (destination instanceof Queue) {
                destinationType = Queue.class;
            }
            else if (destination instanceof Topic) {
                destinationType = Topic.class;
            }
            else {
                throw new IllegalArgumentException("Invalid destination object. Can only handle Queues or Topics.");
            }

            // create a new connection and the client iD if relevant.
            connection = connectionFactory.createConnection();
            if (clientId != null && clientId.trim().length() > 0) {
                connection.setClientID(clientId.trim());
            }

            // build the JMS objects
            if (destinationType == Queue.class) {
                initializeJmsObjectsForQueue();
            }
            else {
                initializeJmsObjectsForTopic();
            }

            stopped = false;

            // start the JMS connection
            connection.start();

            // set up the scheduler service for committing batches
            if (batched && batchClosureMillis > 0) {
                scheduler = Executors.newScheduledThreadPool(1);
                scheduler.schedule(new Runnable() {
                    @Override
                    public void run() {
                        for (Session session : sessions) {
                            try {
                                session.commit();
                                if (log.isDebugEnabled()) {
                                    log.debug("Committing session from time-based batch completion [session=" +
                                        session + "]");
                                }
                            }
                            catch (JMSException ignored) {
                                log.warning("Error while committing session: from batch time-based completion " +
                                    "[session=" + session + "]");
                            }
                        }
                        for (IgniteJmsMessageListener ml : listeners) {
                            ml.resetBatchCounter();
                        }
                    }
                }, batchClosureMillis, TimeUnit.MILLISECONDS);
            }

        }
        catch (Throwable t) {
            throw new IgniteException("Exception while initializing JmsStreamer", t);
        }

    }

    /**
     * Stops streamer.
     */
    public void stop() throws IgniteException {
        if (stopped)
            throw new IgniteException("Attempted to stop an already stopped JMS Streamer");


        try {
            stopped = true;

            if (scheduler != null && !scheduler.isShutdown()) {
                scheduler.shutdown();
                scheduler = null;
            }

            connection.stop();
            connection.close();

            for (Session s : sessions) {
                s.close();
            }

            sessions.clear();
            consumers.clear();
            listeners.clear();
        }
        catch (Throwable t) {
            throw new IgniteException("Exception while stopping JmsStreamer", t);
        }
    }

    /**
     * Sets the JMS {@link ConnectionFactory}.
     *
     * @param connectionFactory JMS {@link ConnectionFactory} for this streamer to use.
     */
    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    /**
     * <i>Compulsory.</i> The {@link MessageTransformer} that converts an incoming JMS {@link Message} (or subclass)
     * into one or multiple cache entries.
     *
     * @param transformer The implementation of the MessageTransformer to use.
     */
    public void setTransformer(MessageTransformer<T, K, V> transformer) {
        this.transformer = transformer;
    }

    /**
     * Sets the JMS {@link Destination} explicitly. Takes precedence over destinationName if both are set.
     *
     * @param destination JMS {@link Destination} if setting it explicitly.
     */
    public void setDestination(Destination destination) {
        this.destination = destination;
    }

    /**
     * Sets the name of the JMS destination to consume from.
     *
     * @param destinationName The name of the destination; will be passed on directly to the broker.
     */
    public void setDestinationName(String destinationName) {
        this.destinationName = destinationName;
    }

    /**
     * Sets the type of the destination to create, when used in combination with {@link #setDestinationName(String)}. It
     * can be an interface or the implementation class specific to the broker.
     *
     * @param destinationType The class representing the destination type. Suggested values: {@link Queue} or {@link
     * Topic}. <i>Compulsory</i> if using {@link #destinationName}.
     * @see Queue
     * @see Topic
     */
    public void setDestinationType(Class<? extends Destination> destinationType) {
        this.destinationType = destinationType;
    }

    /**
     * Sets the number of threads to concurrently consume JMS messages. <p> When working with queues, we will start as
     * many {@link javax.jms.Session} objects as indicated by this field, i.e. you will get native concurrency. <p> On
     * the other hand, when consuming from a topic, for obvious reason we will only start 1 message consumer but we will
     * distribute the processing of received messages to as many concurrent threads as indicated.
     *
     * @param threads Number of threads to use. Default: <tt>1</tt>.
     */
    public void setThreads(int threads) {
        this.threads = threads;
    }

    /**
     * Sets the client ID of the JMS {@link Connection}.
     *
     * @param clientId Client ID in case we're using durable subscribers. Default: none.
     */
    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    /**
     * A <tt>true</tt> value is only accepted in combination with topic consumption.
     *
     * @param durableSubscription Whether or not to use durable subscriptions. Default: <tt>false</tt>.
     */
    public void setDurableSubscription(boolean durableSubscription) {
        this.durableSubscription = durableSubscription;
    }

    /**
     * Instructs the streamer whether to use local JMS transactions or not.
     *
     * @param transacted Whether to consume or not in a transacted manner. Default: <tt>false</tt>.
     */
    public void setTransacted(boolean transacted) {
        this.transacted = transacted;
    }

    /**
     * Batch consumption leverages JMS Transactions to minimise round trips to the broker. <p> Rather than ACKing every
     * single message received, they will be received in the context of a JMS transaction which will be committed once
     * the indicated batch closure size or batch closure time has elapsed. <p> Warning: May lead to duplicate
     * consumption.
     *
     * @param batched Whether to consume messages in batches. Value <tt>true</tt> implies <tt>transacted = true</tt>.
     * Default: <tt>false</tt>.
     * @see #setBatchClosureMillis(long)
     * @see #setBatchClosureSize(int)
     */
    public void setBatched(boolean batched) {
        this.batched = batched;
    }

    /**
     * When using batched consumption, sets the amount of messages that will be received before a batch is committed.
     *
     * @param batchClosureSize The amount of messages processed before a batch is committed. Default: <tt>50</tt>.
     */
    public void setBatchClosureSize(int batchClosureSize) {
        this.batchClosureSize = batchClosureSize;
    }

    /**
     * When using batched consumption, sets the time in milliseconds that will elapse before a batch is committed.
     *
     * @param batchClosureMillis Milliseconds before a batch is committed. Default: <tt>1000ms</tt>.
     */
    public void setBatchClosureMillis(long batchClosureMillis) {
        this.batchClosureMillis = batchClosureMillis;
    }

    /**
     * When using Durable Subscribers, sets the name of the durable subscriber. It is compulsory.
     *
     * @param durableSubscriptionName Name of the durable subscriber. Default: none.
     */
    public void setDurableSubscriptionName(String durableSubscriptionName) {
        this.durableSubscriptionName = durableSubscriptionName;
    }

    private void initializeJmsObjectsForTopic() throws JMSException {
        Session session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);
        Topic topic = (Topic)destination;

        if (destination == null)
            topic = session.createTopic(destinationName);

        MessageConsumer consumer = durableSubscription ? session.createDurableSubscriber(topic, durableSubscriptionName) :
            session.createConsumer(topic);

        IgniteJmsMessageListener messageListener = new IgniteJmsMessageListener(session, true);
        consumer.setMessageListener(messageListener);

        consumers.add(consumer);
        sessions.add(session);
        listeners.add(messageListener);
    }

    private void initializeJmsObjectsForQueue() throws JMSException {
        for (int i = 0; i < threads; i++) {
            Session session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);

            if (destination == null)
                destination = session.createQueue(destinationName);

            MessageConsumer consumer = session.createConsumer(destination);

            IgniteJmsMessageListener messageListener = new IgniteJmsMessageListener(session, false);
            consumer.setMessageListener(messageListener);

            consumers.add(consumer);
            sessions.add(session);
            listeners.add(messageListener);
        }
    }

    private void processMessage(T message) {
        final IgniteDataStreamer<K, V> streamer = getStreamer();

        Map<K, V> entries = transformer.apply(message);

        if (entries == null || entries.size() == 0)
            return;

        streamer.addData(entries);
    }

    /**
     * Message listener for queues.
     */
    private class IgniteJmsMessageListener implements MessageListener {

        private Session session;
        private AtomicInteger counter = new AtomicInteger(0);
        private Executor executor;

        public IgniteJmsMessageListener(Session session, boolean createThreadPool) {
            this.session = session;

            // if we don't need a thread pool, create a dummy one that executes the task synchronously
            //noinspection NullableProblems
            this.executor = createThreadPool ? Executors.newFixedThreadPool(threads) : new Executor() {
                @Override
                public void execute(Runnable command) {
                    command.run();
                }
            };
        }

        @Override
        public void onMessage(final Message message) {
            if (stopped) {
                return;
            }

            executor.execute(new Runnable() {
                @Override @SuppressWarnings("unchecked")
                public void run() {
                    processMessage((T)message);
                    if (batched) {
                        // batch completion may be handled by timer only
                        if (batchClosureSize <= 0)
                            return;

                        else if (counter.incrementAndGet() >= batchClosureSize) {
                            try {
                                session.commit();
                                counter.set(0);
                            }
                            catch (Exception e) {
                                log.warning("Could not commit JMS session upon completion of batch.", e);
                            }
                        }
                    }
                    else if (transacted) {
                        try {
                            session.commit();
                        }
                        catch (JMSException e) {
                            log.warning("Could not commit JMS session (non-batched).", e);
                        }
                    }
                }
            });

        }

        public void resetBatchCounter() {
            counter.set(0);
        }
    }

}
