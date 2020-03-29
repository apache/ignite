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

package org.apache.ignite.ml.inference.builder;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteQueue;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.CollectionConfiguration;
import org.apache.ignite.ml.inference.Model;
import org.apache.ignite.ml.inference.parser.ModelParser;
import org.apache.ignite.ml.inference.reader.ModelReader;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;

/**
 * Builder that allows to start Apache Ignite services for distributed inference and get a facade that allows to work
 * with this distributed inference infrastructure as with a single inference model (see {@link Model}).
 *
 * The common workflow is based on a request/response queues and multiple workers represented by Apache Ignite services.
 * When the {@link #build(ModelReader, ModelParser)} method is called Apache Ignite starts the specified number of
 * service instances and request/response queues. Each service instance reads request queue, processes inbound requests
 * and writes responses to response queue. The facade returned by the {@link #build(ModelReader, ModelParser)}
 * method operates with request/response queues. When the {@link Model#predict(Object)} method is called the argument
 * is sent as a request to the request queue. When the response is appeared in the response queue the {@link Future}
 * correspondent to the previously sent request is completed and the processing finishes.
 *
 * Be aware that {@link Model#close()} method must be called to clear allocated resources, stop services and remove
 * queues.
 */
public class IgniteDistributedModelBuilder implements AsyncModelBuilder {
    /** Template of the inference service name. */
    private static final String INFERENCE_SERVICE_NAME_PATTERN = "inference_service_%s";

    /** Template of the inference request queue name. */
    private static final String INFERENCE_REQUEST_QUEUE_NAME_PATTERN = "inference_queue_req_%s";

    /** Template of the inference response queue name. */
    private static final String INFERENCE_RESPONSE_QUEUE_NAME_PATTERN = "inference_queue_res_%s";

    /** Default capacity for all queues used in this class (request queue, response queue, received queue). */
    private static final int QUEUE_CAPACITY = 100;

    /** Default configuration for Apache Ignite queues used in this class (request queue, response queue). */
    private static final CollectionConfiguration queueCfg = new CollectionConfiguration();

    /** Ignite instance. */
    private final Ignite ignite;

    /** Number of service instances maintaining to make distributed inference. */
    private final int instances;

    /** Max per node number of instances. */
    private final int maxPerNode;

    /**
     * Constructs a new instance of Ignite distributed inference model builder.
     *
     * @param ignite Ignite instance.
     * @param instances Number of service instances maintaining to make distributed inference.
     * @param maxPerNode Max per node number of instances.
     */
    public IgniteDistributedModelBuilder(Ignite ignite, int instances, int maxPerNode) {
        this.ignite = ignite;
        this.instances = instances;
        this.maxPerNode = maxPerNode;
    }

    /**
     * Starts the specified in constructor number of service instances and request/response queues. Each service
     * instance reads request queue, processes inbound requests and writes responses to response queue. The returned
     * facade is represented by the {@link Model} operates with request/response queues, but hides these details
     * behind {@link Model#predict(Object)} method of {@link Model}.
     *
     * Be aware that {@link Model#close()} method must be called to clear allocated resources, stop services and
     * remove queues.
     *
     * @param reader Inference model reader.
     * @param parser Inference model parser.
     * @param <I> Type of model input.
     * @param <O> Type of model output.
     * @return Facade represented by {@link Model}.
     */
    @Override public <I extends Serializable, O extends Serializable> Model<I, Future<O>> build(
        ModelReader reader, ModelParser<I, O, ?> parser) {
        return new DistributedInfModel<>(ignite, UUID.randomUUID().toString(), reader, parser, instances, maxPerNode);
    }

    /**
     * Facade that operates with request/response queues to make distributed inference, but hides these details
     * behind {@link Model#predict(Object)} method of {@link Model}.
     *
     * Be aware that {@link Model#close()} method must be called to clear allocated resources, stop services and
     * remove queues.
     *
     * @param <I> Type of model input.
     * @param <O> Type of model output.
     */
    private static class DistributedInfModel<I extends Serializable, O extends Serializable>
        implements Model<I, Future<O>> {
        /** Ignite instance. */
        private final Ignite ignite;

        /** Suffix that with correspondent templates formats service and queue names. */
        private final String suffix;

        /** Request queue. */
        private final IgniteQueue<I> reqQueue;

        /** Response queue. */
        private final IgniteQueue<O> resQueue;

        /** Futures that represents requests that have been sent, but haven't been responded yet. */
        private final BlockingQueue<CompletableFuture<O>> futures = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

        /** Thread pool for receiver to work in. */
        private final ExecutorService receiverThreadPool = Executors.newSingleThreadExecutor();

        /** Flag identified that model is up and running. */
        private final AtomicBoolean running = new AtomicBoolean(false);

        /** Receiver future. */
        private volatile Future<?> receiverFut;

        /**
         * Constructs a new instance of distributed inference model.
         *
         * @param ignite Ignite instance.
         * @param suffix Suffix that with correspondent templates formats service and queue names.
         * @param reader Inference model reader.
         * @param parser Inference model parser.
         * @param instances Number of service instances maintaining to make distributed inference.
         * @param maxPerNode Max per node number of instances.
         */
        DistributedInfModel(Ignite ignite, String suffix, ModelReader reader, ModelParser<I, O, ?> parser,
            int instances, int maxPerNode) {
            this.ignite = ignite;
            this.suffix = suffix;

            reqQueue = ignite.queue(String.format(INFERENCE_REQUEST_QUEUE_NAME_PATTERN, suffix), QUEUE_CAPACITY,
                queueCfg);
            resQueue = ignite.queue(String.format(INFERENCE_RESPONSE_QUEUE_NAME_PATTERN, suffix), QUEUE_CAPACITY,
                queueCfg);

            startReceiver();
            startService(reader, parser, instances, maxPerNode);

            running.set(true);
        }

        /** {@inheritDoc} */
        @Override public Future<O> predict(I input) {
            if (!running.get())
                throw new IllegalStateException("Inference model is not running");

            CompletableFuture<O> fut = new CompletableFuture<>();

            try {
                futures.put(fut);
            }
            catch (InterruptedException e) {
                close(); // In case of exception in the above code the model state becomes invalid and model is closed.
                throw new RuntimeException(e);
            }

            reqQueue.put(input);
            return fut;
        }

        /**
         * Starts Apache Ignite services that represent distributed inference infrastructure.
         *
         * @param reader Inference model reader.
         * @param parser Inference model parser.
         * @param instances Number of service instances maintaining to make distributed inference.
         * @param maxPerNode Max per node number of instances.
         */
        private void startService(ModelReader reader, ModelParser<I, O, ?> parser, int instances, int maxPerNode) {
            ignite.services().deployMultiple(
                String.format(INFERENCE_SERVICE_NAME_PATTERN, suffix),
                new IgniteDistributedInfModelService<>(reader, parser, suffix),
                instances,
                maxPerNode
            );
        }

        /**
         * Stops Apache Ignite services that represent distributed inference infrastructure.
         */
        private void stopService() {
            ignite.services().cancel(String.format(INFERENCE_SERVICE_NAME_PATTERN, suffix));
        }

        /**
         * Starts the thread that reads the response queue and completed correspondent futures from {@link #futures}
         * queue.
         */
        private void startReceiver() {
            receiverFut = receiverThreadPool.submit(() -> {
                try {
                    while (!Thread.currentThread().isInterrupted()) {
                        O res;
                        try {
                            res = resQueue.take();
                        }
                        catch (IllegalStateException e) {
                            if (!resQueue.removed())
                                throw e;
                            continue;
                        }

                        CompletableFuture<O> fut = futures.remove();
                        fut.complete(res);
                    }
                }
                finally {
                    close(); // If the model is not stopped yet we need to stop it to protect queue from new writes.
                    while (!futures.isEmpty()) {
                        CompletableFuture<O> fut = futures.remove();
                        fut.cancel(true);
                    }
                }
            });
        }

        /**
         * Stops receiver thread that reads the response queue and completed correspondent futures from
         * {@link #futures} queue.
         */
        private void stopReceiver() {
            if (receiverFut != null && !receiverFut.isDone())
                receiverFut.cancel(true);
            // The receiver thread pool is not reused, so it should be closed here.
            receiverThreadPool.shutdown();
        }

        /**
         * Remove request/response Ignite queues.
         */
        private void removeQueues() {
            reqQueue.close();
            resQueue.close();
        }

        /** {@inheritDoc} */
        @Override public void close() {
            boolean runningBefore = running.getAndSet(false);

            if (runningBefore) {
                stopService();
                stopReceiver();
                removeQueues();
            }
        }
    }

    /**
     * Apache Ignite service that makes inference reading requests from the request queue and writing responses to the
     * response queue. This service is assumed to be deployed in {@link #build(ModelReader, ModelParser)} method
     * and cancelled in {@link Model#close()} method of the inference model.
     *
     * @param <I> Type of model input.
     * @param <O> Type of model output.
     */
    private static class IgniteDistributedInfModelService<I extends Serializable, O extends Serializable>
        implements Service {
        /** */
        private static final long serialVersionUID = -3596084917874395597L;

        /** Inference model reader. */
        private final ModelReader reader;

        /** Inference model parser. */
        private final ModelParser<I, O, ?> parser;

        /** Suffix that with correspondent templates formats service and queue names. */
        private final String suffix;

        /** Request queue, is created in {@link #init(ServiceContext)} method. */
        private transient IgniteQueue<I> reqQueue;

        /** Response queue, is created in {@link #init(ServiceContext)} method. */
        private transient IgniteQueue<O> resQueue;

        /** Inference model, is created in {@link #init(ServiceContext)} method. */
        private transient Model<I, O> mdl;

        /**
         * Constructs a new instance of Ignite distributed inference model service.
         *
         * @param reader Inference model reader.
         * @param parser Inference model parser.
         * @param suffix Suffix that with correspondent templates formats service and queue names.
         */
        IgniteDistributedInfModelService(ModelReader reader, ModelParser<I, O, ?> parser, String suffix) {
            this.reader = reader;
            this.parser = parser;
            this.suffix = suffix;
        }

        /** {@inheritDoc} */
        @Override public void init(ServiceContext ctx) {
            Ignite ignite = Ignition.localIgnite();

            reqQueue = ignite.queue(String.format(INFERENCE_REQUEST_QUEUE_NAME_PATTERN, suffix), QUEUE_CAPACITY,
                queueCfg);
            resQueue = ignite.queue(String.format(INFERENCE_RESPONSE_QUEUE_NAME_PATTERN, suffix), QUEUE_CAPACITY,
                queueCfg);

            mdl = parser.parse(reader.read());
        }

        /** {@inheritDoc} */
        @Override public void execute(ServiceContext ctx) {
            while (!ctx.isCancelled()) {
                I req;
                try {
                    req = reqQueue.take();
                }
                catch (IllegalStateException e) {
                    // If the queue is removed during the take() operation exception should be ignored.
                    if (!reqQueue.removed())
                        throw e;
                    continue;
                }

                O res = mdl.predict(req);

                try {
                    resQueue.put(res);
                }
                catch (IllegalStateException e) {
                    // If the queue is removed during the put() operation exception should be ignored.
                    if (!resQueue.removed())
                        throw e;
                }
            }
        }

        /** {@inheritDoc} */
        @Override public void cancel(ServiceContext ctx) {
            // Do nothing. Queues are assumed to be closed in model close() method.
        }
    }
}
