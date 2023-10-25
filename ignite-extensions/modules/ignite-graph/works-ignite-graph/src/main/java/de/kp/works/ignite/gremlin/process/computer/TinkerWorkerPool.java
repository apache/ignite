/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package de.kp.works.ignite.gremlin.process.computer;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.ignite.internal.IgniteEx;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MapReducePool;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramPool;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import org.apache.tinkerpop.gremlin.util.function.TriConsumer;

import de.kp.works.ignite.gremlin.IgniteGraph;
import de.kp.works.ignite.gremlin.TinkerHelper;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Stephen Mallette (http://stephen.genoprime.com)
 */
public final class TinkerWorkerPool implements AutoCloseable {
   
    private final int numberOfWorkers;
    private final ExecutorService workerPool;
    private final CompletionService<Object> completionService;

    private VertexProgramPool vertexProgramPool;
    private MapReducePool mapReducePool;
    private final Queue<TinkerWorkerMemory> workerMemoryPool = new ConcurrentLinkedQueue<>();
    private final List<List<Vertex>> workerVertices = new ArrayList<>();

    public TinkerWorkerPool(final IgniteGraph graph, final TinkerMemory memory) {
        // use Ignite Service pool
        this.workerPool = ((IgniteEx)TinkerHelper.ignite(graph)).context().pools().getDataStreamerExecutorService();
        this.numberOfWorkers = TinkerHelper.ignite(graph).configuration().getServiceThreadPoolSize();
        this.completionService = new ExecutorCompletionService<>(this.workerPool);
        for (int i = 0; i < this.numberOfWorkers; i++) {
            this.workerMemoryPool.add(new TinkerWorkerMemory(memory));
            this.workerVertices.add(new ArrayList<>());
        }
        //int batchSize = graph.allVertices().size() / this.numberOfWorkers;
        int batchSize = 64;
        if (0 == batchSize)
            batchSize = 1;
        int counter = 0;
        int index = 0;

        List<Vertex> currentWorkerVertices = this.workerVertices.get(index);
        final Iterator<Vertex> iterator = graph.vertices();
        while (iterator.hasNext()) {
            final Vertex vertex = iterator.next();
            if (counter++ < batchSize || index == this.workerVertices.size() - 1) {
                currentWorkerVertices.add(vertex);
            } else {
                currentWorkerVertices = this.workerVertices.get(++index);
                currentWorkerVertices.add(vertex);
                counter = 1;
            }
        }
    }

    public void setVertexProgram(final VertexProgram vertexProgram) {
        this.vertexProgramPool = new VertexProgramPool(vertexProgram, this.numberOfWorkers);
    }

    public void setMapReduce(final MapReduce mapReduce) {
        this.mapReducePool = new MapReducePool(mapReduce, this.numberOfWorkers);
    }

    public void executeVertexProgram(final TriConsumer<Iterator<Vertex>, VertexProgram, TinkerWorkerMemory> worker) throws InterruptedException {
        for (int i = 0; i < this.numberOfWorkers; i++) {
            final int index = i;
            this.completionService.submit(() -> {
                final VertexProgram vp = this.vertexProgramPool.take();
                final TinkerWorkerMemory workerMemory = this.workerMemoryPool.poll();
                final List<Vertex> vertices = this.workerVertices.get(index);
                worker.accept(vertices.iterator(), vp, workerMemory);
                this.vertexProgramPool.offer(vp);
                this.workerMemoryPool.offer(workerMemory);
                return null;
            });
        }
        for (int i = 0; i < this.numberOfWorkers; i++) {
            try {
                this.completionService.take().get();
            } catch (InterruptedException ie) {
                throw ie;
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    public void executeMapReduce(final Consumer<MapReduce> worker) throws InterruptedException {
        for (int i = 0; i < this.numberOfWorkers; i++) {
            this.completionService.submit(() -> {
                final MapReduce mr = this.mapReducePool.take();
                worker.accept(mr);
                this.mapReducePool.offer(mr);
                return null;
            });
        }
        for (int i = 0; i < this.numberOfWorkers; i++) {
            try {
                this.completionService.take().get();
            } catch (InterruptedException ie) {
                throw ie;
            } catch (final Exception e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        }
    }

    public void closeNow() throws Exception {
        this.workerPool.shutdownNow();
    }

    @Override
    public void close() throws Exception {
        this.workerPool.shutdown();
    }
}