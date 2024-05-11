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

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.util.MemoryHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class TinkerMemory implements Memory.Admin {

    public final Map<String, MemoryComputeKey> memoryKeys = new HashMap<>();
    public Map<String, Optional<Object>> previousMap;
    public Map<String, Optional<Object>> currentMap;
    private final AtomicInteger iteration = new AtomicInteger(0);
    private final AtomicLong runtime = new AtomicLong(0l);
    private boolean inExecute = false;

    public TinkerMemory(final VertexProgram<?> vertexProgram, final Set<MapReduce> mapReducers) {
        // ConcurrentHashMap makes us use Optional since you cant store null in them as values (or keys)
        this.currentMap = new ConcurrentHashMap<>();
        this.previousMap = new ConcurrentHashMap<>();
        if (null != vertexProgram) {
            for (final MemoryComputeKey memoryComputeKey : vertexProgram.getMemoryComputeKeys()) {
                this.memoryKeys.put(memoryComputeKey.getKey(), memoryComputeKey);
            }
        }
        for (final MapReduce mapReduce : mapReducers) {
            this.memoryKeys.put(mapReduce.getMemoryKey(), MemoryComputeKey.of(mapReduce.getMemoryKey(), Operator.assign, false, false));
        }
    }

    @Override
    public Set<String> keys() {
        return this.previousMap.keySet().stream().filter(key -> !this.inExecute || this.memoryKeys.get(key).isBroadcast()).collect(Collectors.toSet());
    }

    @Override
    public void incrIteration() {
        this.iteration.getAndIncrement();
    }

    @Override
    public void setIteration(final int iteration) {
        this.iteration.set(iteration);
    }

    @Override
    public int getIteration() {
        return this.iteration.get();
    }

    @Override
    public void setRuntime(final long runTime) {
        this.runtime.set(runTime);
    }

    @Override
    public long getRuntime() {
        return this.runtime.get();
    }

    protected void complete() {
        this.iteration.decrementAndGet();
        this.previousMap = this.currentMap;
        this.memoryKeys.values().stream().filter(MemoryComputeKey::isTransient).forEach(computeKey -> this.previousMap.remove(computeKey.getKey()));
    }

    protected void completeSubRound() {
        this.previousMap = new ConcurrentHashMap<>(this.currentMap);
        this.inExecute = !this.inExecute;
    }

    @Override
    public boolean isInitialIteration() {
        return this.getIteration() == 0;
    }

    @Override
    public <R> R get(final String key) throws IllegalArgumentException {
        if (!this.previousMap.containsKey(key)) throw Memory.Exceptions.memoryDoesNotExist(key);

        final Optional<Object> o = this.previousMap.get(key);
        final R r = (R) o.orElse(null);
        if (this.inExecute && !this.memoryKeys.get(key).isBroadcast())
            throw Memory.Exceptions.memoryDoesNotExist(key);
        else
            return r;
    }

    @Override
    public void set(final String key, final Object value) {
        checkKeyValue(key, value);
        if (this.inExecute)
            throw Memory.Exceptions.memorySetOnlyDuringVertexProgramSetUpAndTerminate(key);
        this.currentMap.put(key, Optional.ofNullable(value));
    }

    @Override
    public void add(final String key, final Object value) {
        checkKeyValue(key, value);
        if (!this.inExecute)
            throw Memory.Exceptions.memoryAddOnlyDuringVertexProgramExecute(key);
        this.currentMap.compute(key, (k, v) -> Optional.ofNullable(null == v || !v.isPresent() ? value : this.memoryKeys.get(key).getReducer().apply(v.get(), value)));
    }

    @Override
    public String toString() {
        return StringFactory.memoryString(this);
    }

    protected void checkKeyValue(final String key, final Object value) {
        if (!this.memoryKeys.containsKey(key))
            throw GraphComputer.Exceptions.providedKeyIsNotAMemoryComputeKey(key);
    }
}
