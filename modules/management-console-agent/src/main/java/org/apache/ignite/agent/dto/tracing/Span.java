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

package org.apache.ignite.agent.dto.tracing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.ignite.internal.util.typedef.internal.S;

/**
 * DTO for span.
 */
public class Span {
    /** Trace id. */
    private String traceId;

    /** Parent id. */
    private String parentId;

    /** Span id. */
    private String spanId;

    /** Name. */
    private String name;

    /** Timestamp. */
    private long ts;

    /** Duration. */
    private long duration;

    /** Tags. */
    private Map<String, String> tags = new HashMap<>();

    /** Annotations. */
    private List<Annotation> annotations = new ArrayList<>();

    /**
     * @return Trace id.
     */
    public String getTraceId() {
        return traceId;
    }

    /**
     * @param traceId Trace id.
     * @return {@code This} for chaining method calls.
     */
    public Span setTraceId(String traceId) {
        this.traceId = traceId;

        return this;
    }

    /**
     * @return Parent span id.
     */
    public String getParentId() {
        return parentId;
    }

    /**
     * @param parentId Parent id.
     * @return {@code This} for chaining method calls.
     */
    public Span setParentId(String parentId) {
        this.parentId = parentId;

        return this;
    }

    /**
     * @return Span id.
     */
    public String getSpanId() {
        return spanId;
    }

    /**
     * @param spanId Span id.
     * @return {@code This} for chaining method calls.
     */
    public Span setSpanId(String spanId) {
        this.spanId = spanId;

        return this;
    }

    /**
     * @return Name.
     */
    public String getName() {
        return name;
    }

    /**
     * @param name Name.
     * @return {@code This} for chaining method calls.
     */
    public Span setName(String name) {
        this.name = name;

        return this;
    }

    /**
     * @return Timestamp.
     */
    public long getTimestamp() {
        return ts;
    }

    /**
     * @param ts Timestamp.
     * @return {@code This} for chaining method calls.
     */
    public Span setTimestamp(long ts) {
        this.ts = ts;

        return this;
    }

    /**
     * @return Duration.
     */
    public long getDuration() {
        return duration;
    }

    /**
     * @param duration Duration.
     * @return {@code This} for chaining method calls.
     */
    public Span setDuration(long duration) {
        this.duration = duration;

        return this;
    }

    /**
     * @return Tags.
     */
    public Map<String, String> getTags() {
        return tags;
    }

    /**
     * @param tags Tags.
     * @return {@code This} for chaining method calls.
     */
    public Span setTags(Map<String, String> tags) {
        this.tags = tags;

        return this;
    }

    /**
     * @return Annotation list.
     */
    public List<Annotation> getAnnotations() {
        return annotations;
    }

    /**
     * @param annotations Annotations.
     * @return {@code This} for chaining method calls.
     */
    public Span setAnnotations(List<Annotation> annotations) {
        this.annotations = annotations;

        return this;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(Span.class, this);
    }
}
