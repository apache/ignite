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

package org.apache.ignite.internal.processors.tracing;

import java.util.Iterator;
import org.apache.ignite.internal.processors.tracing.MTC.TraceSurroundings;

import static org.apache.ignite.internal.processors.tracing.SpanTags.ERROR;

/**
 * Represents wrapper which allows the iterator methods to execute within context of a specified span.
 */
public class TraceableIterator<T> implements Iterator<T> {
    /** Iterator to which all calls will be delegated. */
    private final Iterator<T> iter;

    /** Span that represents trace context in which iterator runs. */
    private final Span span;

    /**
     * @param iter Iterator to which all calls will be delegated.
     */
    public TraceableIterator(Iterator<T> iter) {
        this.iter = iter;
        this.span = MTC.span();
    }

    /** {@inheritDoc} */
    @Override public boolean hasNext() {
        try (TraceSurroundings ignored = MTC.supportContinual(span)) {
            return iter.hasNext();
        }
        catch (Throwable th) {
            span.addTag(ERROR, th::getMessage);

            throw th;
        }
    }

    /** {@inheritDoc} */
    @Override public T next() {
        try (TraceSurroundings ignored = MTC.supportContinual(span)) {
            return iter.next();
        }
        catch (Throwable th) {
            span.addTag(ERROR, th::getMessage);

            throw th;
        }
    }
}
