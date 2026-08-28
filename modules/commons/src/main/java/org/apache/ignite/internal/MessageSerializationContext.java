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

package org.apache.ignite.internal;

import org.apache.ignite.internal.processors.rollingupgrade.feature.IgniteFeature;
import org.apache.ignite.lang.IgniteFuture;

/**
 * Represents context that determines how data transmitted between nodes is serialized and deserialized.
 */
public interface MessageSerializationContext {
    /**
     * @param feature Feature that deprecated the field.
     * @return {@code true} if the message field should be included during serialization or deserialization.
     */
    boolean includeFieldDeprecatedBy(IgniteFeature feature);

    /**
     * @param feature Feature that introduced the field.
     * @return {@code true} if the message field should be included during serialization or deserialization.
     */
    boolean includeFieldIntroducedBy(IgniteFeature feature);

    /**
     * {@link MessageSerializationContext} implementation that ignores Rolling Upgrade compatibility during
     * message serialization.
     *
     * <p>Using this context instructs the serialization framework to always serialize the actual message state: all
     * newly introduced fields are included, and all deprecated fields are excluded.</p>
     */
    MessageSerializationContext IGNORED = new MessageSerializationContext() {
        /** {@inheritDoc} */
        @Override public boolean includeFieldDeprecatedBy(IgniteFeature feature) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean includeFieldIntroducedBy(IgniteFeature feature) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "MessageSerializationContext [IGNORED]";
        }
    };

    /**
     * Stub {@link MessageSerializationContext} implementation used when the serialization context has not yet been determined.
     *
     * <p>The serialization context is unavailable between connection establishment and serialization protocol negotiation.
     * Messages sent during this period cannot rely on the {@link IgniteFuture} mechanism to adjust the message serialization
     * in an RU-compatible way.</p>
     */
    MessageSerializationContext UNNEGOTIATED = new MessageSerializationContext() {
        /** {@inheritDoc} */
        @Override public boolean includeFieldDeprecatedBy(IgniteFeature feature) {
            throw buildError(feature);
        }

        /** {@inheritDoc} */
        @Override public boolean includeFieldIntroducedBy(IgniteFeature feature) {
            throw buildError(feature);
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "MessageSerializationContext [UNNEGOTIATED]";
        }

        /** */
        private IllegalStateException buildError(IgniteFeature feature) {
            return new IllegalStateException(
                "A feature-guarded field was serialized before the peer's features were negotiated [feature=" + feature + ']'
            );
        }
    };
}
