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

/** Represents the context that determines how message fields are serialized and deserialized when transmitted between nodes. */
public interface MessageSerializationContext {
    /**
     * @param feature Feature that deprecated the field.
     * @return {@code true} if the message field should be included during message serialization or deserialization.
     */
    boolean includeFieldDeprecatedBy(IgniteFeature feature);

    /**
     * @param feature Feature that introduced the field.
     * @return {@code true} if the message field should be included during message serialization or deserialization.
     */
    boolean includeFieldIntroducedBy(IgniteFeature feature);

    /**
     * @return {@code true} if messages serialized under this context carry the raw-field suffix: a count of raw fields and
     *      the raw fields themselves, written after the positional fields of every message.
     */
    boolean includeRawFields();

    /**
     * {@link MessageSerializationContext} implementation that instructs the serialization framework to always
     * serialize the latest message fields schema: all newly introduced fields are included, and all deprecated fields are
     * excluded.
     */
    MessageSerializationContext LATEST_SCHEMA = new MessageSerializationContext() {
        /** {@inheritDoc} */
        @Override public boolean includeFieldDeprecatedBy(IgniteFeature feature) {
            return false;
        }

        /** {@inheritDoc} */
        @Override public boolean includeFieldIntroducedBy(IgniteFeature feature) {
            return true;
        }

        /** {@inheritDoc} */
        @Override public boolean includeRawFields() {
            return true;
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "MessageSerializationContext [LATEST_SCHEMA]";
        }
    };

    /**
     * {@link MessageSerializationContext} implementation that permits only messages whose schema is immutable across
     * versions, i.e. declares no fields gated by an {@link IgniteFeature}: evaluating any feature gate throws
     * {@link IllegalStateException}.
     *
     * <p>Used between connection establishment and serialization protocol negotiation, when the peer's features are not
     * yet known. Messages exchanged during this period must therefore declare no fields gated by
     * {@code @Order(introducedBy = ...)} or {@code @Order(deprecatedBy = ...)}.</p>
     */
    MessageSerializationContext IMMUTABLE_SCHEMA = new MessageSerializationContext() {
        /** {@inheritDoc} */
        @Override public boolean includeFieldDeprecatedBy(IgniteFeature feature) {
            throw buildError(feature);
        }

        /** {@inheritDoc} */
        @Override public boolean includeFieldIntroducedBy(IgniteFeature feature) {
            throw buildError(feature);
        }

        /** {@inheritDoc} */
        @Override public boolean includeRawFields() {
            throw new IllegalStateException(
                "A message without an immutable schema was serialized before the peer's features were negotiated");
        }

        /** {@inheritDoc} */
        @Override public String toString() {
            return "MessageSerializationContext [IMMUTABLE_SCHEMA]";
        }

        /** */
        private IllegalStateException buildError(IgniteFeature feature) {
            return new IllegalStateException(
                "A feature-gated field was serialized before the peer's features were negotiated [feature=" + feature + ']'
            );
        }
    };
}
