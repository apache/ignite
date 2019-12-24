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

package org.apache.ignite.internal.processors.cache.query.continuous;

import java.util.UUID;
import javax.cache.configuration.Factory;
import org.apache.ignite.Ignite;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgnitionEx;
import org.apache.ignite.internal.processors.security.OperationSecurityContext;
import org.apache.ignite.lang.IgniteClosure;
import org.apache.ignite.resources.IgniteInstanceResource;

import static java.util.Objects.requireNonNull;

/**
 *  Security aware transformer factory.
 */
@SuppressWarnings("rawtypes")
public class SecurityAwareTransformerFactory extends AbstractSecurityAwareComponent implements Factory<IgniteClosure> {
    /** */
    private static final long serialVersionUID = 0L;

    /**
     * Default constructor.
     */
    public SecurityAwareTransformerFactory() {
        // No-op.
    }

    /**
     * @param subjectId Security subject id.
     * @param original Original factory.
     */
    public SecurityAwareTransformerFactory(UUID subjectId, Factory<IgniteClosure> original) {
        super(subjectId, original);
    }

    /** {@inheritDoc} */
    @Override public IgniteClosure create() {
        Factory<IgniteClosure> factory = (Factory<IgniteClosure>)original;

        return new SecurityAwareIgniteClosure(subjectId, factory.create());
    }


    /** */
    private static class SecurityAwareIgniteClosure implements IgniteClosure {
        /** Security subject id. */
        private final UUID subjectId;

        /** Original transformer. */
        private final IgniteClosure original;

        /** Ignite. */
        private IgniteEx ignite;

        /**
         * @param subjectId Security subject id.
         * @param original Original transformer.
         */
        public SecurityAwareIgniteClosure(UUID subjectId, IgniteClosure original) {
            this.subjectId = requireNonNull(subjectId, "Parameter 'subjectId' cannot be null.");
            this.original = requireNonNull(original, "Parameter 'original' cannot be null.");
        }

        /** {@inheritDoc} */
        @Override public Object apply(Object o) {
            try (OperationSecurityContext c = ignite.context().security().withContext(subjectId)) {
                return original.apply(o);
            }
        }

        /**
         * @param ignite Ignite.
         */
        @IgniteInstanceResource
        public void ignite(Ignite ignite) {
            if (ignite != null)
                this.ignite = ignite instanceof IgniteEx ? (IgniteEx)ignite : IgnitionEx.gridx(ignite.name());
        }
    }
}
