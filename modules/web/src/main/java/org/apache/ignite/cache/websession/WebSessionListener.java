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

package org.apache.ignite.cache.websession;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collection;
import javax.cache.processor.EntryProcessor;
import javax.cache.processor.MutableEntry;
import org.apache.ignite.internal.util.typedef.T2;
import org.apache.ignite.internal.util.typedef.internal.U;

/**
 * Session listener for web sessions caching.
 */
class WebSessionListener {
    /**
     * Creates new instance of attribute processor. Used for compatibility.
     *
     * @param updates Updates.
     * @return New instance of attribute processor.
     */
    public static EntryProcessor<String, WebSession, Void> newAttributeProcessor(
        final Collection<T2<String, Object>> updates) {
        return new AttributesProcessor(updates);
    }

    /**
     * Multiple attributes update transformer.
     */
    private static class AttributesProcessor implements EntryProcessor<String, WebSession, Void>, Externalizable {
        /** */
        private static final long serialVersionUID = 0L;

        /** Updates list. */
        private Collection<T2<String, Object>> updates;

        /**
         * Required by {@link Externalizable}.
         */
        public AttributesProcessor() {
            // No-op.
        }

        /**
         * @param updates Updates list.
         */
        AttributesProcessor(Collection<T2<String, Object>> updates) {
            assert updates != null;

            this.updates = updates;
        }

        /** {@inheritDoc} */
        @Override public Void process(MutableEntry<String, WebSession> entry, Object... args) {
            if (!entry.exists())
                return null;

            WebSession ses0 = entry.getValue();

            WebSession ses = new WebSession(ses0.getId(), ses0);

            for (T2<String, Object> update : updates) {
                String name = update.get1();

                assert name != null;

                Object val = update.get2();

                if (val != null)
                    ses.setAttribute(name, val);
                else
                    ses.removeAttribute(name);
            }

            entry.setValue(ses);

            return null;
        }

        /** {@inheritDoc} */
        @Override public void writeExternal(ObjectOutput out) throws IOException {
            U.writeCollection(out, updates);
        }

        /** {@inheritDoc} */
        @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            updates = U.readCollection(in);
        }
    }
}
