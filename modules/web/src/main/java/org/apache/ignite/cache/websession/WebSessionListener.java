/*
 *                   GridGain Community Edition Licensing
 *                   Copyright 2019 GridGain Systems, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License") modified with Commons Clause
 * Restriction; you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 *
 * Commons Clause Restriction
 *
 * The Software is provided to you by the Licensor under the License, as defined below, subject to
 * the following condition.
 *
 * Without limiting other conditions in the License, the grant of rights under the License will not
 * include, and the License does not grant to you, the right to Sell the Software.
 * For purposes of the foregoing, “Sell” means practicing any or all of the rights granted to you
 * under the License to provide to third parties, for a fee or other consideration (including without
 * limitation fees for hosting or consulting/ support services related to the Software), a product or
 * service whose value derives, entirely or substantially, from the functionality of the Software.
 * Any license notice or attribution required by the License must also include this Commons Clause
 * License Condition notice.
 *
 * For purposes of the clause above, the “Licensor” is Copyright 2019 GridGain Systems, Inc.,
 * the “License” is the Apache License, Version 2.0, and the Software is the GridGain Community
 * Edition software provided with this notice.
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
