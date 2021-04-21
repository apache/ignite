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
package org.apache.ignite.testframework;

import java.util.Iterator;
import org.apache.ignite.internal.util.GridConcurrentLinkedHashSet;

/**
 * This log listener allows to check the order or presence of messages in log. Messages are matched via regexps.
 * Also allows to unify various messages into groups to check messages only within group.
 * Groups can be ordered or non-ordered. In non-ordered groups, message order will not be checked, only
 * will be checked the presence of given messages.
 * <p>
 * Message groups can also be unified into higher-level groups, that can also be ordered or non-ordered and in general
 * work like message groups.
 */
public class MessageOrderLogListener extends LogListener {
    /** */
    private final MessageGroup matchesGrp;

    /**
     * Constructor accepting array of messages.
     *
     * @param messages array of messages that will be unified into ordered group.
     */
    public MessageOrderLogListener(String... messages) {
        this(new MessageGroup(true) {{
            for (String m : messages)
                add(m);
        }});
    }

    /**
     * Constructor accepting message group
     *
     * @param matchesGrp group
     */
    public MessageOrderLogListener(MessageGroup matchesGrp) {
        this.matchesGrp = matchesGrp;
    }

    /** {@inheritDoc} */
    @Override public boolean check() {
        return matchesGrp.check();
    }

    /** {@inheritDoc} */
    @Override public void reset() {
        matchesGrp.reset();
    }

    /** {@inheritDoc} */
    @Override public void accept(String s) {
        matchesGrp.accept(s);
    }

    /**
     * Allows to unify messages into groups and groups into higher-level groups.
     */
    public static class MessageGroup extends LogListener {
        /** */
        private final boolean ordered;

        /** */
        private final String containedStr;

        /** */
        private final GridConcurrentLinkedHashSet<MessageGroup> groups;

        /** */
        private final GridConcurrentLinkedHashSet<MessageGroup> matched = new GridConcurrentLinkedHashSet<>();

        /** */
        private volatile boolean checked;

        /** */
        private volatile String actualStr;

        /** */
        private volatile int lastAcceptedIdx;

        /**
         * Default constructor.
         *
         * @param ordered whether to check order of messages in this group.
         */
        public MessageGroup(boolean ordered) {
            this(ordered, null);
        }

        /** */
        private MessageGroup(boolean ordered, String s) {
            this.ordered = ordered;

            containedStr = s;

            groups = s == null ? new GridConcurrentLinkedHashSet<>() : null;
        }

        /**
         * Adds a message regexp to group.
         *
         * @param s message regexp
         * @return this for chaining
         */
        public MessageGroup add(String s) {
            return add(new MessageGroup(false, s));
        }

        /**
         * Adds another message group to this group.
         *
         * @param grp group
         * @return this for chaining
         */
        public MessageGroup add(MessageGroup grp) {
            groups.add(grp);

            return this;
        }

        /** {@inheritDoc} */
        @Override public boolean check() {
            if (checked)
                return true;

            if (groups != null) {
                for (MessageGroup group : groups) {
                    if (!matched.contains(group) || !group.check())
                        return false;
                }

                return true;
            }

            return false;
        }

        /** {@inheritDoc} */
        @Override public void reset() {
            checked = false;

            actualStr = null;

            lastAcceptedIdx = 0;

            if (groups != null) {
                for (MessageGroup group : groups)
                    group.reset();
            }

            for (Iterator<MessageGroup> iter = matched.iterator(); iter.hasNext();) {
                iter.next();

                iter.remove();
            }
        }

        /** {@inheritDoc} */
        @Override public void accept(String s) {
            internalAccept(s);
        }

        /**
         * Tries to accept given string. If fails, tries to accept it using internal message groups.
         *
         * @param s message
         * @return whether accepted
         */
        private boolean internalAccept(String s) {
            if (checked)
                return false;
            else if (containedStr != null && s.matches(containedStr)) {
                checked = true;

                actualStr = s;

                return true;
            }
            else {
                if (groups != null) {
                    int i = 0;

                    for (MessageGroup group : groups) {
                        if (i < lastAcceptedIdx && ordered) {
                            i++;

                            continue;
                        }

                        if (group.internalAccept(s)) {
                            matched.add(group);

                            lastAcceptedIdx = i;

                            return true;
                        }

                        i++;
                    }
                }

                return false;
            }
        }

        /** */
        public String getActualStr() {
            return actualStr;
        }
    }
}
