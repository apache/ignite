/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.raft.jraft.conf;

import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.raft.jraft.entity.LogId;
import org.apache.ignite.raft.jraft.entity.PeerId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A configuration entry with current peers and old peers.
 */
public class ConfigurationEntry {

    private static final Logger LOG = LoggerFactory.getLogger(ConfigurationEntry.class);

    private LogId id = new LogId(0, 0);
    private Configuration conf = new Configuration();
    private Configuration oldConf = new Configuration();

    public LogId getId() {
        return this.id;
    }

    public void setId(final LogId id) {
        this.id = id;
    }

    public Configuration getConf() {
        return this.conf;
    }

    public void setConf(final Configuration conf) {
        this.conf = conf;
    }

    public Configuration getOldConf() {
        return this.oldConf;
    }

    public void setOldConf(final Configuration oldConf) {
        this.oldConf = oldConf;
    }

    public ConfigurationEntry() {
        super();
    }

    public ConfigurationEntry(final LogId id, final Configuration conf, final Configuration oldConf) {
        super();
        this.id = id;
        this.conf = conf;
        this.oldConf = oldConf;
    }

    public boolean isStable() {
        return this.oldConf.isEmpty();
    }

    public boolean isEmpty() {
        return this.conf.isEmpty();
    }

    public Set<PeerId> listPeers() {
        final Set<PeerId> ret = new HashSet<>(this.conf.listPeers());
        ret.addAll(this.oldConf.listPeers());
        return ret;
    }

    /**
     * Returns true when the conf entry is valid.
     *
     * @return if the the entry is valid
     */
    public boolean isValid() {
        if (!this.conf.isValid()) {
            return false;
        }

        // The peer set and learner set should not have intersection set.
        final Set<PeerId> intersection = listPeers();
        intersection.retainAll(listLearners());
        if (intersection.isEmpty()) {
            return true;
        }
        LOG.error("Invalid conf entry {}, peers and learners have intersection: {}.", this, intersection);
        return false;
    }

    public Set<PeerId> listLearners() {
        final Set<PeerId> ret = new HashSet<>(this.conf.getLearners());
        ret.addAll(this.oldConf.getLearners());
        return ret;
    }

    public boolean containsLearner(final PeerId learner) {
        return this.conf.getLearners().contains(learner) || this.oldConf.getLearners().contains(learner);
    }

    public boolean contains(final PeerId peer) {
        return this.conf.contains(peer) || this.oldConf.contains(peer);
    }

    @Override
    public String toString() {
        return "ConfigurationEntry [id=" + this.id + ", conf=" + this.conf + ", oldConf=" + this.oldConf + "]";
    }
}
