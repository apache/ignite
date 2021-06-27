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
package org.apache.ignite.raft.jraft.entity;

import java.io.Serializable;
import org.apache.ignite.network.NetworkAddress;
import org.apache.ignite.raft.client.Peer;
import org.apache.ignite.raft.jraft.core.ElectionPriority;
import org.apache.ignite.raft.jraft.util.AsciiStringUtil;
import org.apache.ignite.raft.jraft.util.Copiable;
import org.apache.ignite.raft.jraft.util.CrcUtil;
import org.apache.ignite.raft.jraft.util.Endpoint;
import org.apache.ignite.raft.jraft.util.StringUtils;
import org.apache.ignite.raft.jraft.util.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represent a participant in a replicating group.
 */
public class PeerId implements Copiable<PeerId>, Serializable, Checksum {
    private static final long serialVersionUID = 8083529734784884641L;

    private static final Logger LOG = LoggerFactory.getLogger(PeerId.class);

    /**
     * Peer address.
     */
    private Endpoint endpoint = new Endpoint(Utils.IP_ANY, 0);

    /**
     * Index in same addr, default is 0.
     */
    private int idx; // TODO IGNITE-14832 asch drop support for peer index

    /**
     * Cached toString result.
     */
    private String str;

    /**
     * Node's local priority value, if node don't support priority election, this value is -1.
     */
    private int priority = ElectionPriority.Disabled;

    public static final PeerId ANY_PEER = new PeerId();

    private long checksum;

    public PeerId() {
        super();
    }

    @Override
    public long checksum() {
        if (this.checksum == 0) {
            this.checksum = CrcUtil.crc64(AsciiStringUtil.unsafeEncode(toString()));
        }
        return this.checksum;
    }

    /**
     * Create an empty peer.
     *
     * @return empty peer
     */
    public static PeerId emptyPeer() {
        return new PeerId();
    }

    @Override
    public PeerId copy() {
        return new PeerId(this.endpoint.copy(), this.idx, this.priority);
    }

    /**
     * Parse a peer from string in the format of "ip:port:idx", returns null if fail to parse.
     *
     * @param s input string with the format of "ip:port:idx"
     * @return parsed peer
     */
    public static PeerId parsePeer(final String s) {
        final PeerId peer = new PeerId();
        if (peer.parse(s)) {
            return peer;
        }
        return null;
    }

    public PeerId(final Endpoint endpoint, final int idx) {
        super();
        this.endpoint = endpoint;
        this.idx = idx;
    }

    public PeerId(NetworkAddress address) {
        this(address.host(), address.port());
    }

    public PeerId(final String ip, final int port) {
        this(ip, port, 0);
    }

    public PeerId(final String ip, final int port, final int idx) {
        super();
        this.endpoint = new Endpoint(ip, port);
        this.idx = idx;
    }

    public PeerId(final Endpoint endpoint, final int idx, final int priority) {
        super();
        this.endpoint = endpoint;
        this.idx = idx;
        this.priority = priority;
    }

    public PeerId(final String ip, final int port, final int idx, final int priority) {
        super();
        this.endpoint = new Endpoint(ip, port);
        this.idx = idx;
        this.priority = priority;
    }

    public Endpoint getEndpoint() {
        return this.endpoint;
    }

    public String getIp() {
        return this.endpoint.getIp();
    }

    public int getPort() {
        return this.endpoint.getPort();
    }

    public int getIdx() {
        return this.idx;
    }

    public int getPriority() {
        return priority;
    }

    public void setPriority(int priority) {
        this.priority = priority;
        this.str = null;
    }

    /**
     * Returns true when ip is ANY_IP, port is zero and idx is zero too.
     */
    public boolean isEmpty() {
        return getIp().equals(Utils.IP_ANY) && getPort() == 0 && this.idx == 0;
    }

    @Override
    public String toString() {
        if (this.str == null) {
            final StringBuilder buf = new StringBuilder(this.endpoint.toString());

            if (this.idx != 0) {
                buf.append(':').append(this.idx);
            }

            if (this.priority != ElectionPriority.Disabled) {
                if (this.idx == 0) {
                    buf.append(':');
                }
                buf.append(':').append(this.priority);
            }

            this.str = buf.toString();
        }
        return this.str;
    }

    /**
     * Parse peerId from string that generated by {@link #toString()} This method can support parameter string values
     * are below:
     *
     * <pre>
     * PeerId.parse("a:b")          = new PeerId("a", "b", 0 , -1)
     * PeerId.parse("a:b:c")        = new PeerId("a", "b", "c", -1)
     * PeerId.parse("a:b::d")       = new PeerId("a", "b", 0, "d")
     * PeerId.parse("a:b:c:d")      = new PeerId("a", "b", "c", "d")
     * </pre>
     */
    public boolean parse(final String s) {
        if (StringUtils.isEmpty(s)) {
            return false;
        }

        final String[] tmps = Utils.parsePeerId(s);
        if (tmps.length < 2 || tmps.length > 4) {
            return false;
        }
        try {
            final int port = Integer.parseInt(tmps[1]);
            this.endpoint = new Endpoint(tmps[0], port);

            switch (tmps.length) {
                case 3:
                    this.idx = Integer.parseInt(tmps[2]);
                    break;
                case 4:
                    if ("".equals(tmps[2])) {
                        this.idx = 0;
                    }
                    else {
                        this.idx = Integer.parseInt(tmps[2]);
                    }
                    this.priority = Integer.parseInt(tmps[3]);
                    break;
                default:
                    break;
            }
            this.str = null;
            return true;
        }
        catch (final Exception e) {
            LOG.error("Parse peer from string failed: {}.", s, e);
            return false;
        }
    }

    /**
     * To judge whether this node can participate in election or not.
     *
     * @return the restul that whether this node can participate in election or not.
     */
    public boolean isPriorityNotElected() {
        return this.priority == ElectionPriority.NotElected;
    }

    /**
     * To judge whether the priority election function is disabled or not in this node.
     *
     * @return the result that whether this node has priority election function or not.
     */
    public boolean isPriorityDisabled() {
        return this.priority <= ElectionPriority.Disabled;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (this.endpoint == null ? 0 : this.endpoint.hashCode());
        result = prime * result + this.idx;
        return result;
    }

    @Override
    public boolean equals(final Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final PeerId other = (PeerId) obj;
        if (this.endpoint == null) {
            if (other.endpoint != null) {
                return false;
            }
        }
        else if (!this.endpoint.equals(other.endpoint)) {
            return false;
        }
        return this.idx == other.idx;
    }

    public static PeerId fromPeer(Peer p) {
        return new PeerId(p.address().host(), p.address().port(), 0, p.getPriority());
    }
}
