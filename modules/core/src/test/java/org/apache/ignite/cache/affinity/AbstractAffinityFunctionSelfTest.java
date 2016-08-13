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

package org.apache.ignite.cache.affinity;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.model.Person;
import org.apache.ignite.cache.affinity.model.Organization;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.events.EventType;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityFunctionContextImpl;
import org.apache.ignite.internal.util.typedef.X;
import org.apache.ignite.testframework.GridTestNode;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/**
 *
 */
public abstract class AbstractAffinityFunctionSelfTest extends GridCommonAbstractTest {
    /** MAC prefix. */
    private static final String MAC_PREF = "MAC";

    /** Organizations cache name. */
    private static final String ORG_CACHE = "ORG_CACHE";

    /** Persons collocated with Organizations cache name. */
    private static final String COLLOCATED_PERSON_CACHE = "COLLOCATED_PERSON_CACHE";

    /**
     * Returns affinity function.
     *
     * @return Affinity function.
     */
    protected abstract AffinityFunction affinityFunction();

    /**
     * @throws Exception If failed.
     */
    public void testNodeRemovedNoBackups() throws Exception {
        checkNodeRemoved(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeRemovedOneBackup() throws Exception {
        checkNodeRemoved(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeRemovedTwoBackups() throws Exception {
        checkNodeRemoved(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNodeRemovedThreeBackups() throws Exception {
        checkNodeRemoved(3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomReassignmentNoBackups() throws Exception {
        checkRandomReassignment(0);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomReassignmentOneBackup() throws Exception {
        checkRandomReassignment(1);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomReassignmentTwoBackups() throws Exception {
        checkRandomReassignment(2);
    }

    /**
     * @throws Exception If failed.
     */
    public void testRandomReassignmentThreeBackups() throws Exception {
        checkRandomReassignment(3);
    }

    /**
     * @throws Exception If failed.
     */
    public void testNullKeyForPartitionCalculation() throws Exception {
        Ignite ignt = Ignition.start(new IgniteConfiguration());

        AffinityFunction aff = affinityFunction();

        CacheConfiguration<Long, Organization> orgCacheCfg = new CacheConfiguration<>(ORG_CACHE);

        orgCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
        orgCacheCfg.setIndexedTypes(Long.class, Organization.class);

        CacheConfiguration<AffinityKey<Long>, Person> colPersonCacheCfg = new CacheConfiguration<>(COLLOCATED_PERSON_CACHE);

        colPersonCacheCfg.setCacheMode(CacheMode.PARTITIONED); // Default.
        colPersonCacheCfg.setIndexedTypes(AffinityKey.class, Person.class);
        colPersonCacheCfg.setAffinity(aff);

        try (
            IgniteCache<Long, Organization> orgCache = ignt.getOrCreateCache(orgCacheCfg);
            IgniteCache<AffinityKey<Long>, Person> colPersonCache = ignt.getOrCreateCache(colPersonCacheCfg);
        ) {
            // Clear cache before running the example.
            orgCache.clear();

            // Organizations.
            Organization org1 = new Organization("ApacheIgnite");

            Organization org2 = new Organization("Other");

            orgCache.put(org1.id(), org1);

            orgCache.put(org2.id(), org2);

            // Clear caches before running the example.
            colPersonCache.clear();

            // People.
            Person p1 = new Person(org1, "John", "Doe", 2000, "John Doe has Master Degree.");
            Person p2 = new Person(org1, "Jane", "Doe", 1000, "Jane Doe has Bachelor Degree.");
            Person p3 = new Person(org2, "John", "Smith", 1000, "John Smith has Bachelor Degree.");
            Person p4 = new Person(org2, "Jane", "Smith", 2000, "Jane Smith has Master Degree.");

            // Note that in this test we use custom affinity key for Person objects
            // to ensure that all persons are collocated with their organizations.
            colPersonCache.put(p1.key(), p1);
            colPersonCache.put(p2.key(), p2);
            colPersonCache.put(p3.key(), p3);
            colPersonCache.put(p4.key(), p4);

            try {
                colPersonCache.get(new AffinityKey<>(p1.id));
                fail("Should throw IllegalArgumentException due to NULL affinity key.");
            } catch (IllegalArgumentException e) {
                X.printerr("Unable to get value p1 due to NULL affinity key.");
            }
        } finally {
            // Distributed cache could be removed from cluster only by #destroyCache() call.
            ignt.destroyCache(COLLOCATED_PERSON_CACHE);
            ignt.destroyCache(ORG_CACHE);
        }
    }

    /**
     * @throws Exception If failed.
     */
    protected void checkNodeRemoved(int backups) throws Exception {
        checkNodeRemoved(backups, 1, 1);
    }

    /**
     * @throws Exception If failed.
     */
    protected void checkNodeRemoved(int backups, int neighborsPerHost, int neighborsPeriod) throws Exception {

        AffinityFunction aff = affinityFunction();

        int nodesCnt = 50;

        List<ClusterNode> nodes = new ArrayList<>(nodesCnt);

        List<List<ClusterNode>> prev = null;

        for (int i = 0; i < nodesCnt; i++) {
            info("======================================");
            info("Assigning partitions: " + i);
            info("======================================");

            ClusterNode node = new GridTestNode(UUID.randomUUID());

            if (neighborsPerHost > 0)
                node.attribute(MAC_PREF + ((i / neighborsPeriod) % (nodesCnt / neighborsPerHost)));

            nodes.add(node);

            DiscoveryEvent discoEvt = new DiscoveryEvent(node, "", EventType.EVT_NODE_JOINED, node);

            GridAffinityFunctionContextImpl ctx =
                new GridAffinityFunctionContextImpl(nodes, prev, discoEvt, new AffinityTopologyVersion(i), backups);

            List<List<ClusterNode>> assignment = aff.assignPartitions(ctx);

            info("Assigned.");

            verifyAssignment(assignment, backups, aff.partitions(), nodes.size());

            prev = assignment;
        }

        info("======================================");
        info("Will remove nodes.");
        info("======================================");

        for (int i = 0; i < nodesCnt - 1; i++) {
            info("======================================");
            info("Assigning partitions: " + i);
            info("======================================");

            ClusterNode rmv = nodes.remove(nodes.size() - 1);

            DiscoveryEvent discoEvt = new DiscoveryEvent(rmv, "", EventType.EVT_NODE_LEFT, rmv);

            List<List<ClusterNode>> assignment = aff.assignPartitions(
                new GridAffinityFunctionContextImpl(nodes, prev, discoEvt, new AffinityTopologyVersion(i),
                    backups));

            info("Assigned.");

            verifyAssignment(assignment, backups, aff.partitions(), nodes.size());

            prev = assignment;
        }
    }

    /**
     * @param backups Backups.
     */
    protected void checkRandomReassignment(int backups) {
        AffinityFunction aff = affinityFunction();

        Random rnd = new Random();

        int maxNodes = 50;

        List<ClusterNode> nodes = new ArrayList<>(maxNodes);

        List<List<ClusterNode>> prev = null;

        int state = 0;

        int i = 0;

        while (true) {
            boolean add;

            if (nodes.size() < 2) {
                // Returned back to one node?
                if (state == 1)
                    return;

                add = true;
            }
            else if (nodes.size() == maxNodes) {
                if (state == 0)
                    state = 1;

                add = false;
            }
            else {
                // Nodes size in [2, maxNodes - 1].
                if (state == 0)
                    add = rnd.nextInt(3) != 0; // 66% to add, 33% to remove.
                else
                    add = rnd.nextInt(3) == 0; // 33% to add, 66% to remove.
            }

            DiscoveryEvent discoEvt;

            if (add) {
                ClusterNode addedNode = new GridTestNode(UUID.randomUUID());

                nodes.add(addedNode);

                discoEvt = new DiscoveryEvent(addedNode, "", EventType.EVT_NODE_JOINED, addedNode);
            }
            else {
                ClusterNode rmvNode = nodes.remove(rnd.nextInt(nodes.size()));

                discoEvt = new DiscoveryEvent(rmvNode, "", EventType.EVT_NODE_LEFT, rmvNode);
            }

            info("======================================");
            info("Assigning partitions [iter=" + i + ", discoEvt=" + discoEvt + ", nodesSize=" + nodes.size() + ']');
            info("======================================");

            List<List<ClusterNode>> assignment = aff.assignPartitions(
                new GridAffinityFunctionContextImpl(nodes, prev, discoEvt, new AffinityTopologyVersion(i),
                    backups));

            verifyAssignment(assignment, backups, aff.partitions(), nodes.size());

            prev = assignment;

            i++;
        }
    }


    /**
     * @param assignment Assignment to verify.
     */
    private void verifyAssignment(List<List<ClusterNode>> assignment, int keyBackups, int partsCnt, int topSize) {
        Map<UUID, Collection<Integer>> mapping = new HashMap<>();

        int ideal = Math.round((float)partsCnt / topSize * Math.min(keyBackups + 1, topSize));

        for (int part = 0; part < assignment.size(); part++) {
            for (ClusterNode node : assignment.get(part)) {
                assert node != null;

                Collection<Integer> parts = mapping.get(node.id());

                if (parts == null) {
                    parts = new HashSet<>();

                    mapping.put(node.id(), parts);
                }

                assertTrue(parts.add(part));
            }
        }

        int max = -1, min = Integer.MAX_VALUE;

        for (Collection<Integer> parts : mapping.values()) {
            max = Math.max(max, parts.size());
            min = Math.min(min, parts.size());
        }

        log().warning("max=" + max + ", min=" + min + ", ideal=" + ideal + ", minDev=" + deviation(min, ideal) + "%, " +
            "maxDev=" + deviation(max, ideal) + "%");
    }

    /**
     * @param val Value.
     * @param ideal Ideal.
     */
    private static int deviation(int val, int ideal) {
        return Math.round(Math.abs(((float)val - ideal) / ideal * 100));
    }
}
