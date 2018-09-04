package org.apache.ignite.internal.processors.query.h2.twostep;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import javax.cache.Cache;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteSystemProperties;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.affinity.AffinityDependsOnPreviousState;
import org.apache.ignite.cache.affinity.AffinityFunction;
import org.apache.ignite.cache.affinity.AffinityFunctionContext;
import org.apache.ignite.cache.affinity.rendezvous.RendezvousAffinityFunction;
import org.apache.ignite.cache.query.SqlQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

/** */
public class QueryColocationCheckSelfTest extends GridCommonAbstractTest {
    /** */
    public static final String REPL_SQL = "select * from Organization";

    /** */
    public static final String TWO_STATELESS_SQL = "select * from Location, " +
        "\"subs\".SubOrganization as sub, \"org\".Organization as org " +
        " where Location.orgId=sub.id AND sub.orgId=org.id AND lower(org.name) = lower(?)";

    /** */
    public static final String STATEFUL_SQL = "select * from Kid, \"org\".Organization as org, " +
        "\"pers\".Person as per where Kid.parentId=per.id AND per.orgId = org.id AND lower(org.name) = lower(?)";

    /** */
    public static final String TWO_DIFFERENT_SQL = "select * from Person, \"subs\".SubOrganization as sub " +
        "where Person.orgId = sub.orgId AND lower(sub.name) = lower(?)";

    /** */
    private static final int KID_PER_PERSON_COUNT = 5;

    /** */
    private static final int SUBS_PER_ORGANISATION_COUNT = 10;

    /** */
    private static final int NODES_COUNT = 1;

    /** */
    private static final String ORG = "org";

    /** */
    private static IgniteCache<String, JoinSqlTestHelper.Person> personCache;

    /** */
    private static IgniteCache<String, JoinSqlTestHelper.Organization> orgCache;

    /** */
    private static IgniteCache<String, Kid> kidCache;

    /** */
    private static IgniteCache<String, SubOrganization> subOrgCache;

    /** */
    private static IgniteCache<String, Location> locationCache;

    /** */
    public void testPassDistributedJoins(){
        SqlQuery<String, JoinSqlTestHelper.Person> qry = new SqlQuery<String, JoinSqlTestHelper.Person>(
            JoinSqlTestHelper.Person.class, TWO_DIFFERENT_SQL).setArgs("sub1");

        qry.setDistributedJoins(true);

        List<Cache.Entry<String,JoinSqlTestHelper.Person>> prsns = personCache.query(qry).getAll();

        assertNotNull(prsns);
    }

    /** */
    public void testPassAllReplicated(){
        SqlQuery<String, JoinSqlTestHelper.Organization> qry = new SqlQuery<String, JoinSqlTestHelper.Organization>(
            JoinSqlTestHelper.Organization.class, REPL_SQL);

        List<Cache.Entry<String,JoinSqlTestHelper.Organization>> orgs = orgCache.query(qry).getAll();

        assertNotNull(orgs);
    }

    /** */
    public void testPassReplicatedAndOnePartitioned(){
        SqlQuery<String, JoinSqlTestHelper.Person> qry = new SqlQuery<String, JoinSqlTestHelper.Person>(
            JoinSqlTestHelper.Person.class, JoinSqlTestHelper.JOIN_SQL).setArgs("Organization #0");

        List<Cache.Entry<String,JoinSqlTestHelper.Person>> prsns = personCache.query(qry).getAll();

        assertNotNull(prsns);
    }

    /** */
    public void testPassReplicatedAndTwoStatelessPartitioned(){
        SqlQuery<String, Location> qry = new SqlQuery<String, Location>(
            Location.class, TWO_STATELESS_SQL).setArgs("Organization #0");

        List<Cache.Entry<String,Location>> locs = locationCache.query(qry).getAll();

        assertNotNull(locs);
    }

    public void testFailReplicatedAndStatefulPartitioned(){
        SqlQuery<String, Kid> qry = new SqlQuery<String, Kid>(
            Kid.class, STATEFUL_SQL).setArgs("Organization #0");

        try {
            List<Cache.Entry<String, Kid>> kids = kidCache.query(qry).getAll();

            fail("No exceptions are emitted. Check property is set to"+IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_PARTITIONS_CO_LOCATION_CHECK_ENABLED));
        }catch (Exception e){
            log().error("Caught exception", e);
            
            assertTrue(e.getMessage(), e.getMessage().contains("Partitioned cache uses stateful affinity function ["));
        }
    }

    public void testFailReplicatedAndTwoDifferentAffinityPartitioned(){
        SqlQuery<String, JoinSqlTestHelper.Person> qry = new SqlQuery<String, JoinSqlTestHelper.Person>(
            JoinSqlTestHelper.Person.class, TWO_DIFFERENT_SQL).setArgs("sub1");

        try {
            List<Cache.Entry<String, JoinSqlTestHelper.Person>> prsns = personCache.query(qry).getAll();

            fail("No exceptions are emitted. Check property is set to"+IgniteSystemProperties.getBoolean(IgniteSystemProperties.IGNITE_PARTITIONS_CO_LOCATION_CHECK_ENABLED));
        }catch (Exception e){
            log().error("Caught exception", e);

            assertTrue(e.getMessage(), e.getMessage().contains("Query has cache groups with different affinity functions ["));
        }
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        System.setProperty(IgniteSystemProperties.IGNITE_PARTITIONS_CO_LOCATION_CHECK_ENABLED, "true");

        startGridsMultiThreaded(NODES_COUNT, false);

        final AffinityFunction statefulAffinity = new TestAffinityFunction();

        final AffinityFunction statelessAffinity = new RendezvousAffinityFunction();

        personCache = ignite(0).getOrCreateCache(new CacheConfiguration<String, JoinSqlTestHelper.Person>("pers")
            .setIndexedTypes(String.class, JoinSqlTestHelper.Person.class).setAffinity(new RendezvousAffinityFunction())
        );

        orgCache = ignite(0).getOrCreateCache(new CacheConfiguration<String, JoinSqlTestHelper.Organization>(ORG)
            .setCacheMode(CacheMode.REPLICATED)
            .setIndexedTypes(String.class, JoinSqlTestHelper.Organization.class)
        );

        kidCache = ignite(0).getOrCreateCache(new CacheConfiguration<String, Kid>("kids")
            .setIndexedTypes(String.class, Kid.class).setAffinity(statefulAffinity)
        );

        subOrgCache = ignite(0).getOrCreateCache(new CacheConfiguration<String, SubOrganization>("subs")
            .setIndexedTypes(String.class, SubOrganization.class).setAffinity(statelessAffinity)
        );

        locationCache = ignite(0).getOrCreateCache(new CacheConfiguration<String, Location>("locs")
            .setIndexedTypes(String.class, Location.class).setAffinity(statelessAffinity)
        );
        awaitPartitionMapExchange();

        JoinSqlTestHelper.populateDataIntoOrg(orgCache);
        JoinSqlTestHelper.populateDataIntoPerson(personCache);
        populateDataIntoKid(kidCache);
        populateDataIntoSub(subOrgCache);
        populateDataIntoLocation(locationCache);
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        // Give some time to clean up.
        try {
            Thread.sleep(3000);
        }
        catch (InterruptedException e) {
            // no op
        }
    }

    /** {@inheritDoc} */
    @Override protected void afterTestsStopped() throws Exception {
        stopAllGrids();
    }

    /** */
    private void populateDataIntoKid(IgniteCache<String, Kid> cache) {
        for(int i=0;i<JoinSqlTestHelper.PERSON_PER_ORG_COUNT*JoinSqlTestHelper.ORG_COUNT; i++)
            for (int j = 0; j < KID_PER_PERSON_COUNT; j++) {
                Kid kid = new Kid();

                kid.setId("kid" + j);

                kid.setParentId("pers"+i);

                kid.setName("kid"+j+"Of"+i);

                cache.put(kid.getId(), kid);
            }
    }

    /** */
    private void populateDataIntoSub(IgniteCache<String, SubOrganization> cache) {
        for(int i=0;i<JoinSqlTestHelper.ORG_COUNT; i++)
            for (int j = 0; j < SUBS_PER_ORGANISATION_COUNT; j++) {
                SubOrganization sub = new SubOrganization();

                sub.setId("sub" + j);

                sub.setOrgId("org"+i);

                sub.setName("sub"+j+"Of"+i);

                cache.put(sub.getId(), sub);
            }
    }

    /** */
    private void populateDataIntoLocation(IgniteCache<String, Location> cache) {
        for(int i=0;i<JoinSqlTestHelper.ORG_COUNT*SUBS_PER_ORGANISATION_COUNT; i++) {
            Location sub = new Location();

            sub.setId("loc" + i);

            sub.setOrgId("sub" + i);

            sub.setName("loc" + i + "Of" + i);

            cache.put(sub.getId(), sub);
        }
    }

    /**
     * Test affinity function.
     */
    @AffinityDependsOnPreviousState
    private static class TestAffinityFunction implements AffinityFunction{
        /** {@inheritDoc} */
        @Override public void reset() {
            // No-op.
        }

        /** {@inheritDoc} */
        @Override public int partitions() {
            return 1;
        }

        /** {@inheritDoc} */
        @Override public int partition(Object key) {
            return 0;
        }

        /** {@inheritDoc} */
        @Override public List<List<ClusterNode>> assignPartitions(AffinityFunctionContext affCtx) {
            List<List<ClusterNode>> res = new ArrayList<>();

            res.add(nodes(0, affCtx.currentTopologySnapshot()));

            return res;
        }

        /** {@inheritDoc} */
        public List<ClusterNode> nodes(int part, Collection<ClusterNode> nodes) {
            return new ArrayList<>(nodes);
        }

        /** {@inheritDoc} */
        @Override public void removeNode(UUID nodeId) {
            // No-op.
        }
    }

    /** */
    public class Kid {
        /** */
        @QuerySqlField(index = true)
        private String id;

        /** */
        @QuerySqlField(index = true)
        private String parentId;

        /** */
        @QuerySqlField(index = true)
        private String name;

        /** */
        public String getId() {
            return id;
        }

        /** */
        public void setId(String id) {
            this.id = id;
        }

        /** */
        public String getparentId() {
            return parentId;
        }

        /** */
        public void setParentId(String parentId) {
            this.parentId = parentId;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public void setName(String name) {
            this.name = name;
        }
    }

    /** */
    public class SubOrganization {
        /** */
        @QuerySqlField(index = true)
        private String id;

        /** */
        @QuerySqlField(index = true)
        private String orgId;

        /** */
        @QuerySqlField(index = true)
        private String name;

        /** */
        public String getId() {
            return id;
        }

        /** */
        public void setId(String id) {
            this.id = id;
        }

        /** */
        public String getOrgId() {
            return orgId;
        }

        /** */
        public void setOrgId(String orgId) {
            this.orgId = orgId;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public void setName(String name) {
            this.name = name;
        }
    }

    /** */
    public class Location {
        /** */
        @QuerySqlField(index = true)
        private String id;

        /** */
        @QuerySqlField(index = true)
        private String orgId;

        /** */
        @QuerySqlField(index = true)
        private String name;

        /** */
        public String getId() {
            return id;
        }

        /** */
        public void setId(String id) {
            this.id = id;
        }

        /** */
        public String getOrgId() {
            return orgId;
        }

        /** */
        public void setOrgId(String orgId) {
            this.orgId = orgId;
        }

        /** */
        public String getName() {
            return name;
        }

        /** */
        public void setName(String name) {
            this.name = name;
        }
    }
}
