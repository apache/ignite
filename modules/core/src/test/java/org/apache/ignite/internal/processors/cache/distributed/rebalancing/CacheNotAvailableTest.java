package org.apache.ignite.internal.processors.cache.distributed.rebalancing;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import javax.cache.processor.EntryProcessorException;
import javax.cache.processor.MutableEntry;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteException;
import org.apache.ignite.cache.CacheEntryProcessor;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.cache.CacheRebalanceMode;
import org.apache.ignite.cache.CacheWriteSynchronizationMode;
import org.apache.ignite.cache.affinity.AffinityKeyMapped;
import org.apache.ignite.cache.affinity.fair.FairAffinityFunction;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.events.CacheEvent;
import org.apache.ignite.events.CacheRebalancingEvent;
import org.apache.ignite.events.Event;
import org.apache.ignite.events.EventType;
import org.apache.ignite.lang.IgnitePredicate;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.services.Service;
import org.apache.ignite.services.ServiceContext;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.assertions.Assertion;
import org.apache.ignite.testframework.junits.common.GridRollingRestartAbstractTest;
import org.junit.Test;

/**
 * Created by spencer.firestone on 2/17/17.
 */
public class CacheNotAvailableTest
        extends GridRollingRestartAbstractTest {

    private static final String CACHE_NAME = "myCache";

    /**
     * Number of companies for which to create data.
     */
    private static final int COMPANY_COUNT = 10;

    /**
     * Number of employees per company.
     */
    private static final int EMPLOYEE_COUNT = 100;

    /**
     * Flag to indicate that the topology is changing because of the {@link RollingRestartThread}.
     */
    private volatile boolean topologyChanging = false;

    @Override
    public IgnitePredicate<Ignite> getRestartCheck() {
        return (IgnitePredicate<Ignite>) ignite -> topologyChanging;
    }

    @Override
    public int getMaxRestarts() {
        return 5;
    }

    @Override
    public int getRestartInterval() {
        return 5_000;
    }

    @Override
    public Assertion getRestartAssertion() {
        return new CacheNodeSafeAssertion(grid(0), CACHE_NAME);
    }

    @Override
    public int serverCount() {
        return 3;
    }

    @Override
    protected CacheConfiguration<Employee, Integer> getCacheConfiguration() {
        return new CacheConfiguration<Employee, Integer>()
                .setCacheMode(CacheMode.PARTITIONED)
                .setBackups(1)
                .setWriteSynchronizationMode(CacheWriteSynchronizationMode.FULL_SYNC)
                .setRebalanceMode(CacheRebalanceMode.SYNC)
                .setAffinity(new FairAffinityFunction(COMPANY_COUNT));
    }

    @Test
    public void testCacheNotAvailable() throws Exception {
        final Ignite ignite = grid(0);
        ignite.services().deployNodeSingleton("myService", new TestService());
        System.out.println(ignite.services().serviceDescriptors());
        GridTestUtils.waitForCondition(() ->  ignite.services().serviceDescriptors().size() == 1, 5000);

        final IgniteCache<Employee, Integer> cache = ignite.getOrCreateCache(CACHE_NAME);

        final Map<String, Integer> expectedCompanySalaries = new HashMap<>();
        final Map<Employee, Integer> expectedEmployeeSalaries = new HashMap<>();

        populateCache(cache, expectedCompanySalaries, expectedEmployeeSalaries);

        // Verify partitions initially
        verifyPartitions(cache, expectedEmployeeSalaries.keySet(), expectedCompanySalaries);

        this.topologyChanging = true;

        // In a loop, call EntryProcessor on each key to ensure the partition isn't rebalancing and to sum the Employee
        // salaries. Trigger rolling restarts during this time.
        while (this.rollingRestartThread.getRestartTotal() < getMaxRestarts()) {
            try {
                verifyPartitions(cache, expectedEmployeeSalaries.keySet(), expectedCompanySalaries);
            }
            catch (EntryProcessorException e) {
                // We need this here to catch the IgniteException that is being thrown 20% - 30% of the time after a
                // restarted node because the cache is not yet available. This is what we are trying to trigger with the
                // test.
                if (e.getCause() instanceof IgniteException) {
                    System.out.format("Ignoring temporary Ignite exception: %s\n", e.getMessage());
                }
//                else {
                throw e;
//                }
            }
        }
    }

    private void populateCache(final IgniteCache<Employee, Integer> cache,
                               final Map<String, Integer> expectedCompanySalaries,
                               final Map<Employee, Integer> expectedEmployeeSalaries) {
        final ThreadLocalRandom random = ThreadLocalRandom.current();

        // Populate cache
        IntStream.range(0, COMPANY_COUNT).forEach(companyId -> {
            // InitialEmployeeId is 1 + a random multiple of 100 between 0 and 99 inclusive.
            final String companyName = "company" + (char) ('A' + companyId);
            final int initialEmployeeId = 1 + random.nextInt(100) * 100;
            // Add employees to each company with their salary at 10x their employee ID
            IntStream.range(initialEmployeeId, initialEmployeeId + EMPLOYEE_COUNT).forEach(employeeId -> {
                final Employee employee = new Employee(employeeId, companyName);
                final int employeeSalary = employeeId * 10;

                // Update local expected company salary
                expectedCompanySalaries.merge(companyName, employeeSalary, Integer::sum);

                // Update local cache
                expectedEmployeeSalaries.put(employee, employeeSalary);

                // Update fabric cache
                cache.put(employee, employeeSalary);
            });
        });

    }

    /**
     * Verifies that the partitions for the given {@link Employee}s are in
     * a stable state and have all the company salary information expected.
     *
     * @param cache the cache to verify
     * @param employees the set of {@link Employee}s (cache keys)
     * @param expectedCompanySalaries a map of the expected sum of employee salaries per company
     */
    private void verifyPartitions(final IgniteCache<Employee, Integer> cache,
                                  final Set<Employee> employees,
                                  final Map<String, Integer> expectedCompanySalaries) {
        cache.invokeAll(employees, new CompanySalaryVerifier())
                .forEach((employee, result) ->
                        assertVerifierResult(expectedCompanySalaries.get(employee.getCompanyName()), result.get()));
    }

    /**
     * Asserts that a given {@link TestService.VerifierResult} has the expected salary value and that
     * its partition is in a stable state.
     *
     * @param expectedCompanySalary the expected company salary value to compare against the result
     * @param result the {@link TestService.VerifierResult} to validate
     */
    private void assertVerifierResult(final int expectedCompanySalary,
                                      final TestService.VerifierResult result) {
        if (expectedCompanySalary != result.getCompanySalarySum()) {
            System.out.format("%s [p%d]: %d, %s (Expecting %d)\n", result.getNodeId(), result.getPartition(),
                    result.getCompanySalarySum(), result.getPartitionState(), expectedCompanySalary);
        }
        assertEquals(String.format("Partition %d on node %s was in the %s state",
                result.getPartition(), result.getNodeId(), result.getPartitionState()),
                PartitionState.STABLE, result.getPartitionState());
        assertEquals(String.format("Sum of salaries for the company on partition %d on node %s was not what was expected",
                result.getPartition(), result.getNodeId()),
                expectedCompanySalary, result.getCompanySalarySum());
    }

    /**
     * {@link CacheEntryProcessor} to track if any LOADED/UNLOADED events have occurred while the
     * EntryProcessor is calculating a sum of all {@link Employee} salaries stored on the partition.
     */
    static class CompanySalaryVerifier
            implements CacheEntryProcessor<Employee, Integer, TestService.VerifierResult> {
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * Calculates a sum of {@link Employee} salaries for the partition and returns the result along with the
         * stability of the partition during the calculation.
         *
         * @param entry the entry to process
         * @return a {@link TestService.VerifierResult} with the execution's {@link PartitionState} and the salary sum
         */
        @Override
        public TestService.VerifierResult process(final MutableEntry<Employee, Integer> entry, final Object... objects) {
            final int partition = this.ignite.affinity(CACHE_NAME).partition(entry.getKey());

            final TestService.VerifierResult result = new TestService.VerifierResult(partition, ignite.cluster().localNode().id().toString());

            result.mergeUnstablePartitionState(partition);

            StreamSupport.stream(this.ignite.<Employee, Integer>cache(CACHE_NAME).localEntries().spliterator(), false)
                    .filter(localEntry -> entry.getKey().getCompanyName().equals(localEntry.getKey().getCompanyName()))
                    .forEach(localEntry -> {
                        result.mergeUnstablePartitionState(partition);
                        result.addSalary(localEntry.getValue());
                    });
            return result;
        }
    }

    private static class TestService
            implements Service {
        /** Auto-injected grid instance. */
        @IgniteInstanceResource
        private Ignite ignite;

        /**
         * The map of this service's node's partition states.
         */
        private final static Map<Integer, PartitionState> PARTITION_STATES = new ConcurrentHashMap<>();

        @Override
        public void cancel(final ServiceContext ctx) {
            this.ignite.events().stopLocalListen(this.partitionStateUpdater);
        }

        @Override
        public void init(final ServiceContext ctx) throws Exception {
            this.ignite.events().localListen(this.partitionStateUpdater,
                    EventType.EVT_CACHE_REBALANCE_STARTED,
                    EventType.EVT_CACHE_REBALANCE_PART_DATA_LOST,
                    EventType.EVT_CACHE_REBALANCE_STOPPED,
                    EventType.EVT_CACHE_ENTRY_CREATED,
                    EventType.EVT_CACHE_ENTRY_DESTROYED,
                    EventType.EVT_CACHE_ENTRY_EVICTED,

                    EventType.EVT_CACHE_OBJECT_PUT,
                    EventType.EVT_CACHE_OBJECT_REMOVED,
                    EventType.EVT_CACHE_OBJECT_EXPIRED,
                    EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED,
                    EventType.EVT_CACHE_REBALANCE_OBJECT_UNLOADED,
                    EventType.EVT_CACHE_REBALANCE_PART_LOADED,
                    EventType.EVT_CACHE_REBALANCE_PART_UNLOADED);
        }

        @Override
        public void execute(final ServiceContext ctx) throws Exception {
        }

        private final IgnitePredicate<Event> partitionStateUpdater = evt -> {
            // Handle:
            // EVT_CACHE_REBALANCE_OBJECT_LOADED
            // EVT_CACHE_REBALANCE_OBJECT_UNLOADED
            if (evt instanceof CacheEvent) {
                final CacheEvent cacheEvt = (CacheEvent) evt;
//                System.out.format("%s part=%d, eventType=%s, eventNode=%s: (%s: %s -> %s)\n", cacheEvt.node().id(), cacheEvt.partition(), evt.name(), cacheEvt.eventNode().id(), cacheEvt.key(), cacheEvt.oldValue(), cacheEvt.newValue());
                switch (cacheEvt.type()) {
                    case EventType.EVT_CACHE_REBALANCE_OBJECT_LOADED:
                        PARTITION_STATES.put(cacheEvt.partition(), PartitionState.LOADING);
                        break;
                    case EventType.EVT_CACHE_REBALANCE_OBJECT_UNLOADED:
                        PARTITION_STATES.put(cacheEvt.partition(), PartitionState.UNLOADING);
                        break;
                }
            }

            // Handle:
            // EVT_CACHE_REBALANCE_PART_LOADED
            // EVT_CACHE_REBALANCE_PART_UNLOADED
            if (evt instanceof CacheRebalancingEvent) {
                final CacheRebalancingEvent cacheRebalancingEvt = (CacheRebalancingEvent) evt;
                System.out.format("part=%d, eventType=%s\n", cacheRebalancingEvt.partition(), evt.name());
                switch (cacheRebalancingEvt.type()) {
                    case EventType.EVT_CACHE_REBALANCE_PART_LOADED:
                    case EventType.EVT_CACHE_REBALANCE_PART_UNLOADED:
                        PARTITION_STATES.remove(cacheRebalancingEvt.partition());
                        break;
                }
            }
            return true;
        };

        /**
         * A class to store the results of the {@link CompanySalaryVerifier}'s execution. The {@link PartitionState} will
         * be {@link PartitionState#STABLE} unless a {@link PartitionState#LOADING} or {@link PartitionState#UNLOADING}
         * event has occurred during execution. The sum of the partition's salaries is also returned to compare against
         * an expected value.
         */
        private static class VerifierResult
                implements Serializable {
            /**
             * The partition on which the verifier was run.
             */
            private final int partition;

            /**
             * The id of the node on which the verifier was run.
             */
            private final String nodeId;

            /**
             * The state of the partition on which the verifier was run.
             */
            private PartitionState partitionState = PartitionState.STABLE;

            /**
             * The sum of the salaries on the partition on which the verifier was run.
             */
            private int companySalarySum = 0;

            /**
             * Creates a new {@link VerifierResult}.
             *
             * @param partition the partition that this result is running on
             * @param nodeId the node on which this partition is contained
             */
            private VerifierResult(final int partition, final String nodeId) {
                this.partition = partition;
                this.nodeId = nodeId;
            }

            /**
             * Updates the result's STABLE {@link PartitionState} for the given partition. If the result's state is not
             * STABLE, does nothing.
             *
             * @param partition the partition to check
             */
            private void mergeUnstablePartitionState(final int partition) {
                if (this.partitionState == PartitionState.STABLE) {
                    this.partitionState = PARTITION_STATES.getOrDefault(partition, PartitionState.STABLE);
                }
            }

            /**
             * Adds the salary to the running company salary sum.
             *
             * @param salary salary to add to the sum
             */
            private void addSalary(final int salary) {
                this.companySalarySum += salary;
            }

            /**
             * Gets the partition on which the verifier was run.
             *
             * @return the partition on which the verifier was run
             */
            int getPartition() {
                return this.partition;
            }

            /**
             * Gets the ID of the node on which the verifier was run.
             *
             * @return the ID of the node on which the verifier was run.
             */
            String getNodeId() {
                return this.nodeId;
            }

            /**
             * If the partition received any {@link PartitionState#LOADING} or {@link PartitionState#UNLOADING} events
             * during the Verifier's run, returns those values. Otherwise returns {@link PartitionState#STABLE}.
             *
             * @return the least stable {@link PartitionState} of the partition during the verifier's run
             */
            PartitionState getPartitionState() {
                return this.partitionState;
            }

            /**
             * Gets the sum of the salaries stored on the partition.
             *
             * @return the sum of the salaries stored on the partition
             */
            int getCompanySalarySum() {
                return this.companySalarySum;
            }
        }
    }

    /**
     * Employee object to contain an employee ID and a company name (used for affinity).
     */
    private static class Employee {

        /**
         * Employee ID.
         */
        private final int id;

        /**
         * Employee's company's name (used for affinity).
         */
        @AffinityKeyMapped
        private final String companyName;

        /**
         * Constructor.
         *
         * @param id employee ID
         * @param companyName employee's company's name
         */
        Employee(final int id, final String companyName) {
            this.id = id;
            this.companyName = companyName;
        }

        /**
         * Gets the employee ID.
         *
         * @return the employee ID
         */
        public int getId() {
            return this.id;
        }

        /**
         * Gets the employee's company's name.
         *
         * @return the employee's company's name
         */
        public String getCompanyName() {
            return this.companyName;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            final Employee that = (Employee) o;
            return this.id == that.id &&
                    Objects.equals(this.companyName, that.companyName);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.id, this.companyName);
        }

        @Override
        public String toString() {
            return this.companyName + "-" + this.id;
        }
    }

    /**
     * Enumeration for the state of a partition.
     */
    enum PartitionState {
        STABLE,
        LOADING,
        UNLOADING
    }
}
