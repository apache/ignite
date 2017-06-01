package org.apache.ignite.ml.clustering;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Test suite for all tests located in org.apache.ignite.ml.clustering package.
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        KMeansDistributedClustererTest.class,
        KMeansLocalClustererTest.class
})
public class ClusteringTesetSuite {
}
