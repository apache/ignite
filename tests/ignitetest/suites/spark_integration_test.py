from ducktape.tests.test import Test

from ignitetest.services.ignite import IgniteService
from ignitetest.services.ignite_client_app import IgniteClientApp, SparkIgniteClientApp
from ignitetest.services.spark import SparkService


class SparkIntegrationTest(Test):
    """
    Test performs:
    1. Start of Spark cluster.
    2. Start of Spark client application.
    3. Checks results of client application.
    """

    def __init__(self, test_context):
        super(SparkIntegrationTest, self).__init__(test_context=test_context)
        self.spark = SparkService(test_context, num_nodes=2)
        self.ignite = IgniteService(test_context, num_nodes=1)

    def setUp(self):
        # starting all nodes except last.
        self.spark.start()
        self.ignite.start()

    def teardown(self):
        self.spark.stop()
        self.ignite.stop()

    def test_spark_client(self):
        self.logger.info("Spark integration test.")

        IgniteClientApp(self.test_context,
                        java_class_name="org.apache.ignite.internal.test.IgniteApplication").run()

        SparkIgniteClientApp(self.test_context, self.spark.nodes[0]).run()
