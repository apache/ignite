
from ducktape.tests.test import Test

from ignitetest.services.zk.zookeeper import ZookeeperService


class ZkTest(Test):
    def __init__(self, test_context):
        super(ZkTest, self).__init__(test_context=test_context)

        self.zk = ZookeeperService(test_context, num_nodes=3)
        #self.ignite = IgniteService(test_context, num_nodes=1)
    def setUp(self):
        pass # self.zk.start()

    def teardown(self):
        self.zk.stop()

    def test(self):
        self.zk.start()

        for node in self.zk.nodes:
            print(node)