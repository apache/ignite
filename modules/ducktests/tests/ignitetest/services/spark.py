import os.path

from ducktape.cluster.remoteaccount import RemoteCommandError
from ducktape.services.service import Service

from ignitetest.ignite_utils.ignite_config import IgniteConfig
from ignitetest.services.ignite_client_app import IgniteClientApp, create_client_configs


class SparkService(Service):
    INSTALL_DIR = "/opt/spark-{version}".format(version="2.3.4")
    PERSISTENT_ROOT = "/mnt/spark"

    logs = {}

    def __init__(self, context, num_nodes=3):
        """
        :param context: test context
        :param num_nodes: number of Ignite nodes.
        """
        Service.__init__(self, context, num_nodes)

        self.log_level = "DEBUG"
        self.ignite_config = IgniteConfig()

        for node in self.nodes:
            self.logs["master_logs" + node.account.hostname] = {
                "path": self.master_log_path(node),
                "collect_default": True
            }
            self.logs["worker_logs" + node.account.hostname] = {
                "path": self.slave_log_path(node),
                "collect_default": True
            }

    def start(self):
        Service.start(self)

        self.logger.info("Waiting for Spark to start...")

    def start_cmd(self, node):
        if node == self.nodes[0]:
            script = "start-master.sh"
        else:
            script = "start-slave.sh spark://{spark_master}:7077".format(spark_master=self.nodes[0].account.hostname)

        start_script = os.path.join(SparkService.INSTALL_DIR, "sbin", script)

        cmd = "export SPARK_LOG_DIR={spark_dir}; ".format(spark_dir=SparkService.PERSISTENT_ROOT)
        cmd += "export SPARK_WORKER_DIR={spark_dir}; ".format(spark_dir=SparkService.PERSISTENT_ROOT)
        cmd += "{start_script} &".format(start_script=start_script)

        return cmd

    def start_node(self, node, timeout_sec=30):
        create_client_configs(node, self.ignite_config)

        cmd = self.start_cmd(node)
        self.logger.debug("Attempting to start SparkService on %s with command: %s" % (str(node.account), cmd))

        if node == self.nodes[0]:
            log_file = self.master_log_path(node)
            log_msg = "Started REST server for submitting applications"
        else:
            log_file = self.slave_log_path(node)
            log_msg = "Successfully registered with master"

        self.logger.debug("Monitoring - %s" % log_file)

        with node.account.monitor_log(log_file) as monitor:
            node.account.ssh(cmd)
            monitor.wait_until(log_msg, timeout_sec=timeout_sec, backoff_sec=5,
                               err_msg="Spark doesn't start at %d seconds" % timeout_sec)

        if len(self.pids(node)) == 0:
            raise Exception("No process ids recorded on node %s" % node.account.hostname)

    def stop_node(self, node, clean_shutdown=True, timeout_sec=60):
        if node == self.nodes[0]:
            node.account.ssh(os.path.join(SparkService.INSTALL_DIR, "sbin", "stop-master.sh"))
        else:
            node.account.ssh(os.path.join(SparkService.INSTALL_DIR, "sbin", "stop-slave.sh"))

    def clean_node(self, node):
        node.account.kill_java_processes(self.java_class_name(node),
                                         clean_shutdown=False, allow_fail=True)
        node.account.ssh("sudo rm -rf -- %s" % SparkService.PERSISTENT_ROOT, allow_fail=False)

    def pids(self, node):
        """Return process ids associated with running processes on the given node."""
        try:
            cmd = "jcmd | grep -e %s | awk '{print $1}'" % self.java_class_name(node)
            pid_arr = [pid for pid in node.account.ssh_capture(cmd, allow_fail=True, callback=int)]
            return pid_arr
        except (RemoteCommandError, ValueError) as e:
            return []

    def java_class_name(self, node):
        if node == self.nodes[0]:
            return "org.apache.spark.deploy.master.Master"
        else:
            return "org.apache.spark.deploy.worker.Worker"

    def alive(self, node):
        return len(self.pids(node)) > 0

    def master_log_path(self, node):
        return "{SPARK_LOG_DIR}/spark-{userID}-org.apache.spark.deploy.master.Master-{instance}-{host}.out".format(
            SPARK_LOG_DIR=SparkService.PERSISTENT_ROOT,
            userID=node.account.user,
            instance=1,
            host=node.account.hostname)

    def slave_log_path(self, node):
        return "{SPARK_LOG_DIR}/spark-{userID}-org.apache.spark.deploy.worker.Worker-{instance}-{host}.out".format(
            SPARK_LOG_DIR=SparkService.PERSISTENT_ROOT,
            userID=node.account.user,
            instance=1,
            host=node.account.hostname)
