<!--
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
-->

Checkpointing provides an ability to save an intermediate job state. It can be useful when long running jobs need to store some intermediate state to protect from node failures. Then on restart of a failed node, a job would load the saved checkpoint and continue from where it left off. The only requirement for job checkpoint state is to implement `java.io.Serializable` interface.

Checkpoints are available through the following methods on `GridTaskSession` interface:
* `ComputeTaskSession.loadCheckpoint(String)`
* `ComputeTaskSession.removeCheckpoint(String)`
* `ComputeTaskSession.saveCheckpoint(String, Object)`
[block:api-header]
{
  "type": "basic",
  "title": "Master Node Failure Protection"
}
[/block]
One important use case for checkpoint that is not readily apparent is to guard against failure of the "master" node - the node that started the original execution. When master node fails, Ignite doesn’t anywhere to send the results of job execution to, and thus the result will be discarded.

To failover this scenario one can store the final result of the job execution as a checkpoint and have the logic re-run the entire task in case of a "master" node failure. In such case the task re-run will be much faster since all the jobs' can start from the saved checkpoints.
[block:api-header]
{
  "type": "basic",
  "title": "Setting Checkpoints"
}
[/block]
Every compute job can periodically *checkpoint* itself by calling `ComputeTaskSession.saveCheckpoint(...)` method.

If job did save a checkpoint, then upon beginning of its execution, it should check if the checkpoint is available and start executing from the last saved checkpoint.
[block:code]
{
  "codes": [
    {
      "code": "IgniteCompute compute = ignite.compute();\n\ncompute.run(new IgniteRunnable() {\n  // Task session (injected on closure instantiation).\n  @TaskSessionResource\n  private ComputeTaskSession ses;\n\n  @Override \n  public Object applyx(Object arg) throws GridException {\n    // Try to retrieve step1 result.\n    Object res1 = ses.loadCheckpoint(\"STEP1\");\n\n    if (res1 == null) {\n      res1 = computeStep1(arg); // Do some computation.\n\n      // Save step1 result.\n      ses.saveCheckpoint(\"STEP1\", res1);\n    }\n\n    // Try to retrieve step2 result.\n    Object res2 = ses.loadCheckpoint(\"STEP2\");\n\n    if (res2 == null) {\n      res2 = computeStep2(res1); // Do some computation.\n\n      // Save step2 result.\n      ses.saveCheckpoint(\"STEP2\", res2);\n    }\n\n    ...\n  }\n}",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "CheckpointSpi"
}
[/block]
In Ignite, checkpointing functionality is provided by `CheckpointSpi` which has the following out-of-the-box implementations:
[block:parameters]
{
  "data": {
    "h-0": "Class",
    "h-1": "Description",
    "h-2": "Default",
    "0-0": "[SharedFsCheckpointSpi](#file-system-checkpoint-configuration)\n(default)",
    "0-1": "This implementation uses a shared file system to store checkpoints.",
    "0-2": "Yes",
    "1-0": "[CacheCheckpointSpi](#cache-checkpoint-configuration)",
    "1-1": "This implementation uses a cache to store checkpoints.",
    "2-0": "[JdbcCheckpointSpi](#database-checkpoint-configuration)",
    "2-1": "This implementation uses a database to store checkpoints.",
    "3-1": "This implementation uses Amazon S3 to store checkpoints.",
    "3-0": "[S3CheckpointSpi](#amazon-s3-checkpoint-configuration)"
  },
  "cols": 2,
  "rows": 4
}
[/block]
`CheckpointSpi` is provided in `IgniteConfiguration` and passed into Ignition class at startup. 
[block:api-header]
{
  "type": "basic",
  "title": "File System Checkpoint Configuration"
}
[/block]
The following configuration parameters can be used to configure `SharedFsCheckpointSpi`:
[block:parameters]
{
  "data": {
    "h-0": "Setter Method",
    "h-1": "Description",
    "h-2": "Default",
    "0-0": "`setDirectoryPaths(Collection)`",
    "0-1": "Sets directory paths to the shared folders where checkpoints are stored. The path can either be absolute or relative to the path specified in `IGNITE_HOME` environment or system varialble.",
    "0-2": "`IGNITE_HOME/work/cp/sharedfs`"
  },
  "cols": 3,
  "rows": 1
}
[/block]

[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.IgniteConfiguration\" singleton=\"true\">\n  ...\n  <property name=\"checkpointSpi\">\n    <bean class=\"org.apache.ignite.spi.checkpoint.sharedfs.SharedFsCheckpointSpi\">\n    <!-- Change to shared directory path in your environment. -->\n      <property name=\"directoryPaths\">\n        <list>\n          <value>/my/directory/path</value>\n          <value>/other/directory/path</value>\n        </list>\n      </property>\n    </bean>\n  </property>\n  ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "IgniteConfiguration cfg = new IgniteConfiguration();\n \nSharedFsCheckpointSpi checkpointSpi = new SharedFsCheckpointSpi();\n \n// List of checkpoint directories where all files are stored.\nCollection<String> dirPaths = new ArrayList<String>();\n \ndirPaths.add(\"/my/directory/path\");\ndirPaths.add(\"/other/directory/path\");\n \n// Override default directory path.\ncheckpointSpi.setDirectoryPaths(dirPaths);\n \n// Override default checkpoint SPI.\ncfg.setCheckpointSpi(checkpointSpi);\n \n// Starts Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Cache Checkpoint Configuration"
}
[/block]
`CacheCheckpointSpi` is a cache-based implementation for checkpoint SPI. Checkpoint data will be stored in the Ignite data grid in a pre-configured cache. 

The following configuration parameters can be used to configure `CacheCheckpointSpi`:
[block:parameters]
{
  "data": {
    "h-0": "Setter Method",
    "h-1": "Description",
    "h-2": "Default",
    "0-0": "`setCacheName(String)`",
    "0-1": "Sets cache name to use for storing checkpoints.",
    "0-2": "`checkpoints`"
  },
  "cols": 3,
  "rows": 1
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Database Checkpoint Configuration"
}
[/block]
`JdbcCheckpointSpi` uses database to store checkpoints. All checkpoints are stored in the database table and are available from all nodes in the grid. Note that every node must have access to the database. A job state can be saved on one node and loaded on another (e.g., if a job gets preempted on a different node after node failure).

The following configuration parameters can be used to configure `JdbcCheckpointSpi` (all are optional):
[block:parameters]
{
  "data": {
    "h-0": "Setter Method",
    "h-1": "Description",
    "h-2": "Default",
    "0-0": "`setDataSource(DataSource)`",
    "0-1": "Sets DataSource to use for database access.",
    "0-2": "No value",
    "1-0": "`setCheckpointTableName(String)`",
    "1-1": "Sets checkpoint table name.",
    "1-2": "`CHECKPOINTS`",
    "2-0": "`setKeyFieldName(String)`",
    "2-1": "Sets checkpoint key field name.",
    "2-2": "`NAME`",
    "3-0": "`setKeyFieldType(String)`",
    "3-1": "Sets checkpoint key field type. The field should have corresponding SQL string type (`VARCHAR` , for example).",
    "3-2": "`VARCHAR(256)`",
    "4-0": "`setValueFieldName(String)`",
    "4-1": "Sets checkpoint value field name.",
    "4-2": "`VALUE`",
    "5-0": "`setValueFieldType(String)`",
    "5-1": "Sets checkpoint value field type. Note, that the field should have corresponding SQL BLOB type. The default value is BLOB, won’t work for all databases. For example, if using HSQL DB, then the type should be `longvarbinary`.",
    "5-2": "`BLOB`",
    "6-0": "`setExpireDateFieldName(String)`",
    "6-1": "Sets checkpoint expiration date field name.",
    "6-2": "`EXPIRE_DATE`",
    "7-0": "`setExpireDateFieldType(String)`",
    "7-1": "Sets checkpoint expiration date field type. The field should have corresponding SQL `DATETIME` type.",
    "7-2": "`DATETIME`",
    "8-0": "`setNumberOfRetries(int)`",
    "8-1": "Sets number of retries in case of any database errors.",
    "8-2": "2",
    "9-0": "`setUser(String)`",
    "9-1": "Sets checkpoint database user name. Note that authentication will be performed only if both, user and password are set.",
    "9-2": "No value",
    "10-0": "`setPassword(String)`",
    "10-1": "Sets checkpoint database password.",
    "10-2": "No value"
  },
  "cols": 3,
  "rows": 11
}
[/block]
##Apache DBCP
[Apache DBCP](http://commons.apache.org/proper/commons-dbcp/) project provides various wrappers for data sources and connection pools. You can use these wrappers as Spring beans to configure this SPI from Spring configuration file or code. Refer to [Apache DBCP](http://commons.apache.org/proper/commons-dbcp/) project for more information.
[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\" singleton=\"true\">\n  ...\n  <property name=\"checkpointSpi\">\n    <bean class=\"org.apache.ignite.spi.checkpoint.database.JdbcCheckpointSpi\">\n      <property name=\"dataSource\">\n        <ref bean=\"anyPoolledDataSourceBean\"/>\n      </property>\n      <property name=\"checkpointTableName\" value=\"CHECKPOINTS\"/>\n      <property name=\"user\" value=\"test\"/>\n      <property name=\"password\" value=\"test\"/>\n    </bean>\n  </property>\n  ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "JdbcCheckpointSpi checkpointSpi = new JdbcCheckpointSpi();\n \njavax.sql.DataSource ds = ... // Set datasource.\n \n// Set database checkpoint SPI parameters.\ncheckpointSpi.setDataSource(ds);\ncheckpointSpi.setUser(\"test\");\ncheckpointSpi.setPassword(\"test\");\n \nIgniteConfiguration cfg = new IgniteConfiguration();\n \n// Override default checkpoint SPI.\ncfg.setCheckpointSpi(checkpointSpi);\n \n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]

[block:api-header]
{
  "type": "basic",
  "title": "Amazon S3 Checkpoint Configuration"
}
[/block]
`S3CheckpointSpi` uses Amazon S3 storage to store checkpoints. For information about Amazon S3 visit [http://aws.amazon.com/](http://aws.amazon.com/).

The following configuration parameters can be used to configure `S3CheckpointSpi`:
[block:parameters]
{
  "data": {
    "h-0": "Setter Method",
    "h-1": "Description",
    "h-2": "Default",
    "0-0": "`setAwsCredentials(AWSCredentials)`",
    "0-1": "Sets AWS credentials to use for storing checkpoints.",
    "0-2": "No value (must be provided)",
    "1-0": "`setClientConfiguration(Client)`",
    "1-1": "Sets AWS client configuration.",
    "1-2": "No value",
    "2-0": "`setBucketNameSuffix(String)`",
    "2-1": "Sets bucket name suffix.",
    "2-2": "default-bucket"
  },
  "cols": 3,
  "rows": 3
}
[/block]

[block:code]
{
  "codes": [
    {
      "code": "<bean class=\"org.apache.ignite.configuration.IgniteConfiguration\" singleton=\"true\">\n  ...\n  <property name=\"checkpointSpi\">\n    <bean class=\"org.apache.ignite.spi.checkpoint.s3.S3CheckpointSpi\">\n      <property name=\"awsCredentials\">\n        <bean class=\"com.amazonaws.auth.BasicAWSCredentials\">\n          <constructor-arg value=\"YOUR_ACCESS_KEY_ID\" />\n          <constructor-arg value=\"YOUR_SECRET_ACCESS_KEY\" />\n        </bean>\n      </property>\n    </bean>\n  </property>\n  ...\n</bean>",
      "language": "xml"
    },
    {
      "code": "IgniteConfiguration cfg = new IgniteConfiguration();\n \nS3CheckpointSpi spi = new S3CheckpointSpi();\n \nAWSCredentials cred = new BasicAWSCredentials(YOUR_ACCESS_KEY_ID, YOUR_SECRET_ACCESS_KEY);\n \nspi.setAwsCredentials(cred);\n \nspi.setBucketNameSuffix(\"checkpoints\");\n \n// Override default checkpoint SPI.\ncfg.setCheckpointSpi(cpSpi);\n \n// Start Ignite node.\nIgnition.start(cfg);",
      "language": "java"
    }
  ]
}
[/block]