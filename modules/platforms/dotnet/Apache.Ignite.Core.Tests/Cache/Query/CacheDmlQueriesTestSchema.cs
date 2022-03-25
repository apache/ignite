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

namespace Apache.Ignite.Core.Tests.Cache.Query
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Cache.Query;
    using Apache.Ignite.Core.Common;
    using NUnit.Framework;

    /// <summary>
    /// Tests Data Manipulation Language queries related to schema.
    /// </summary>
    public class CacheDmlQueriesTestSchema
    {
        /// Table name
        private const string TableName = "T1";

        private const string SchemaName1 = "SCHEMA_1";
        private const string SchemaName2 = "SCHEMA_2";
        private const string SchemaName3 = "ScHeMa3";
        private const string SchemaName4 = "SCHEMA_4";

        private const string QSchemaName4 = "\"" + SchemaName3 + "\"";

        /// <summary>
        /// Sets up test fixture.
        /// </summary>
        [TestFixtureSetUp]
        public void FixtureSetUp()
        {
            var cfg = new IgniteConfiguration(TestUtils.GetTestConfiguration())
            {
                SqlSchemas = new List<string>
                {
                    SchemaName1,
                    SchemaName2,
                    QSchemaName4,
                    SchemaName4
                }
            };

            Ignition.Start(cfg);
        }

        /// <summary>
        /// Tears down test fixture.
        /// </summary>
        [TestFixtureTearDown]
        public void FixtureTearDown()
        {
            Ignition.StopAll(true);
        }

        /// <summary>
        /// Schema explicitly defined.
        /// </summary>
        [Test]
        public void TestBasicOpsExplicitPublicSchema()
        {
            ExecuteStmtsAndVerify(() => true);
        }

        /// <summary>
        /// Schema is imlicit.
        /// </summary>
        [Test]
        public void TestBasicOpsImplicitPublicSchema()
        {
            ExecuteStmtsAndVerify(() => false);
        }

        /// <summary>
        /// Schema is mixed.
        /// </summary>
        [Test]
        public void TestBasicOpsMixedPublicSchema()
        {
            int i = 0;

            ExecuteStmtsAndVerify(() => ((++i & 1) == 0));
        }

        /// <summary>
        /// Create and drop non-existing schema.
        /// </summary>
        [Test]
        public void TestCreateDropNonExistingSchema()
        {
            Assert.Throws<IgniteException>(() =>
                Sql("CREATE TABLE UNKNOWN_SCHEMA." + TableName + "(id INT PRIMARY KEY, val INT)"));

            Assert.Throws<IgniteException>(() => Sql("DROP TABLE UNKNOWN_SCHEMA." + TableName));
        }

        /// <summary>
        /// Create tables in different schemas for same cache.
        /// </summary>
        [Test]
        public void TestCreateTblsInDiffSchemasForSameCache()
        {
            const string testCache = "cache1";

            Sql("CREATE TABLE " + SchemaName1 + '.' + TableName
                + " (s1_key INT PRIMARY KEY, s1_val INT) WITH \"cache_name=" + testCache + "\"");

            Assert.Throws<IgniteException>(
                () => Sql("CREATE TABLE " + SchemaName2 + '.' + TableName
                          + " (s1_key INT PRIMARY KEY, s2_val INT) WITH \"cache_name=" + testCache + "\"")
            );

            Sql("DROP TABLE " + SchemaName1 + '.' + TableName);
        }

        /// <summary>
        /// Basic test with different schemas.
        /// </summary>
        [Test]
        public void TestBasicOpsDiffSchemas()
        {
            Sql("CREATE TABLE " + SchemaName1 + '.' + TableName + " (s1_key INT PRIMARY KEY, s1_val INT)");
            Sql("CREATE TABLE " + SchemaName2 + '.' + TableName + " (s2_key INT PRIMARY KEY, s2_val INT)");
            Sql("CREATE TABLE " + QSchemaName4 + '.' + TableName + " (s3_key INT PRIMARY KEY, s3_val INT)");
            Sql("CREATE TABLE " + SchemaName4 + '.' + TableName + " (s4_key INT PRIMARY KEY, s4_val INT)");

            Sql("INSERT INTO " + SchemaName1 + '.' + TableName + " (s1_key, s1_val) VALUES (1, 2)");
            Sql("INSERT INTO " + SchemaName2 + '.' + TableName + " (s2_key, s2_val) VALUES (1, 2)");
            Sql("INSERT INTO " + QSchemaName4 + '.' + TableName + " (s3_key, s3_val) VALUES (1, 2)");
            Sql("INSERT INTO " + SchemaName4 + '.' + TableName + " (s4_key, s4_val) VALUES (1, 2)");

            Sql("UPDATE " + SchemaName1 + '.' + TableName + " SET s1_val = 5");
            Sql("UPDATE " + SchemaName2 + '.' + TableName + " SET s2_val = 5");
            Sql("UPDATE " + QSchemaName4 + '.' + TableName + " SET s3_val = 5");
            Sql("UPDATE " + SchemaName4 + '.' + TableName + " SET s4_val = 5");

            Sql("DELETE FROM " + SchemaName1 + '.' + TableName);
            Sql("DELETE FROM " + SchemaName2 + '.' + TableName);
            Sql("DELETE FROM " + QSchemaName4 + '.' + TableName);
            Sql("DELETE FROM " + SchemaName4 + '.' + TableName);

            Sql("CREATE INDEX t1_idx_1 ON " + SchemaName1 + '.' + TableName + "(s1_val)");
            Sql("CREATE INDEX t1_idx_1 ON " + SchemaName2 + '.' + TableName + "(s2_val)");
            Sql("CREATE INDEX t1_idx_1 ON " + QSchemaName4 + '.' + TableName + "(s3_val)");
            Sql("CREATE INDEX t1_idx_1 ON " + SchemaName4 + '.' + TableName + "(s4_val)");

            Sql("SELECT * FROM " + SchemaName1 + '.' + TableName);
            Sql("SELECT * FROM " + SchemaName2 + '.' + TableName);
            Sql("SELECT * FROM " + QSchemaName4 + '.' + TableName);
            Sql("SELECT * FROM " + SchemaName4 + '.' + TableName);

            Sql("SELECT * FROM " + SchemaName1 + '.' + TableName
                + " JOIN " + SchemaName2 + '.' + TableName
                + " JOIN " + QSchemaName4 + '.' + TableName
                + " JOIN " + SchemaName4 + '.' + TableName);

            VerifyTables();

            Sql("DROP TABLE " + SchemaName1 + '.' + TableName);
            Sql("DROP TABLE " + SchemaName2 + '.' + TableName);
            Sql("DROP TABLE " + QSchemaName4 + '.' + TableName);
            Sql("DROP TABLE " + SchemaName4 + '.' + TableName);
        }

        /// <summary>
        /// Verify tables.
        /// </summary>
        private static void VerifyTables()
        {
            Sql("SELECT SCHEMA_NAME, KEY_ALIAS FROM SYS.TABLES ORDER BY SCHEMA_NAME", res =>
            {
                Assert.AreEqual(new List<List<object>>
                {
                    new List<object> {SchemaName1, "S1_KEY"},
                    new List<object> {SchemaName2, "S2_KEY"},
                    new List<object> {SchemaName4, "S4_KEY"},
                    new List<object> {SchemaName3, "S3_KEY"}
                }, res);
            });
        }

        /// <summary>
        /// Get test table name.
        /// </summary>
        private static string GetTableName(bool withSchema)
        {
            return withSchema ? "PUBLIC." + TableName : TableName;
        }

        /// <summary>
        /// Perform SQL query.
        /// </summary>
        private static void Sql(string qry, Action<IList<IList<object>>> validator = null)
        {
            var res = Ignition
                .GetIgnite()
                .GetOrCreateCache<int, int>("TestCache")
                .Query(new SqlFieldsQuery(qry))
                .GetAll();

            if (validator != null)
                validator.Invoke(res);
        }

        /// <summary>
        /// Generate one-row result set.
        /// </summary>
        private static List<List<object>> OneRowList(params object[] vals)
        {
            return new List<List<object>> {vals.ToList()};
        }

        /// <summary>
        /// Create/insert/update/delete/drop table in PUBLIC schema.
        /// </summary>
        private static void ExecuteStmtsAndVerify(Func<bool> withSchemaDecisionSup)
        {
            Sql("CREATE TABLE " + GetTableName(withSchemaDecisionSup()) + " (id INT PRIMARY KEY, val INT)");

            Sql("CREATE INDEX t1_idx_1 ON " + GetTableName(withSchemaDecisionSup()) + "(val)");

            Sql("INSERT INTO " + GetTableName(withSchemaDecisionSup()) + " (id, val) VALUES(1, 2)");
            Sql("SELECT * FROM " + GetTableName(withSchemaDecisionSup()),
                res => Assert.AreEqual(OneRowList(1, 2), res));

            Sql("UPDATE " + GetTableName(withSchemaDecisionSup()) + " SET val = 5");
            Sql("SELECT * FROM " + GetTableName(withSchemaDecisionSup()),
                res => Assert.AreEqual(OneRowList(1, 5), res));

            Sql("DELETE FROM " + GetTableName(withSchemaDecisionSup()) + " WHERE id = 1");
            Sql("SELECT COUNT(*) FROM " + GetTableName(withSchemaDecisionSup()),
                res => Assert.AreEqual(OneRowList(0), res));

            Sql("SELECT COUNT(*) FROM SYS.TABLES WHERE schema_name = 'PUBLIC' " +
                "AND table_name = \'" + TableName + "\'", res => Assert.AreEqual(OneRowList(1), res));

            Sql("DROP TABLE " + GetTableName(withSchemaDecisionSup()));
        }
    }
}