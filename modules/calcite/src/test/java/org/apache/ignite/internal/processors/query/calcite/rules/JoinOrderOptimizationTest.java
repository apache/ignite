/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.query.calcite.rules;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import org.apache.calcite.rel.rules.JoinToMultiJoinRule;
import org.apache.ignite.internal.processors.query.calcite.QueryChecker;
import org.apache.ignite.internal.processors.query.calcite.RuleApplyListener;
import org.apache.ignite.internal.processors.query.calcite.integration.AbstractBasicIntegrationTest;
import org.apache.ignite.internal.processors.query.calcite.rule.logical.IgniteMultiJoinOptimizeRule;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.apache.ignite.internal.processors.query.calcite.hint.HintDefinition.ENFORCE_JOIN_ORDER;

/**
 * Test of joins order heuristic optimization.
 *
 * @see JoinToMultiJoinRule
 * @see IgniteMultiJoinOptimizeRule
 */
@RunWith(Parameterized.class)
public class JoinOrderOptimizationTest extends AbstractBasicIntegrationTest {
    /** Test query. */
    @Parameterized.Parameter
    public String qry;

    /** Test queries. */
    @Parameterized.Parameters
    public static Collection<String> runConfig() {
        return testQueries();
    }

    /** {@inheritDoc} */
    @Override protected void beforeTestsStarted() throws Exception {
        super.beforeTestsStarted();

        initSchema();

        gatherStatistics();
    }

    /** {@inheritDoc} */
    @Override protected boolean destroyCachesAfterTest() {
        return false;
    }

    /** */
    private void initSchema() {
        int warehsSz = 50;
        int catgSz = 100;
        int reviewSz = 50000;
        int prodSz = 100;
        int discSz = 2000;
        int shipSz = 15000;
        int usrSz = 10000;
        int ordSz = 20000;
        int ordDetSz = 100000;

        sql("CREATE TABLE Products (ProdId INT PRIMARY KEY, ProdNm VARCHAR(100), Price DECIMAL(10, 2)) WITH \"VALUE_TYPE='PROD'\"");
        sql("INSERT INTO Products SELECT x, 'Product_' || x::VARCHAR, 100.0 + x % 100.0 FROM system_range(1, ?)",
            prodSz);

        sql("CREATE TABLE ProductCategories (ProdCatgId INT PRIMARY KEY, ProdId INT, CatgId INT)");
        sql("INSERT INTO ProductCategories SELECT x, 1 + RAND_INTEGER(?), 1 + RAND_INTEGER(?) FROM system_range(1, ?)",
            prodSz - 1, catgSz - 1, prodSz);

        sql("CREATE TABLE Discounts (DiscId INT PRIMARY KEY, ProdId INT, DiscPercent DECIMAL(5, 2), ValidUntil DATE)");
        sql("INSERT INTO Discounts SELECT x, 1 + RAND_INTEGER(?), 1 + x % 15, date '2025-02-10' + (x % 365)::INTERVAL DAYS " +
            "FROM system_range(1, ?)", prodSz - 1, discSz);

        sql("CREATE TABLE Shipping (ShippId INT PRIMARY KEY, OrdId INT, ShippDate DATE, ShippAddrs VARCHAR(255))");
        sql("INSERT INTO Shipping SELECT x, 1 + RAND_INTEGER(?), date '2020-01-01' + RAND_INTEGER(365)::INTERVAL DAYS, "
            + " 'Addrs_' || x::VARCHAR FROM system_range(1, ?)", ordSz - 1, shipSz);

        sql("CREATE TABLE Users (UsrId INT PRIMARY KEY, UsrNm VARCHAR(100), Email VARCHAR(100))  WITH \"VALUE_TYPE='USR'\"");
        sql("INSERT INTO Users SELECT x, 'User_' || x::VARCHAR, 'email_' || x::VARCHAR || '@nowhere.xyz' FROM system_range(1, ?)",
            usrSz);

        sql("CREATE TABLE Orders (OrdId INT PRIMARY KEY, UsrId INT, ProdId INT, OrdDate DATE, TotalAmount DECIMAL(10, 2)) " +
            "WITH \"VALUE_TYPE='ORD'\"");
        sql("INSERT INTO Orders SELECT x, 1 + RAND_INTEGER(?), 1 + RAND_INTEGER(?), date '2025-02-10' + (x % 365)::INTERVAL DAYS, " +
            "1 + x % 10 FROM system_range(1, ?)", usrSz - 1, prodSz, ordSz);

        sql("CREATE TABLE OrderDetails (OrdDetId INT PRIMARY KEY, OrdId INT, ProdId INT, Qnty INT)  WITH \"VALUE_TYPE='ORD_DET'\"");
        sql("INSERT INTO OrderDetails SELECT x, 1 + RAND_INTEGER(?), 1 + RAND_INTEGER(?), 1 + x % 10 FROM system_range(1, ?)",
            ordSz - 1, prodSz - 1, ordDetSz);

        sql("CREATE TABLE Warehouses (WrhId INT PRIMARY KEY, WrhNm VARCHAR(100), LocNm VARCHAR(100))");
        sql("INSERT INTO Warehouses SELECT x, 'Wrh_' || x::VARCHAR, 'Location_' || x::VARCHAR FROM system_range(1, ?)", warehsSz);

        sql("CREATE TABLE Categories (CatgId INT PRIMARY KEY, CatgName VARCHAR(100))");
        sql("INSERT INTO Categories SELECT x, 'Category_' || x::VARCHAR FROM system_range(1, ?)", catgSz);

        sql("CREATE TABLE Reviews (RevId INT PRIMARY KEY, ProdId INT, usrId INT, RevTxt VARCHAR, Rating INT)");
        sql("INSERT INTO Reviews SELECT x, 1 + RAND_INTEGER(?), 1 + RAND_INTEGER(?), 'Prod. review ' || x::VARCHAR, 1 + RAND_INTEGER(4) " +
            "FROM system_range(1, ?)", prodSz - 1, usrSz - 1, reviewSz);
    }

    /** Tests that query result doesn't change with the joins order optimization. */
    @Test
    public void testTheSameResults() {
        assert !qry.contains(ENFORCE_JOIN_ORDER.name());
        assert qry.startsWith("SELECT ");

        String qryFixedJoins = qry.replaceAll("SELECT", "SELECT /*+ " + ENFORCE_JOIN_ORDER + " */");

        RuleApplyListener planLsnr = new RuleApplyListener(IgniteMultiJoinOptimizeRule.INSTANCE);

        // First, call with fixed join order.
        List<List<?>> expectedResult = new ArrayList<>();

        assertQuery(qryFixedJoins).withPlannerListener(planLsnr).withResultChecker(expectedResult::addAll).check();

        // Ensure that the optimization rule wasn't fired.
        assertFalse(planLsnr.ruleSucceeded());

        assertFalse(expectedResult.isEmpty());

        QueryChecker checker = assertQuery(qry).withPlannerListener(planLsnr);

        // Make sure that the optimized query has the same results.
        expectedResult.forEach(row -> checker.returns(row.toArray()));

        checker.check();

        // Ensure that the optimization rule has worked.
        assertTrue(planLsnr.ruleSucceeded());
    }

    /** */
    private static Collection<String> testQueries() {
        return F.asList(
            // User orders with products in multiple categories.
            "SELECT \n"
                + "    U.UsrNm, O.OrdId, COUNT(DISTINCT C.CatgName) AS Categories\n"
                + " FROM Users U, Orders O, OrderDetails OD, Products P, ProductCategories PC, Categories C\n"
                + " WHERE U.UsrId = O.UsrId\n"
                + "  AND O.OrdId = OD.OrdId\n"
                + "  AND OD.ProdId = P.ProdId\n"
                + "  AND P.ProdId = PC.ProdId\n"
                + "  AND PC.CatgId = C.CatgId\n"
                + " GROUP BY U.UsrNm, O.OrdId",

            // Orders with total revenue and shipping details.
            "SELECT \n"
                + "    O.OrdId, O.OrdDate, S.ShippAddrs, SUM(OD.Qnty * P.Price) AS TotalOrderValue\n"
                + " FROM Orders O, Shipping S, OrderDetails OD, Products P\n"
                + "WHERE O.OrdId = S.OrdId\n"
                + "  AND O.OrdId = OD.OrdId\n"
                + "  AND OD.ProdId = P.ProdId\n"
                + "GROUP BY O.OrdId, O.OrdDate, S.ShippAddrs",

            // Products in warehouses by category.
            "SELECT \n"
                + "    W.WrhNm, C.CatgName, P.ProdNm\n"
                + " FROM Warehouses W, Products P, ProductCategories PC, Categories C\n"
                + "WHERE W.WrhId = (P.ProdId % 5 + 1)\n"
                + "  AND P.ProdId = PC.ProdId\n"
                + "  AND PC.CatgId = C.CatgId",

            // Average product rating by category.
            "SELECT \n"
                + "    C.CatgName, P.ProdNm, AVG(R.Rating) AS AvgRating\n"
                + " FROM Categories C, ProductCategories PC, Products P, Reviews R\n"
                + "WHERE C.CatgId = PC.CatgId\n"
                + "  AND PC.ProdId = P.ProdId\n"
                + "  AND P.ProdId = R.ProdId\n"
                + "GROUP BY C.CatgName, P.ProdNm",

            // Products ordered by user.
            "SELECT \n"
                + "    U.UsrNm, P.ProdNm, SUM(OD.Qnty) AS TotalQnty\n"
                + " FROM Users U, Orders O, OrderDetails OD, Products P\n"
                + "WHERE U.UsrId = O.UsrId\n"
                + "  AND O.OrdId = OD.OrdId\n"
                + "  AND OD.ProdId = P.ProdId\n"
                + "GROUP BY U.UsrNm, P.ProdNm",

            // Joining of all the tables.
            "SELECT \n"
                + "    U.UsrId, U.UsrNm, O.OrdId, O.OrdDate, P.ProdNm, OD.Qnty, \n"
                + "    C.CatgName, S.ShippAddrs, R.Rating, D.DiscPercent, W.WrhNm\n"
                + " FROM Users U, Orders O, OrderDetails OD, Products P, ProductCategories PC, Categories C, \n"
                + "    Shipping S, Reviews R, Discounts D, Warehouses W\n"
                + "WHERE U.UsrId = O.UsrId\n"
                + "  AND O.OrdId = OD.OrdId\n"
                + "  AND OD.ProdId = P.ProdId\n"
                + "  AND P.ProdId = PC.ProdId\n"
                + "  AND PC.CatgId = C.CatgId\n"
                + "  AND O.OrdId = S.OrdId\n"
                + "  AND P.ProdId = R.ProdId"
                + "  AND U.UsrId = R.UsrId\n"
                + "  AND P.ProdId = D.ProdId\n"
                + "  AND W.WrhId = (P.ProdId % 5 + 1)",

            // Correlated query.
            "SELECT OD.OrdDetId, O.OrdId, P.ProdId, U.UsrNm, " +
                "(SELECT MAX(C.ShippDate) from Shipping C where C.OrdId=OD.OrdId) as corr " +
                "from OrderDetails OD " +
                "JOIN Orders O on O.OrdId = OD.OrdId " +
                "JOIN Products P on P.ProdId = OD.ProdId " +
                "JOIN Users U on O.UsrId = U.UsrId",

            // Yet another correlated query.
            "SELECT S.ShippDate, O.OrdId, P.ProdId, U.UsrNm, " +
                "(SELECT MAX(C.Qnty) from OrderDetails C where C.OrdId=O.OrdId and C.ProdId=O.ProdId) as corr, " +
                "(SELECT AVG(C2.TotalAmount) from Orders C2 where C2.ProdId=O.ProdId) as corr2 " +
                "from Shipping S " +
                "JOIN Orders O on O.OrdId = S.OrdId " +
                "JOIN Products P on P.ProdId = O.ProdId " +
                "JOIN Users U on O.UsrId = U.UsrId"
        );
    }
}
