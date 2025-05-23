package org.apache.ignite.internal.processors.query.calcite.integration;

import java.math.BigDecimal;
import org.junit.Test;

/**
 * Integration test for WINDOW operator.
 */
public class WindowIntegrationTest extends AbstractBasicIntegrationTransactionalTest {
    /** {@inheritDoc} */
    @Override protected void init() throws Exception {
        super.init();

        sql("CREATE TABLE empsalary(depname VARCHAR, empno BIGINT, salary INT, enroll_date DATE) WITH " + atomicity());
        sql("INSERT INTO empsalary VALUES " +
            "('develop', 10, 5300, '2007-08-01'), " +
            "('sales', 1, 5000, '2006-10-01'), " +
            "('person', 5, 3500, '2007-12-10'), " +
            "('sales', 4, 4900, '2007-08-08'), " +
            "('person', 2, 3900, '2006-12-23'), " +
            "('develop', 7, 4200, '2008-01-01'), " +
            "('develop', 9, 4500, '2008-01-01'), " +
            "('sales', 3, 4800, '2007-08-01'), " +
            "('develop', 8, 6000, '2006-10-01'), " +
            "('develop', 11, 5200, '2007-08-15');");
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() {
        // NO-OP
    }

    /**  */
    @Test
    public void testRowNumber() {
        assertQuery("SELECT empno, ROW_NUMBER() OVER (ORDER BY salary) FROM empsalary")
            .returns(1L, 7L)
            .returns(2L, 2L)
            .returns(3L, 5L)
            .returns(4L, 6L)
            .returns(5L, 1L)
            .returns(7L, 3L)
            .returns(8L, 10L)
            .returns(9L, 4L)
            .returns(10L, 9L)
            .returns(11L, 8L)
            .check();
    }

    /**  */
    @Test
    public void testCountAllRange() {
        assertQuery("SELECT depname, COUNT(1) OVER (ORDER BY depname RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM empsalary")
            .returns("develop", 10L)
            .returns("develop", 10L)
            .returns("develop", 10L)
            .returns("develop", 10L)
            .returns("develop", 10L)
            .returns("person", 10L)
            .returns("person", 10L)
            .returns("sales", 10L)
            .returns("sales", 10L)
            .returns("sales", 10L)
            .check();
    }

    /**  */
    @Test
    public void testCountRangePartitioned() {
        assertQuery("SELECT depname, COUNT(1) OVER (PARTITION BY depname) FROM empsalary")
            .returns("develop", 5L)
            .returns("develop", 5L)
            .returns("develop", 5L)
            .returns("develop", 5L)
            .returns("develop", 5L)
            .returns("person", 2L)
            .returns("person", 2L)
            .returns("sales", 3L)
            .returns("sales", 3L)
            .returns("sales", 3L)
            .check();
    }

    /**  */
    @Test
    public void testCountRangePartitionedWithIntBounds() {
        assertQuery("SELECT depname, COUNT(1) OVER (PARTITION BY depname ORDER BY empno RANGE BETWEEN 2 PRECEDING AND 1 FOLLOWING) FROM empsalary")
            .returns("develop", 2L)
            .returns("develop", 3L)
            .returns("develop", 3L)
            .returns("develop", 4L)
            .returns("develop", 4L)
            .returns("person", 1L)
            .returns("person", 1L)
            .returns("sales", 1L)
            .returns("sales", 2L)
            .returns("sales", 3L)
            .check();
    }

    /**  */
    @Test
    public void testCountRangePartitionedWithDateBounds() {
        assertQuery("SELECT depname, COUNT(1) OVER (PARTITION BY depname ORDER BY enroll_date RANGE BETWEEN INTERVAL 730 DAYS PRECEDING AND INTERVAL 360 DAYS FOLLOWING) FROM empsalary")
            .returns("develop", 3L)
            .returns("develop", 5L)
            .returns("develop", 5L)
            .returns("develop", 5L)
            .returns("develop", 5L)
            .returns("person", 2L)
            .returns("person", 2L)
            .returns("sales", 3L)
            .returns("sales", 3L)
            .returns("sales", 3L)
            .check();
    }

    /**  */
    @Test
    public void testCountRowsPartitioned() {
        // todo: there is a bug in calcite parser - it ignores frame spec (ROWS...) in case order by is missing in query
        assertQuery("SELECT depname, COUNT(1) OVER (PARTITION BY depname ORDER BY empno ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM empsalary")
            .returns("develop", 1L)
            .returns("develop", 2L)
            .returns("develop", 3L)
            .returns("develop", 4L)
            .returns("develop", 5L)
            .returns("person", 1L)
            .returns("person", 2L)
            .returns("sales", 1L)
            .returns("sales", 2L)
            .returns("sales", 3L)
            .check();
    }

    /**  */
    @Test
    public void testCountRowsPartitionedWithBounds() {
        assertQuery("SELECT depname, COUNT(1) OVER (PARTITION BY depname ORDER BY empno ROWS BETWEEN 2 PRECEDING AND 1 FOLLOWING) FROM empsalary")
            .returns("develop", 2L)
            .returns("develop", 3L)
            .returns("develop", 4L)
            .returns("develop", 4L)
            .returns("develop", 3L)
            .returns("person", 2L)
            .returns("person", 2L)
            .returns("sales", 2L)
            .returns("sales", 3L)
            .returns("sales", 3L)
            .check();
    }

    /**  */
    @Test
    public void testCorrelation() {
        assertQuery("SELECT depname, (SELECT ROW_NUMBER() OVER (ORDER BY depname)) FROM empsalary")
            .returns("develop", 1L)
            .returns("develop", 1L)
            .returns("develop", 1L)
            .returns("develop", 1L)
            .returns("develop", 1L)
            .returns("person", 1L)
            .returns("person", 1L)
            .returns("sales", 1L)
            .returns("sales", 1L)
            .returns("sales", 1L)
            .check();
    }

    /**  */
    @Test
    public void testAggregateWithWindowFunction() {
        assertQuery("SELECT depname, COUNT(*), SUM(SUM(salary)) OVER (PARTITION BY depname) FROM empsalary GROUP BY depname")
            .returns("develop", 5L, BigDecimal.valueOf(25200))
            .returns("person", 2L, BigDecimal.valueOf(7400))
            .returns("sales", 3L, BigDecimal.valueOf(14700))
            .check();
    }
}
