package org.apache.ignite.internal.processors.query.calcite.integration;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.IntStream;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.junit.Test;

/**  */
public class RowNumColumnIntegrationTest extends AbstractBasicIntegrationTest {
    /** {@inheritDoc} */
    @Override protected int nodeCount() {
        return 1;
    }

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        SqlConfiguration sqlCfg = new SqlConfiguration()
                .setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration().setDefault(true));

        return super.getConfiguration(igniteInstanceName)
                .setSqlConfiguration(sqlCfg)
                .setCacheConfiguration(new CacheConfiguration<>(DEFAULT_CACHE_NAME));
    }

    /**  */
    @Test
    public void test() {
        sql("create table PERSON(id int primary key, name varchar)");

        List<List<?>> exp = new ArrayList<>();

        for (int i = 1; i <= 10; i++) {
            List<?> row = List.of(i, "Foo" + i);

            sql("insert into PERSON(id, name) values(?, ?)", row.toArray(Object[]::new));

            exp.add(row);
        }

        assertEquals(
                addRowNums(exp, 1),
                sql("select id, name, rownum from PERSON order by id")
        );
    }

    private static List<?> addRowNums(List<List<?>> rows, int rowNumStartInclusive) {
        return IntStream.range(0, rows.size())
                .mapToObj(i -> addRowNum(rows.get(i), i + rowNumStartInclusive))
                .collect(toList());
    }

    private static List<?> addRowNum(List<?> row, int rowNum) {
        List<Object> res = new ArrayList<>(row.size() + 1);

        res.addAll(row);
        res.add(rowNum);

        return Collections.unmodifiableList(res);
    }
}
