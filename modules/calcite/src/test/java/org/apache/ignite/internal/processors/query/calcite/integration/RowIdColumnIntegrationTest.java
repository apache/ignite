package org.apache.ignite.internal.processors.query.calcite.integration;

import java.util.List;
import org.apache.ignite.calcite.CalciteQueryEngineConfiguration;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.configuration.SqlConfiguration;
import org.junit.Test;

/**  */
// TODO: IGNITE-xxxxx-rowid-poc Документация и прочая красота везде
// TODO: IGNITE-xxxxx-rowid-poc Нужно будет сдлеть больше тестов тут или рядом:
// TODO: IGNITE-xxxxx-rowid-poc - pk - составной ключ (несколько колонок)
// TODO: IGNITE-xxxxx-rowid-poc - запрос с keppBinary = {false, true}
// TODO: IGNITE-xxxxx-rowid-poc - будет видна в фукциях в том числе табличных
// TODO: IGNITE-xxxxx-rowid-poc - всякие dml с указанием колонки в условии и прочее
// TODO: IGNITE-xxxxx-rowid-poc - может проветить еще с as t что-то вроде select t.rowid from PERSON as t
// TODO: IGNITE-xxxxx-rowid-poc - jdbc что будет работать все выше сценарии
public class RowIdColumnIntegrationTest extends AbstractBasicIntegrationTest {
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

    /** */
    @Test
    public void test() {
        //String column = "_key";
        String column = "rowid";

        sql("create table PERSON(id int primary key, name varchar)");

        sql("insert into PERSON(id, name) values(?, ?)", 1, "Foo");
        sql("insert into PERSON(id, name) values(?, ?)", 2, "Bar");
        sql("insert into PERSON(id, name) values(?, ?)", 3, "Cat");
        sql("insert into PERSON(id, name) values(?, ?)", 4, "Cat");
        sql("insert into PERSON(id, name) values(?, ?)", 5, "Cat");
        sql("insert into PERSON(id, name) values(?, ?)", 6, "Cat");
        sql("insert into PERSON(id, name) values(?, ?)", 7, "Cat");
        sql("insert into PERSON(id, name) values(?, ?)", 8, "Cat");
        sql("insert into PERSON(id, name) values(?, ?)", 9, "Cat");
        sql("insert into PERSON(id, name) values(?, ?)", 10, "Cat");

        List<List<?>> selectRes = sql(String.format("select %s, id, name from PERSON order by id", column));

        log.info(">>>>> Select result: " + selectRes);

        for (List<?> row : selectRes) {
            List<List<?>> act = sql(String.format("select id, name from PERSON where %s = ?", column), row.get(0));

            log.info(">>>>> act: " + act);

            assertEquals(List.of(List.of(row.get(1), row.get(2))), act);
        }
    }
}
