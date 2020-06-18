package de.bwaldvogel.mongo.backend.postgresql;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

import org.junit.Test;

import de.bwaldvogel.mongo.backend.TestUtils;
import de.bwaldvogel.mongo.bson.Document;

public class PostgresqlCollectionTest {

    @Test
    public void testConvertOrderByToSql() throws Exception {
        assertThat(PostgresqlCollection.convertOrderByToSql(json(""))).isEqualTo("");

        assertThat(PostgresqlCollection.convertOrderByToSql(json("key: 1")))
            .isEqualTo("ORDER BY CASE WHEN (data ->> 'key')::numeric = -0.0 THEN '0' ELSE data ->> 'key' END ASC NULLS FIRST");

        assertThat(PostgresqlCollection.convertOrderByToSql(json("key1: 1, key2: -1")))
            .isEqualTo("ORDER BY" +
                " CASE WHEN (data ->> 'key1')::numeric = -0.0 THEN '0' ELSE data ->> 'key1' END ASC NULLS FIRST," +
                " CASE WHEN (data ->> 'key2')::numeric = -0.0 THEN '0' ELSE data ->> 'key2' END DESC NULLS LAST");

        assertThat(PostgresqlCollection.convertOrderByToSql(json("$natural: 1")))
            .isEqualTo("ORDER BY id ASC NULLS FIRST");

        assertThat(PostgresqlCollection.convertOrderByToSql(json("$natural: -1")))
            .isEqualTo("ORDER BY id DESC NULLS LAST");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> PostgresqlCollection.convertOrderByToSql(json("foo: 'bar'")))
            .withMessage("Illegal sort value: bar");

        assertThatExceptionOfType(IllegalArgumentException.class)
            .isThrownBy(() -> PostgresqlCollection.convertOrderByToSql(json("$foo: 1")))
            .withMessage("Illegal key: $foo");
    }

    private static Document json(String json) {
        return new Document(TestUtils.json(json));
    }

}