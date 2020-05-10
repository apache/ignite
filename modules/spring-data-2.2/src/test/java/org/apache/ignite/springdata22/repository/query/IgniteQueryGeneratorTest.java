package org.apache.ignite.springdata22.repository.query;

import org.junit.Test;
import org.springframework.data.domain.Sort;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class IgniteQueryGeneratorTest {
    @Test
    public void testAddSortingNullsFirst() {
        assertThat(
                IgniteQueryGenerator.addSorting(
                        new StringBuilder("SELECT * FROM someTable"),
                        Sort.by(new Sort.Order(Sort.Direction.ASC, "someColumn").nullsFirst())
                ).toString(),
                is("SELECT * FROM someTable ORDER BY someColumn ASC NULLS FIRST")
        );
    }

    @Test
    public void testAddSortingNullsLast() {
        assertThat(
                IgniteQueryGenerator.addSorting(
                        new StringBuilder("SELECT * FROM someTable"),
                        Sort.by(new Sort.Order(Sort.Direction.ASC, "someColumn").nullsLast())
                ).toString(),
                is("SELECT * FROM someTable ORDER BY someColumn ASC NULLS LAST")
        );
    }
}
