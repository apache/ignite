package org.apache.ignite.internal.processors.cache.index;

import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.QueryEntity;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.cache.query.annotations.QuerySqlField;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.util.typedef.F;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Objects;

public class ComplexTableIndexUsageTest extends AbstractIndexingCommonTest {
  /** {@inheritDoc} */
  @Override
  protected void beforeTestsStarted() throws Exception {
    super.beforeTestsStarted();
    startGrid(0);
  }

  /** {@inheritDoc} */
  @Override
  protected void afterTestsStopped() throws Exception {
    stopAllGrids();
    super.afterTestsStopped();
  }

  @Test
  public void testIdxQueryColumn() {
    String tblName = "PERSON";
    CacheConfiguration<Long, Person> ccfg = new CacheConfiguration<>(tblName);
    ccfg.setSqlSchema("PUBLIC");

    QueryEntity qryEntity = new QueryEntity(Long.class, Person.class);
    ccfg.setQueryEntities(F.asList(qryEntity));

    IgniteCache<Long, Person> cache = grid(0).createCache(ccfg);

    populateCache(cache);

    List<List<?>> res1 = executeSql("explain SELECT * FROM " + tblName + " WHERE name='Alexey'");
    assertUsingIndex(res1, "name");
    List<List<?>> res2 = executeSql("explain SELECT * FROM " + tblName + " WHERE name='Alexey' and age='31'");
    assertUsingIndex(res2, "name");
    List<List<?>> res3 = executeSql("explain SELECT * FROM " + tblName + " WHERE name='Alexey' or name='Dmitriy'");
    assertUsingIndex(res3, "name");
    List<List<?>> res4 = executeSql("explain SELECT * FROM " + tblName + " WHERE (name='Alexey' or name='Dmitriy') and age='31'");
    assertUsingIndex(res4, "name");

    List<List<?>> resData = executeSql("SELECT * FROM " + tblName + " WHERE (name='Alexey' or name='Dmitriy') and age='31'");
    Assert.assertEquals(resData.size(), 2);
  }

  private void populateCache(IgniteCache<Long, Person> cache) {
    long cnt = 0;
    cache.put(++cnt, new Person("Ivan", 20));
    cache.put(++cnt, new Person("Artem", 31));
    cache.put(++cnt, new Person("Alexey", 31));
    cache.put(++cnt, new Person("Alexey", 32));
    cache.put(++cnt, new Person("Igor", 33));
    cache.put(++cnt, new Person("Semen", 18));
    cache.put(++cnt, new Person("Dmitriy", 31));
    cache.put(++cnt, new Person("Dmitriy", 41));
  }

  /**
   * Check that explain plan result shown using PK index and don't use scan.
   *
   * @param results Result of execut explain plan query.
   * @param field Idx field.
   */
  private void assertUsingIndex(List<List<?>> results, String field) {
    String explainPlan = (String)results.get(0).get(0);
    assertTrue(explainPlan.contains("_" + Objects.requireNonNull(field).toUpperCase() + "_IDX"));
    assertFalse(explainPlan.contains("_SCAN_"));
  }

  /**
   * Run SQL statement on default node.
   *
   * @param stmt Statement to run.
   * @param args arguments of statements
   * @return Run result.
   */
  private List<List<?>> executeSql(String stmt, Object... args) {
    return executeSql(grid(0), stmt, args);
  }

  /**
   * Run SQL statement on specified node.
   *
   * @param node node to execute query.
   * @param stmt Statement to run.
   * @param args arguments of statements
   * @return Run result.
   */
  private List<List<?>> executeSql(IgniteEx node, String stmt, Object... args) {
    return node.context().query().querySqlFields(new SqlFieldsQuery(stmt).setArgs(args), true).getAll();
  }

  static class Person {
    /** Person name. */
    @QuerySqlField(index = true, inlineSize = 10)
    private String name;

    /** Person age */
    @QuerySqlField
    private int age;

    /**
     * Creates a person.
     *
     * @param name Name
     * @param age Age
     */
    public Person(String name, int age) {
      this.name = name;
      this.age = age;
    }

    /**
     * Returns name of the person.
     * @return The name of the person.
     */
    public String getName() {
      return name;
    }

    /**
     * Returns age of the person.
     * @return Person's age.
     */
    public int getAge() {
      return age;
    }
  }

}
