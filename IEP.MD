# IEP-123: Transactions aware queries

## Motivation:

Currently, Ignite provide several interfaces to query data:

  * Key-Value API.
  * SQL.
  * Scan query.
  * Index scan query.

Ignite, also, provides ACID transactions for Key-Value API.
But, for quering data we have known limitation:
All query interfaces doesn't respect changes made inside current transaction.
Simple test to illustrate current behaviour:

```java
public class TransactionVisibilityTest extends GridCommonAbstractTest {
    @Test
    public void testVisibility() throws Exception {
        IgniteEx srv = startGrid();

        IgniteCache<Integer, Integer> cache = srv.createCache(new CacheConfiguration<Integer, Integer>()
            .setName("TBL")
            .setAtomicityMode(CacheAtomicityMode.TRANSACTIONAL)
            .setQueryEntities(Collections.singleton(new QueryEntity()
                .setTableName("TBL")
                .setKeyType(Integer.class.getName())
                .setValueType(Integer.class.getName()))));

        try (Transaction tx = srv.transactions().txStart(PESSIMISTIC, READ_COMMITTED, 1_000, 10)) {
            cache.put(1, 2);

            assertEquals("Must see transaction related data", (Integer)2, cache.get(1));

            List<List<?>> sqlData = executeSql(srv, "SELECT COUNT(*) FROM TBL.TBL");
            List<Cache.Entry<Integer, Integer>> scanData = cache.query(new ScanQuery<Integer, Integer>()).getAll();

            // Fails here.
            assertEquals("Must see transaction related data", 1L, sqlData.get(0).get(0));
            assertEquals("Must see transaction related data", 1, scanData.size());

            tx.commit();
        }

        List<List<?>> sqlData = executeSql(srv, "SELECT COUNT(*) FROM TBL.TBL");
        List<Cache.Entry<Integer, Integer>> scanData = cache.query(new ScanQuery<Integer, Integer>()).getAll();

        assertEquals("Must see committed data", 1L, sqlData.get(0).get(0));
        assertEquals("Must see committed data", 1, scanData.size());
    }

    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        return super.getConfiguration(igniteInstanceName).setSqlConfiguration(
            new SqlConfiguration().setQueryEnginesConfiguration(new CalciteQueryEngineConfiguration()));
    }

    public static List<List<?>> executeSql(IgniteEx node, String sqlText, Object... args) {
        return node.cache("TBL").query(new SqlFieldsQuery(sqlText).setArgs(args)).getAll();
    }
}
```

Traditional databases like MySQL, PostgreSQL and others respects transactions boundaries while executing queries.
psql example:
```genericsql
postgres=# CREATE TABLE TBL(ID BIGINT PRIMARY KEY, VAL BIGINT);
CREATE TABLE
postgres=# START TRANSACTION;
START TRANSACTION
postgres=*# SELECT * FROM TBL;
 id | val
----+-----
(0 rows)
postgres=*# INSERT INTO TBL VALUES(1, 1);
INSERT 0 1
postgres=*# SELECT * FROM TBL;
 id | val
----+-----
  1 |   1
(1 row)

postgres=*# COMMIT;
COMMIT
postgres=# SELECT * FROM TBL;
 id | val
----+-----
  1 |   1
(1 row)
```

It very unexpected for Ignite users to deal with this kind of API inconsistency and lack of interoperability.

**The goal of IEP is to provide consistency and interoperability between Key-Value, SQL, Scan query, Index Query APIs.**

## Description:

Three query API has it's own advantages, use-cases and details, so description will be query API specific.
In separate section different isolation levels will be described.

### Description of transaction support for Key-Value API:
* Transaction state lives in JVM heap in `IgniteTxState`.
    * During, `get` or `put` invocation Ignite checks local state to get or store tx version of entry.
* Entries locks lives in JVM heap: `GridCacheMvcc`.
* On commit time Ignite writes changed entries to WAL and off-heap page-memory using ARIES-like protocol to preserve durability.
* Having only committed data in WAL and off-heap allows to exclude UNDO logic on failover.

SQL and other types of queries works on top of datastructures(BPlusTree) that stores data off-heap.
This lead to the current behavior when transaction and queries not overlap.

### Types of the solutions:

Described implementation and issue has two obvious solutions:

1. Queries must analyze and keep up-to-date current transaction datastructures during invocation.

2. Transaction flow must be modified to store changes off-heap. 
Queries must filter out other transaction changes and analyze only committed or self-changed entries.
Other possible implementation of this approach is MVCC instead of lock-based concurrency control.

First approach has well understood limitations - as locks and changes stored in JVM heap it leads to GC pressure
and limited scalability. Moreover, in some cases (SQL select) transaction state must be sent around cluster to achieve correct results.
Ignite primary use-cases are around OLTP workload which means we deal with small and fast transactions that change coupld of entries.
So, overhead will be small for most cases.

Second approach more scalable but requires huge and risky refactoring of literally all Ignite subsystems.

**In the first phase of implementation of this IEP it is proposed to go with first approach.**

This following of description part provides high-level details of proposed implementation:

### Scan
* updated and deleted entries must be modified, transformed, filtered before returning from iterator.
* new entries must be added to iterator.
  Looks like both can be done on the query runner side without observable performance issues.

### SQL

New SQL Calcite engine gives Ignite the power to control execution flow.
Proposed design of execution flow for different types of queries:

#### INSERT, UPDATE, DELETE, MERGE

Currently, executed on transaction coordinator node.
But, separate thread used to permorm plan with `ModifyNode`.
This means we must do the following:

1. Suspend transaction.
2. Store transaction inside `BaseQueryContext`.
3. Plan and start execute query.
4. Wrap `ModifyNode` in transaction resume/suspend.
5. Resume transaction after query invocation.

#### SELECT 

To invoke select statements correctly execution engine must know `IgniteTxState` in time of table(index) scan.
Ignite must send `IgniteTxState` (possibly filtering out some entries) to the node that invokes query step.
This will allow to SQL engine to mixup transaction state and table(index) data to achieve correct results.

### Transaction isolation levels

#### READ_COMMITTED

No additional locks required. 
Currently, queries works on this isolation level but without knowledge of transaction changes.

#### REPEATABLE_READ

Ignite must acquire shared-lock on each entry that conforms query criteria.
This can be achieved in two ways: clause based locks, entry by entry locks.
Decision must be made additionally in time of implementaion.

#### SERIALIZABLE

Ignite must acquire exclusive-lock on each entry that conforms query criteria.
This can be achieved by clause based locks.

#### Lock performance

With SQL and Scan queries on strict isolation level it very simple for the end-user to acquire a lot of locks during "simple" query.
Mechanism like `CalciteQueryEngineConfiguration#getQueryMemoryQuota`, `CalciteQueryEngineConfiguration#getGlobalMemoryQuota` 
must be created(or reused) to prevent user from harming server nodes.

### Risks and Assumptions

#### Assumptions:

1. Primary Ignite use-case is OLTP with many short-lived small transactions.
2. SQL, Scan queries inside transaction doesn't touch many entries in single execution (mostly PK, index searches).
3. MVCC implementation is out of scope.

#### Risks:

1. Server node OOM failures due to the huge heap usage by transactions state.
2. Performance penalty of SELECT queries executed inside transaction.

## PHASES

### PHASE 1
* Read-Committed isolation ONLY.
* Transaction aware SQL.
* Transaction aware Scan query.
* Transaction quotas.

### PHASE 2
* Repeatable read, serializable isolation support.

### PHASE 3
* Off heap locks implementation.
* Durability, failover protocol changes.