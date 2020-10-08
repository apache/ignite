const IgniteClient = require('apache-ignite-client');
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;
const CacheConfiguration = IgniteClient.CacheConfiguration;
const ObjectType = IgniteClient.ObjectType;
const SqlFieldsQuery = IgniteClient.SqlFieldsQuery;

async function performSqlFieldsQuery() {
    const igniteClient = new IgniteClient();
    try {
        await igniteClient.connect(new IgniteClientConfiguration('127.0.0.1:10800'));
        const cache = await igniteClient.getOrCreateCache('myPersonCache', new CacheConfiguration()
            .setSqlSchema('PUBLIC'));

        // Create table using SqlFieldsQuery
        (await cache.query(new SqlFieldsQuery(
            'CREATE TABLE Person (id INTEGER PRIMARY KEY, firstName VARCHAR, lastName VARCHAR, salary DOUBLE)'))).getAll();

        // Insert data into the table
        const insertQuery = new SqlFieldsQuery('INSERT INTO Person (id, firstName, lastName, salary) values (?, ?, ?, ?)')
            .setArgTypes(ObjectType.PRIMITIVE_TYPE.INTEGER);
        (await cache.query(insertQuery.setArgs(1, 'John', 'Doe', 1000))).getAll();
        (await cache.query(insertQuery.setArgs(2, 'Jane', 'Roe', 2000))).getAll();

        // Obtain sql fields cursor
        const sqlFieldsCursor = await cache.query(
            new SqlFieldsQuery("SELECT concat(firstName, ' ', lastName), salary from Person").setPageSize(1));

        // Iterate over elements returned by the query
        do {
            console.log(await sqlFieldsCursor.getValue());
        } while (sqlFieldsCursor.hasMore());

        // Drop the table
        (await cache.query(new SqlFieldsQuery("DROP TABLE Person"))).getAll();
    } catch (err) {
        console.log(err.message);
    } finally {
        igniteClient.disconnect();
    }
}

performSqlFieldsQuery();