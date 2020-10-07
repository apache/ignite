const IgniteClient = require('apache-ignite-client');
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;
const ObjectType = IgniteClient.ObjectType;
const CacheEntry = IgniteClient.CacheEntry;
const ScanQuery = IgniteClient.ScanQuery;

async function performScanQuery() {
    const igniteClient = new IgniteClient();
    try {
        await igniteClient.connect(new IgniteClientConfiguration('127.0.0.1:10800'));

        //tag::scan-query[]
        const cache = (await igniteClient.getOrCreateCache('myCache')).
            setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER);

        // Put multiple values using putAll()
        await cache.putAll([
            new CacheEntry(1, 'value1'),
            new CacheEntry(2, 'value2'),
            new CacheEntry(3, 'value3')]);

        // Create and configure scan query
        const scanQuery = new ScanQuery().
            setPageSize(1);
        // Obtain scan query cursor
        const cursor = await cache.query(scanQuery);
        // Get all cache entries returned by the scan query
        for (let cacheEntry of await cursor.getAll()) {
            console.log(cacheEntry.getValue());
        }

        //end::scan-query[]

        await igniteClient.destroyCache('myCache');
    }
    catch (err) {
        console.log(err.message);
    }
    finally {
        igniteClient.disconnect();
    }
}

performScanQuery();
