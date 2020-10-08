const IgniteClient = require('apache-ignite-client');
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;

async function getExistingCache() {
    const igniteClient = new IgniteClient();
    try {
        await igniteClient.connect(new IgniteClientConfiguration('127.0.0.1:10800'));
        // Get existing cache by name
        const cache = igniteClient.getCache('myCache');
    }
    catch (err) {
        console.log(err.message);
    }
    finally {
        igniteClient.disconnect();
    }
}

getExistingCache();