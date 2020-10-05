const IgniteClient = require('apache-ignite-client');
const IgniteClientConfiguration = IgniteClient.IgniteClientConfiguration;
const ObjectType = IgniteClient.ObjectType;
const MapObjectType = IgniteClient.MapObjectType;

async function setCacheKeyValueTypes() {
    const igniteClient = new IgniteClient();
    try {
        await igniteClient.connect(new IgniteClientConfiguration('127.0.0.1:10800'));
        //tag::mapping[]
        const cache = await igniteClient.getOrCreateCache('myCache');
        // Set cache key/value types
        cache.setKeyType(ObjectType.PRIMITIVE_TYPE.INTEGER)
            .setValueType(new MapObjectType(
                MapObjectType.MAP_SUBTYPE.LINKED_HASH_MAP,
                ObjectType.PRIMITIVE_TYPE.SHORT,
                ObjectType.PRIMITIVE_TYPE.BYTE_ARRAY));
        //end::mapping[]
        await cache.get(1)
    } catch (err) {
        console.log(err.message);
    } finally {
        igniteClient.disconnect();
    }
}

setCacheKeyValueTypes();
