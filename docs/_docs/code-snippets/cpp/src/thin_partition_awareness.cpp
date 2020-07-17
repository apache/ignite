//tag::thin-partition-awareness[]
#include <ignite/thin/ignite_client.h>
#include <ignite/thin/ignite_client_configuration.h>

using namespace ignite::thin;

void TestClientPartitionAwareness()
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:10800,217.29.2.1:10800,200.10.33.1:10800");
    cfg.SetPartitionAwareness(true);

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cacheClient =
        client.GetOrCreateCache<int32_t, std::string>("TestCache");

    cacheClient.Put(42, "Hello Ignite Partition Awareness!");

    cacheClient.RefreshAffinityMapping();

    // Getting a value
    std::string val = cacheClient.Get(42);
}
//end::thin-partition-awareness[]

int main()
{
    TestClientPartitionAwareness();
}