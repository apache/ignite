//tag::thin-creating-client-instance[]
#include <ignite/thin/ignite_client.h>
#include <ignite/thin/ignite_client_configuration.h>

using namespace ignite::thin;

void TestClient()
{
    IgniteClientConfiguration cfg;

    //Endpoints list format is "<host>[port[..range]][,...]"
    cfg.SetEndPoints("127.0.0.1:11110,example.com:1234..1240");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cacheClient =
        client.GetOrCreateCache<int32_t, std::string>("TestCache");

    cacheClient.Put(42, "Hello Ignite Thin Client!");
}
//end::thin-creating-client-instance[]

int main()
{
    TestClient();
}