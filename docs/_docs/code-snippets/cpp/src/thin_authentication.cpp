//tag::thin-authentication[]
#include <ignite/thin/ignite_client.h>
#include <ignite/thin/ignite_client_configuration.h>

using namespace ignite::thin;

void TestClientWithAuth()
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:10800");

    // Use your own credentials here.
    cfg.SetUser("ignite");
    cfg.SetPassword("ignite");

    IgniteClient client = IgniteClient::Start(cfg);

    cache::CacheClient<int32_t, std::string> cacheClient =
        client.GetOrCreateCache<int32_t, std::string>("TestCache");

    cacheClient.Put(42, "Hello Ignite Thin Client with auth!");
}
//end::thin-authentication[]

int main()
{
    TestClientWithAuth();
}