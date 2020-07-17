#include <ignite/thin/ignite_client.h>
#include <ignite/thin/ignite_client_configuration.h>

using namespace ignite::thin;

int main()
{
    IgniteClientConfiguration cfg;
    cfg.SetEndPoints("127.0.0.1:10800");

    IgniteClient client = IgniteClient::Start(cfg);

    //tag::thin-getting-cache-instance[]
    cache::CacheClient<int32_t, std::string> cache =
        client.GetOrCreateCache<int32_t, std::string>("TestCache");
    //end::thin-getting-cache-instance[]

    //tag::basic-cache-operations[]
    std::map<int, std::string> vals;
    for (int i = 1; i < 100; i++)
    {
        vals[i] = i;
    }

    cache.PutAll(vals);
    cache.Replace(1, "2");
    cache.Put(101, "101");
    cache.RemoveAll();
    //end::basic-cache-operations[]
}