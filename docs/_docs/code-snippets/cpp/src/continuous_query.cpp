#include <iostream>

#include "ignite/ignite.h"
#include "ignite/ignition.h"

using namespace ignite;
using namespace cache;
using namespace query;

/**
 * Listener class.
 */
template<typename K, typename V>
class Listener : public event::CacheEntryEventListener<K, V>
{
public:
    /**
     * Default constructor.
     */
    Listener()
    {
        // No-op.
    }

    /**
     * Event callback.
     *
     * @param evts Events.
     * @param num Events number.
     */
    virtual void OnEvent(const CacheEntryEvent<K, V>* evts, uint32_t num)
    {
        for (uint32_t i = 0; i < num; ++i)
        {
            std::cout << "Queried entry [key=" << (evts[i].HasValue() ? evts[i].GetKey() : K())
                << ", val=" << (evts[i].HasValue() ? evts[i].GetValue() : V()) << ']'
                << std::endl;
        }
    }
};

int main()
{
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    Ignite ignite = Ignition::Start(cfg);

    //tag::continuous-query[]
    Cache<int32_t, std::string> cache = ignite.GetOrCreateCache<int32_t, std::string>("myCache");

    // Custom listener
    Listener<int32_t, std::string> listener;

    // Declaring continuous query.
    continuous::ContinuousQuery<int32_t, std::string> query(MakeReference(listener));

    // Declaring optional initial query
    ScanQuery initialQuery = ScanQuery();

    continuous::ContinuousQueryHandle<int32_t, std::string> handle = cache.QueryContinuous(query, initialQuery);

    // Iterating over existing data stored in the cache.
    QueryCursor<int32_t, std::string> cursor = handle.GetInitialQueryCursor();

    while (cursor.HasNext())
    {
        std::cout << cursor.GetNext().GetKey() << std::endl;
    }
    //end::continuous-query[]
}