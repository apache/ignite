#include <stdint.h>
#include <iostream>

#include <ignite/ignition.h>
#include <ignite/cache/query/continuous/continuous_query.h>

#include "ignite/examples/person.h"

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

//tag::continuous-query-filter[]
template<typename K, typename V>
struct RemoteFilter : event::CacheEntryEventFilter<int32_t, std::string>
{
    /**
     * Default constructor.
     */
    RemoteFilter()
    {
        // No-op.
    }

    /**
     * Destructor.
     */
    virtual ~RemoteFilter()
    {
        // No-op.
    }

    /**
     * Event callback.
     *
     * @param event Event.
     * @return True if the event passes filter.
     */
    virtual bool Process(const CacheEntryEvent<K, V>& event)
    {
        std::cout << "The value for key " << event.GetKey() <<
            " was updated from " << event.GetOldValue() << " to " << event.GetValue() << std::endl;
        return true;
    }
};

namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType< RemoteFilter<int32_t, std::string> >
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("RemoteFilter<int32_t,std::string>");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "RemoteFilter<int32_t,std::string>";

            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static bool IsNull(const RemoteFilter<int32_t, std::string>&)
            {
                return false;
            }

            static void GetNull(RemoteFilter<int32_t, std::string>& dst)
            {
                dst = RemoteFilter<int32_t, std::string>();
            }

            static void Write(BinaryWriter& writer, const RemoteFilter<int32_t, std::string>& obj)
            {
                // No-op.
            }

            static void Read(BinaryReader& reader, RemoteFilter<int32_t, std::string>& dst)
            {
                // No-op.
            }
        };
    }
}

int main()
{
    IgniteConfiguration cfg;
    cfg.springCfgPath = "/path/to/configuration.xml";

    // Start a node.
    Ignite ignite = Ignition::Start(cfg);

    // Get binding.
    IgniteBinding binding = ignite.GetBinding();

    // Registering remote filter.
    binding.RegisterCacheEntryEventFilter<RemoteFilter<int32_t, std::string>>();

    // Get cache instance.
    Cache<int32_t, std::string> cache = ignite.GetOrCreateCache<int32_t, std::string>("myCache");

    // Declaring custom listener.
    Listener<int32_t, std::string> listener;

    // Declaring filter.
    RemoteFilter<int32_t, std::string> filter;

    // Declaring continuous query.
    continuous::ContinuousQuery<int32_t, std::string> qry(MakeReference(listener), MakeReference(filter));
}
//end::continuous-query-filter[]