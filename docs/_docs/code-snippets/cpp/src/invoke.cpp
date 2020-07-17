#include <stdint.h>
#include <iostream>
#include <sstream>

#include <ignite/ignition.h>
#include <ignite/compute/compute.h>
#include "ignite/cache/cache_entry_processor.h"

using namespace ignite;
using namespace cache;

//tag::invoke[]
/**
 * Processor for invoke method.
 */
class IncrementProcessor : public cache::CacheEntryProcessor<std::string, int32_t, int32_t, int32_t>
{
public:
    /**
     * Constructor.
     */
    IncrementProcessor()
    {
        // No-op.
    }

    /**
     * Copy constructor.
     *
     * @param other Other instance.
     */
    IncrementProcessor(const IncrementProcessor& other)
    {
        // No-op.
    }

    /**
     * Assignment operator.
     *
     * @param other Other instance.
     * @return This instance.
     */
    IncrementProcessor& operator=(const IncrementProcessor& other)
    {
        return *this;
    }

    /**
     * Call instance.
     */
    virtual int32_t Process(MutableCacheEntry<std::string, int32_t>& entry, const int& arg)
    {
        // Increment the value for a specific key by 1.
        // The operation will be performed on the node where the key is stored.
        // Note that if the cache does not contain an entry for the given key, it will
        // be created.
        if (!entry.IsExists())
            entry.SetValue(1);
        else
            entry.SetValue(entry.GetValue() + 1);

        return entry.GetValue();
    }
};

/**
 * Binary type structure. Defines a set of functions required for type to be serialized and deserialized.
 */
namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<IncrementProcessor>
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("IncrementProcessor");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "IncrementProcessor";
            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static int32_t GetHashCode(const IncrementProcessor& obj)
            {
                return 0;
            }

            static bool IsNull(const IncrementProcessor& obj)
            {
                return false;
            }

            static void GetNull(IncrementProcessor& dst)
            {
                dst = IncrementProcessor();
            }

            static void Write(BinaryWriter& writer, const IncrementProcessor& obj)
            {
                // No-op.
            }

            static void Read(BinaryReader& reader, IncrementProcessor& dst)
            {
                // No-op.
            }
        };
    }
}

int main()
{
    IgniteConfiguration cfg;
    cfg.springCfgPath = "platforms/cpp/examples/put-get-example/config/example-cache.xml";

    Ignite ignite = Ignition::Start(cfg);

    // Get cache instance.
    Cache<std::string, int32_t> cache = ignite.GetOrCreateCache<std::string, int32_t>("myCache");

    // Get binding instance.
    IgniteBinding binding = ignite.GetBinding();

    // Registering our class as a cache entry processor.
    binding.RegisterCacheEntryProcessor<IncrementProcessor>();

    std::string key("mykey");
    IncrementProcessor inc;

    cache.Invoke<int32_t>(key, inc, NULL);
}
//end::invoke[]