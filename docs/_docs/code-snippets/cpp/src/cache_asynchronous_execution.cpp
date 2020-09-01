#include <stdint.h>
#include <iostream>
#include <sstream>

#include <ignite/ignition.h>
#include <ignite/compute/compute.h>

using namespace ignite;
using namespace cache;

//tag::cache-asynchronous-execution[]
/*
 * Function class.
 */
class HelloWorld : public compute::ComputeFunc<void>
{
    friend struct ignite::binary::BinaryType<HelloWorld>;
public:
    /*
     * Default constructor.
     */
    HelloWorld()
    {
        // No-op.
    }

    /**
     * Callback.
     */
    virtual void Call()
    {
        std::cout << "Job Result: Hello World" << std::endl;
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
        struct BinaryType<HelloWorld>
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("HelloWorld");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "HelloWorld";
            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static int32_t GetHashCode(const HelloWorld& obj)
            {
                return 0;
            }

            static bool IsNull(const HelloWorld& obj)
            {
                return false;
            }

            static void GetNull(HelloWorld& dst)
            {
                dst = HelloWorld();
            }

            static void Write(BinaryWriter& writer, const HelloWorld& obj)
            {
                // No-op.
            }

            static void Read(BinaryReader& reader, HelloWorld& dst)
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

    Ignite ignite = Ignition::Start(cfg);

    // Get binding instance.
    IgniteBinding binding = ignite.GetBinding();

    // Registering our class as a compute function.
    binding.RegisterComputeFunc<HelloWorld>();

    // Get compute instance.
    compute::Compute compute = ignite.GetCompute();

    // Declaring function instance.
    HelloWorld helloWorld;

    // Making asynchronous call.
    compute.RunAsync(helloWorld);
}
//end::cache-asynchronous-execution[]