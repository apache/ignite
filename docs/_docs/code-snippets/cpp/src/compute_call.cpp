#include <stdint.h>
#include <iostream>
#include <sstream>

#include <ignite/ignition.h>
#include <ignite/compute/compute.h>

using namespace ignite;

//tag::compute-call[]
/*
 * Function class.
 */
class CountLength : public compute::ComputeFunc<int32_t>
{
    friend struct ignite::binary::BinaryType<CountLength>;
public:
    /*
     * Default constructor.
     */
    CountLength()
    {
        // No-op.
    }

    /*
     * Constructor.
     *
     * @param text Text.
     */
    CountLength(const std::string& word) :
        word(word)
    {
        // No-op.
    }

    /**
     * Callback.
     * Counts number of characters in provided word.
     *
     * @return Word's length.
     */
    virtual int32_t Call()
    {
        return word.length();
    }

    /** Word to print. */
    std::string word;

};

/**
 * Binary type structure. Defines a set of functions required for type to be serialized and deserialized.
 */
namespace ignite
{
    namespace binary
    {
        template<>
        struct BinaryType<CountLength>
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("CountLength");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "CountLength";
            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static int32_t GetHashCode(const CountLength& obj)
            {
                return 0;
            }

            static bool IsNull(const CountLength& obj)
            {
                return false;
            }

            static void GetNull(CountLength& dst)
            {
                dst = CountLength("");
            }

            static void Write(BinaryWriter& writer, const CountLength& obj)
            {
                writer.RawWriter().WriteString(obj.word);
            }

            static void Read(BinaryReader& reader, CountLength& dst)
            {
                dst.word = reader.RawReader().ReadString();
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
    binding.RegisterComputeFunc<CountLength>();

    // Get compute instance.
    compute::Compute compute = ignite.GetCompute();

    std::istringstream iss("How many characters");
    std::vector<std::string> words((std::istream_iterator<std::string>(iss)),
        std::istream_iterator<std::string>());

    int32_t total = 0;

    // Iterate through all words in the sentence, create and call jobs.
    for (std::string word : words)
    {
        // Add word length received from cluster node.
        total += compute.Call<int32_t>(CountLength(word));
    }
}
//end::compute-call[]