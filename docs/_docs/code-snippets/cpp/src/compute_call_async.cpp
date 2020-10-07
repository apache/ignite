#include <stdint.h>
#include <iostream>
#include <sstream>

#include <ignite/ignition.h>
#include <ignite/compute/compute.h>

using namespace ignite;

//tag::compute-call-async[]
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
    compute::Compute asyncCompute = ignite.GetCompute();

    std::istringstream iss("Count characters using callable");
    std::vector<std::string> words((std::istream_iterator<std::string>(iss)),
        std::istream_iterator<std::string>());

    std::vector<Future<int32_t>> futures;

    // Iterate through all words in the sentence, create and call jobs.
    for (std::string word : words)
    {
        // Counting number of characters remotely.
        futures.push_back(asyncCompute.CallAsync<int32_t>(CountLength(word)));
    }

    int32_t total = 0;

    // Counting total number of characters.
    for (Future<int32_t> future : futures)
    {
        // Waiting for results.
        future.Wait();

        total += future.GetValue();
    }

    // Printing result.
    std::cout << "Total number of characters: " << total << std::endl;
}
//end::compute-call-async[]