#include <stdint.h>
#include <iostream>
#include <sstream>

#include <ignite/ignition.h>
#include <ignite/compute/compute.h>

using namespace ignite;

//tag::compute-run[]
/*
 * Function class.
 */
class PrintWord : public compute::ComputeFunc<void>
{
    friend struct ignite::binary::BinaryType<PrintWord>;
public:
    /*
     * Default constructor.
     */
    PrintWord()
    {
        // No-op.
    }    
    
    /*
     * Constructor.
     *
     * @param text Text.
     */
    PrintWord(const std::string& word) :
        word(word)
    {
        // No-op.
    }

    /**
     * Callback.
     */
    virtual void Call()
    {
        std::cout << word << std::endl;
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
        struct BinaryType<PrintWord>
        {
            static int32_t GetTypeId()
            {
                return GetBinaryStringHashCode("PrintWord");
            }

            static void GetTypeName(std::string& dst)
            {
                dst = "PrintWord";
            }

            static int32_t GetFieldId(const char* name)
            {
                return GetBinaryStringHashCode(name);
            }

            static int32_t GetHashCode(const PrintWord& obj)
            {
                return 0;
            }

            static bool IsNull(const PrintWord& obj)
            {
                return false;
            }

            static void GetNull(PrintWord& dst)
            {
                dst = PrintWord("");
            }

            static void Write(BinaryWriter& writer, const PrintWord& obj)
            {
                writer.RawWriter().WriteString(obj.word);
            }

            static void Read(BinaryReader& reader, PrintWord& dst)
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
    binding.RegisterComputeFunc<PrintWord>();

    // Get compute instance.
    compute::Compute compute = ignite.GetCompute();

    std::istringstream iss("Print words on different cluster nodes");
    std::vector<std::string> words((std::istream_iterator<std::string>(iss)),
        std::istream_iterator<std::string>());

    // Iterate through all words and print
    // each word on a different cluster node.
    for (std::string word : words)
    {
        // Run compute task.
        compute.Run(PrintWord(word));
    }
}
//end::compute-run[]