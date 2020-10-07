namespace ignite
{
    struct Country
    {
        Country() : population(0)
        {
            // No-op.
        }

        Country(const int32_t population, const std::string& name) :
            population(population),
            name(name)
        {
            // No-op.
        }

        std::string ToString() const
        {
            std::ostringstream oss;
            oss << "Country [population=" << population
                << ", name=" << name << ']';
            return oss.str();
        }

        int32_t population;
        std::string name;
    };
}

namespace ignite
{
    namespace binary
    {
        IGNITE_BINARY_TYPE_START(ignite::Country)

            typedef ignite::Country Country;

        IGNITE_BINARY_GET_TYPE_ID_AS_HASH(Country)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(Country)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(Country)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(Country)

            static void Write(BinaryWriter& writer, const ignite::Country& obj)
        {
            writer.WriteInt32("population", obj.population);
            writer.WriteString("name", obj.name);
        }

        static void Read(BinaryReader& reader, ignite::Country& dst)
        {
            dst.population = reader.ReadInt32("population");
            dst.name = reader.ReadString("name");
        }

        IGNITE_BINARY_TYPE_END
    }
};