namespace ignite
{
    struct City
    {
        City() : population(0)
        {
            // No-op.
        }

        City(const int32_t population) :
            population(population)
        {
            // No-op.
        }

        std::string ToString() const
        {
            std::ostringstream oss;
            oss << "City [population=" << population << ']';
            return oss.str();
        }

        int32_t population;
    };
}

namespace ignite
{
    namespace binary
    {
        IGNITE_BINARY_TYPE_START(ignite::City)

            typedef ignite::City City;

        IGNITE_BINARY_GET_TYPE_ID_AS_HASH(City)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(City)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(City)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(City)

            static void Write(BinaryWriter& writer, const ignite::City& obj)
        {
            writer.WriteInt32("population", obj.population);
        }

        static void Read(BinaryReader& reader, ignite::City& dst)
        {
            dst.population = reader.ReadInt32("population");
        }

        IGNITE_BINARY_TYPE_END
    }
};