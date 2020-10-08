namespace ignite
{
    struct CityKey
    {
        CityKey() : id(0)
        {
            // No-op.
        }

        CityKey(int32_t id, const std::string& name) :
            id(id),
            name(name)
        {
            // No-op.
        }

        std::string ToString() const
        {
            std::ostringstream oss;

            oss << "CityKey [id=" << id
                << ", name=" << name << ']';

            return oss.str();
        }

        int32_t id;
        std::string name;
    };
}

namespace ignite
{
    namespace binary
    {
        IGNITE_BINARY_TYPE_START(ignite::CityKey)

            typedef ignite::CityKey CityKey;

        IGNITE_BINARY_GET_TYPE_ID_AS_HASH(CityKey)
            IGNITE_BINARY_GET_TYPE_NAME_AS_IS(CityKey)
            IGNITE_BINARY_GET_FIELD_ID_AS_HASH
            IGNITE_BINARY_IS_NULL_FALSE(CityKey)
            IGNITE_BINARY_GET_NULL_DEFAULT_CTOR(CityKey)

            static void Write(BinaryWriter& writer, const ignite::CityKey& obj)
        {
            writer.WriteInt64("id", obj.id);
            writer.WriteString("name", obj.name);
        }

        static void Read(BinaryReader& reader, ignite::CityKey& dst)
        {
            dst.id = reader.ReadInt32("id");
            dst.name = reader.ReadString("name");
        }

        IGNITE_BINARY_TYPE_END
    }
};