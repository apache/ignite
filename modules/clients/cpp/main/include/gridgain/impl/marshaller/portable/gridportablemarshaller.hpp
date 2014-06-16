/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENT_PORTABLE_MARSHALLER_HPP_INCLUDED
#define GRIDCLIENT_PORTABLE_MARSHALLER_HPP_INCLUDED

#include <vector>
#include <iterator>

#include "gridgain/gridportable.hpp"
#include "gridgain/gridportablereader.hpp"
#include "gridgain/gridportablewriter.hpp"
#include "gridgain/impl/utils/gridclientbyteutils.hpp"

using namespace std;

const int8_t TYPE_NULL = 0;
const int8_t TYPE_BYTE = 1;
const int8_t TYPE_SHORT = 2;
const int8_t TYPE_INT = 3;
const int8_t TYPE_LONG = 4;
const int8_t TYPE_FLOAT = 5;
const int8_t TYPE_DOUBLE = 6;
const int8_t TYPE_BOOLEAN = 7;
const int8_t TYPE_CHAR = 8;
const int8_t TYPE_STRING = 9;
const int8_t TYPE_BYTE_ARRAY = 10;

const int8_t TYPE_LIST = 18;
const int8_t TYPE_MAP = 19;
const int8_t TYPE_UUID = 20;
const int8_t TYPE_USER_OBJECT = 21;

const int8_t OBJECT_TYPE_OBJECT = 0;

GridPortable* createPortable(int32_t typeId, GridPortableReader &reader);

class GridClientPortableMessage : public GridPortable {
public:
    void writePortable(GridPortableWriter &writer) const {
        writer.writeBytes("sesTok", sesTok);
    }

    void readPortable(GridPortableReader &reader) {
        sesTok = reader.readBytes("sesTok");
    }

    bool operator==(const GridPortable& other) const {
        assert(false);

        return false; // Not needed since is not used as key.
    }

    int hashCode() const {
        assert(false);

        return 0; // Not needed since is not used as key.
    }

private:
    vector<int8_t> sesTok;
};

class GridClientResponse : public GridClientPortableMessage {
public:
    int32_t typeId() const {
        return -1000;
    }

    void writePortable(GridPortableWriter &writer) const {
        GridClientPortableMessage::writePortable(writer);

        writer.writeInt32("status", status);
        writer.writeString("errorMsg", errorMsg);
        writer.writeVariant("res", res);
    }

    void readPortable(GridPortableReader &reader) {
        GridClientPortableMessage::readPortable(reader);

        status = reader.readInt32("status");
        errorMsg = reader.readString("errorMsg");
        res = reader.readVariant("res");
    }

    int32_t getStatus() {
        return status;
    }

    std::string getErrorMessage() {
        return errorMsg;
    }

    GridClientVariant getResult() {
        return res;
    }

private:
    int32_t status;

    std::string errorMsg;

    GridClientVariant res;
};

// TODO: 8536.
class GridClientNodeBean : public GridPortable {
    int32_t typeId() const {
        return -1;            
    }

    void writePortable(GridPortableWriter &writer) const {
    }

    void readPortable(GridPortableReader &reader) {
    }

    bool operator==(const GridPortable& other) const {
        assert(false);

        return false; // Not needed since is not used as key.
    }

    int hashCode() const {
        assert(false);

        return 0; // Not needed since is not used as key.
    }
};

// TODO: 8536 reuse existing message classes.
class GridClientTopologyRequest : public GridClientPortableMessage {
public:
    /**
     * Getter method for include metrics flag.
     *
     * @return Include metrics flag.
     */
    bool getIncludeMetrics() const {
        return includeMetrics;
    }

    /**
     * Setter method for include metrics flag.
     *
     * @param pIncludeMetrics Include metrics flag.
     */
    void setIncludeMetrics(bool pIncludeMetrics) {
        includeMetrics = pIncludeMetrics;
    }

    /**
     * Getter method for include attributes flag.
     *
     * @return Include node attributes flag.
     */
     bool getIncludeAttributes() const {
        return includeAttrs;
     }

    /**
     * Setter method for include attributes flag.
     *
     * @param pIncludeAttrs Include node attributes flag.
     */
     void setIncludeAttributes(bool pIncludeAttrs) {
        includeAttrs = pIncludeAttrs;
     }

    /**
     * Getter method for node id.
     *
     * @return Node identifier, if specified, empty string otherwise.
     */
     std::string getNodeId() const {
        return nodeId;
     }

    /**
     * Setter method for node id.
     *
     * @param pNodeId Node identifier to lookup.
     */
     void setNodeId(const std::string& nodeId) {
        this->nodeId = nodeId;
     }

    /**
     * Getter method for node id.
     *
     * @return Node ip address if specified, empty string otherwise.
     */
    std::string getNodeIp() const {
        return nodeIp;
    }

    /**
     * Setter method for node id.
     *
     * @param pNodeIp Node ip address to lookup.
     */
    void setNodeIp(const std::string& nodeIp) {
        this->nodeIp = nodeIp;
    }

    int32_t typeId() const {
        return -1000;
    }

    void writePortable(GridPortableWriter &writer) const {
        GridClientPortableMessage::writePortable(writer);

        writer.writeString("nodeId", nodeId);
    }

    void readPortable(GridPortableReader &reader) {
        GridClientPortableMessage::readPortable(reader);

        nodeId = reader.readString("nodeId");
    }

private:
    /** Id of requested node. */
     std::string nodeId;

    /** IP address of requested node. */
     std::string nodeIp;

    /** Include metrics flag. */
     bool includeMetrics;

    /** Include node attributes flag. */
     bool includeAttrs;
};

class PortableOutput {
public:
	PortableOutput(size_t capacity) {
        out.reserve(capacity);
	}

    void writeByte(int8_t val) {
        out.push_back(val);
    }

	void writeInt(int32_t val) {
        int8_t* bytes = reinterpret_cast<int8_t*>(&val);

        out.insert(out.end(), bytes, bytes + 4);
	}

    void writeLong(int64_t val) {
        int8_t* bytes = reinterpret_cast<int8_t*>(&val);

        out.insert(out.end(), bytes, bytes + 8);
	}

	void writeBytes(const void* src, size_t size) {
        const int8_t* bytes = reinterpret_cast<const int8_t*>(src);

        out.insert(out.end(), bytes, bytes + size);
	}

	vector<int8_t> bytes() {
        return out;
	}

private:
    vector<int8_t> out;
};

class GridPortableWriterImpl : public GridPortableWriter {
public:
	GridPortableWriterImpl() : out(1024) {
	}

    void writePortable(GridPortable &portable) {
        out.writeByte(OBJECT_TYPE_OBJECT);

        out.writeInt(portable.typeId());

        portable.writePortable(*this);
    }

    void writeInt32(char* fieldName, int32_t val) {
		out.writeInt(val);
	}

	void writeString(char* fieldName, const string &str) {
		out.writeInt(str.length());
		out.writeBytes(str.data(), str.length());
	}
	
    void writeBytes(char* fieldName, const std::vector<int8_t>& val) {
        out.writeInt(val.size());
        out.writeBytes(val.data(), val.size());
    }

    void writeCollection(const vector<GridClientVariant>& col) {
        out.writeByte(OBJECT_TYPE_OBJECT);

        out.writeInt(col.size());

        for (auto iter = col.begin(); iter != col.end(); ++iter) {
            GridClientVariant variant = *iter;

            writeVariant(nullptr, variant);
        }
    }

    void writeMap(const unordered_map<GridClientVariant, GridClientVariant>& map) {
        out.writeByte(OBJECT_TYPE_OBJECT);

        out.writeInt(map.size());

        for (auto iter = map.begin(); iter != map.end(); ++iter) {
            GridClientVariant key = iter->first;
            GridClientVariant val = iter->second;

            writeVariant(nullptr, key);
            writeVariant(nullptr, val);
        }
    }

    void writeVariant(char* fieldName, const GridClientVariant &val) {
        if (val.hasInt()) {
            out.writeByte(TYPE_INT);
            out.writeInt(val.getInt());
        }
        else if (val.hasString()) {
            out.writeByte(TYPE_STRING);

            string str = val.getString();

    		out.writeInt(str.length());
	    	out.writeBytes(str.data(), str.length());
        }
        else if (val.hasUuid()) {
            out.writeByte(TYPE_UUID);

            GridClientUuid uuid = val.getUuid();

            out.writeLong(uuid.mostSignificantBits());
            out.writeLong(uuid.leastSignificantBits());
        }
        else if (val.hasPortable()) {
            out.writeByte(TYPE_USER_OBJECT);

            writePortable(*val.getPortable<GridPortable>());
        }
        else if (val.hasVariantVector()) {
            out.writeByte(TYPE_LIST);

            writeCollection(val.getVariantVector());
        }
        else if (val.hasVariantMap()) {
            out.writeByte(TYPE_MAP);

            writeMap(val.getVariantMap());
        }
        else if (!val.hasAnyValue()) {
            out.writeByte(TYPE_NULL);
        }
        else {
            assert(false);
        }
    }
    
    void writeCollection(char* fieldName, const vector<GridClientVariant> &val) {
        writeCollection(val);
    }

    void writeMap(char* fieldName, const unordered_map<GridClientVariant, GridClientVariant> &map) {
        writeMap(map);
    }

	vector<int8_t> bytes() {
		return out.bytes();
	}

private:
	PortableOutput out;
};

class PortableInput {
public:
    PortableInput(vector<int8_t>& data) : bytes(data), pos(0) {
    }

    int8_t readByte() {
        checkAvailable(1);

        return bytes[pos++];
    }

    int32_t readInt() {
        checkAvailable(4);

        int32_t res = 0;

        memcpy(&res, bytes.data() + pos, 4);

        pos += 4;

        return res;
    }

    int64_t readLong() {
        checkAvailable(8);

        int64_t res = 0;

        memcpy(&res, bytes.data() + pos, 8);

        pos += 8;

        return res;
    }

    vector<int8_t> readBytes(int32_t size) {
        checkAvailable(size);

        vector<int8_t> vec;

        vec.insert(vec.end(), bytes.data() + pos, bytes.data() + pos + size);

        pos += size;

        return vec;
    }

private:
    void checkAvailable(int cnt) {
        assert(pos + cnt <= bytes.size());
    }
    
    int32_t pos;

    vector<int8_t>& bytes;
};

class GridPortableReaderImpl : GridPortableReader {
public:
    GridPortableReaderImpl(vector<int8_t>& data) : in(data) {
    }

    GridPortable* readPortable() {
        int8_t type = in.readByte();

        assert(type == OBJECT_TYPE_OBJECT);

        int32_t typeId = in.readInt();

        GridPortable* portable = createPortable(typeId, *this);

        return portable;
    }

    int32_t readInt32(char* fieldName) {
        return in.readInt();
    }

    vector<int8_t> readBytes(char* fieldName) {
        int32_t size = in.readInt();

        return in.readBytes(size);
    }

    string readString(char* fieldName) {
        int size = in.readInt();

        if (size == -1)
            return string();

        vector<int8_t> bytes = in.readBytes(size);

        return string((char*)bytes.data(), size);
    }

    GridClientVariant readVariant(char* fieldName) {
        int8_t type = in.readByte();

        switch (type) {
            case TYPE_NULL:
                return GridClientVariant();

            case TYPE_INT:
                return GridClientVariant(in.readInt());

            case TYPE_LONG:
                return GridClientVariant(in.readLong());

            case TYPE_UUID: {
                if (in.readByte() == 0)
                    return GridClientVariant();

                int64_t mostSignificantBits = in.readLong();
                int64_t leastSignificantBits = in.readLong();

                return GridClientVariant(GridClientUuid(mostSignificantBits, leastSignificantBits));
            }

            case TYPE_LIST:
                return GridClientVariant(readCollection());

            case TYPE_MAP:
                return GridClientVariant(readMap());

            case TYPE_USER_OBJECT:
                return GridClientVariant(readPortable());

            default:
                assert(false);
        }

        return GridClientVariant();
    }

    vector<GridClientVariant> readCollection() {
        int8_t type = in.readByte();

        assert(type == OBJECT_TYPE_OBJECT);

        int32_t size = in.readInt();

        if (size == -1)
            return vector<GridClientVariant>();

        vector<GridClientVariant> vec;

        for (int i = 0; i < size; i++)
            vec.push_back(readVariant(nullptr));

        return vec;
    }

    vector<GridClientVariant> readCollection(char* fieldName) {
        return readCollection();
    }

    unordered_map<GridClientVariant, GridClientVariant> readMap() {
        int8_t type = in.readByte();

        assert(type == OBJECT_TYPE_OBJECT);

        int32_t size = in.readInt();

        if (size == -1)
            return unordered_map<GridClientVariant, GridClientVariant>();
        
        unordered_map<GridClientVariant, GridClientVariant> map;

        for (int i = 0; i < size; i++) {
            GridClientVariant key = readVariant(nullptr);
            GridClientVariant val = readVariant(nullptr);

            map[key] = val;
        }

        return map;
    }

    unordered_map<GridClientVariant, GridClientVariant> readMap(char* fieldName) {
        return readMap();
    }

private:
    PortableInput in;
};

class GridPortableMarshaller {
public:
    vector<int8_t> marshal(GridPortable& portable) {
		GridPortableWriterImpl writer;

		writer.writePortable(portable);

		return writer.bytes();
	}

    template<typename T>
    T* unmarshal(std::vector<int8_t>& data) {
		GridPortableReaderImpl reader(data);

        return static_cast<T*>(reader.readPortable());
    }
};

#endif
