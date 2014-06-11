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

const int8_t OBJECT_TYPE_OBJECT = 0;

class GridClientPortableMessage : public GridPortable {
public:
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

class GridClientResponse : public GridClientPortableMessage {
public:
    int32_t typeId() const {
        return -1000;
    }

    void writePortable(GridPortableWriter &writer) const {
        GridClientPortableMessage::writePortable(writer);

        writer.writeInt("status", status);
        writer.writeString("errorMsg", errorMsg);
        writer.writeVariant("res", res);
    }

    void readPortable(GridPortableReader &reader) {
        GridClientPortableMessage::readPortable(reader);

        status = reader.readInt("status");
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

        out.writeByte(portable.typeId());

        portable.writePortable(*this);
    }

    void writeInt(char* fieldName, int32_t val) {
		out.writeInt(val);
	}

	void writeString(char* fieldName, const string &str) {
		out.writeInt(str.length());
		out.writeBytes(str.data(), str.length());
	}
	
    void writeVariant(char* fieldName, const GridClientVariant &str) {

    }

	vector<int8_t> bytes() {
		return out.bytes();
	}

private:
	PortableOutput out;
};

class GridPortableMarshaller {
public:
    vector<int8_t> marshal(GridPortable &portable) {
		GridPortableWriterImpl writer;

		writer.writePortable(portable);

		return writer.bytes();
	}

    template<typename T>
    shared_ptr<T> unmarshal() {
        shared_ptr<T> res(new T);

        return res;
    }
};

#endif
