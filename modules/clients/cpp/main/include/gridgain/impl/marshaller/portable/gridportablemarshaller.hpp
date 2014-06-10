/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENT_PORTABLE_MARSHALLER_HPP_INCLUDED
#define GRIDCLIENT_PORTABLE_MARSHALLER_HPP_INCLUDED

#include "gridgain/gridportable.hpp"
#include "gridgain/gridportablereader.hpp"
#include "gridgain/gridportablewriter.hpp"

class GridClientPortableMessage : public GridPortable {
public:
    void writePortable(GridPortableWriter &writer) const {
    }

    void readPortable(GridPortableReader &reader) {
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
	PortableOutput(size_t initCapacity) : capacity(initCapacity), cnt(0) {
		buffer = new int8_t[capacity];
	}

	void writeInt(int32_t val) {
		ensureCapacity(4);

		memcpy(buffer, &val, 4);

		cnt += 4;
	}

	void writeBytes(const void* src, size_t size) {
		ensureCapacity(size);

		memcpy(buffer, src, size);

		cnt += size;
	}

	void bytes(int8_t*& bytes, size_t &size) {
		bytes = buffer;

		size = cnt;
	}

private:
	size_t capacity;

	size_t cnt;

	int8_t* buffer;

	void ensureCapacity(int size) {
		if (cnt + size > capacity) {
			int8_t* newBuffer = new int8_t[capacity * 2 + size];
			
			memcpy(newBuffer, buffer, cnt);

			delete[] buffer;

			buffer = newBuffer;
		}
	}
};

class GridPortableWriterImpl : public GridPortableWriter {
public:
	GridPortableWriterImpl() : out(1024) {
	}

    void writeInt(char* fieldName, int32_t val) {
		out.writeInt(val);
	}

	void writeString(char* fieldName, const string &str) {
		out.writeInt(str.length());
		out.writeBytes(str.data(), str.length());
	}
	
	void bytes(int8_t*& bytes, size_t &size) {
		out.bytes(bytes, size);
	}

private:
	PortableOutput out;
};

class GridPortableMarshaller {
public:
    void marshal(GridPortable &portable, int8_t*& bytes, size_t &size) {
		GridPortableWriterImpl writer;

		portable.writePortable(writer);

		writer.bytes(bytes, size);
	}
};

#endif
