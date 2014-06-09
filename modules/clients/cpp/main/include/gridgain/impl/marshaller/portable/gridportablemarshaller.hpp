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

class GridPortableMarshaller {
public:
    void marshal(GridPortable &portable) {
    }
};

class GridPortableWriterImpl : public GridPortableWriter {
public:
    void writeInt(char* fieldName, int32_t val) {
    }
};

#endif
