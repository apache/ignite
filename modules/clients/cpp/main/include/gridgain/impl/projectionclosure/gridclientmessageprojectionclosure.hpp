// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_MESSAGE_PROJECTION_CLOSURE_HPP_INCLUDED
#define GRID_CLIENT_MESSAGE_PROJECTION_CLOSURE_HPP_INCLUDED

#include <string>

#include "gridgain/impl/cmd/gridclientmessage.hpp"
#include "gridgain/impl/projectionclosure/gridclientprojectionclosure.hpp"
#include "gridgain/impl/cmd/gridclientmessagecommand.hpp"

/**
 * Base class for message closures.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class ClientMessageProjectionClosure : public ClientProjectionClosure {
public:
    /**
     * Generic public constructor.
     *
     * @param pClientId Client id.
     */
    ClientMessageProjectionClosure(std::string pClientId) :
        clientId(pClientId) {};

    ClientMessageProjectionClosure(GridUuid & clientId) :
        clientId(clientId) {};

    /**
     * Fills generic message command fields.
     *
     * @param cmd Message command.
     */
    void fillRequestHeader(GridClientMessageCommand& cmd, TGridClientNodePtr node) {
        int requestId = cmd.generateNewId();

        cmd.setRequestId(requestId);
        cmd.setClientId(clientId);
        cmd.setDestinationId(node->getNodeId());
    }

private:
    /** Client id. */
    GridUuid clientId;
};

#endif
