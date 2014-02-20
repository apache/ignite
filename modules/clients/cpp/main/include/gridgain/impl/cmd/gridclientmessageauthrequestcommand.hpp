// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDLOG_AUTH_COMMAND_HPP_INCLUDED
#define GRIDLOG_AUTH_COMMAND_HPP_INCLUDED

#include <string>

#include "gridgain/impl/cmd/gridclientmessagecommand.hpp"

/**
 * Authentication request command.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridAuthenticationRequestCommand : public GridClientMessageCommand {
public:
    /**
     * Set credentials method.
     *
     * @param creds Credentials.
     */
    void credentials(const std::string& creds) {
        creds_ = creds;
    }

    /**
     * Retrieve current credentials.
     *
     * @return Credentials.
     */
    const std::string credentials() const {
        return creds_;
    }

private:
    /** Credentials. */
    std::string creds_;
};
#endif
