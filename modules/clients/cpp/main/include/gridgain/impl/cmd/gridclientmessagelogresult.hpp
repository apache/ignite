/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLEINT_MESSAGE_LOG_RESULT_HPP_INCLUDED
#define GRID_CLEINT_MESSAGE_LOG_RESULT_HPP_INCLUDED

#include "gridgain/impl/cmd/gridclientmessageresult.hpp"

/**
 * Log request command result.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridClientMessageLogResult : public GridClientMessageResult {
public:
    /**
     * Retrieve lines read by the operation.
     *
     * @return Array of lines.
     */
    const std::vector<std::string> lines() const {
        return lines_;
    }

    /**
     * Set lines read by the operation.
     *
     * @param lines Lines read.
     */
    void lines(const std::vector<std::string>& lines) {
        lines_ = lines;
    }

private:
    /** Lines read. */
    std::vector<std::string> lines_;
};

#endif
