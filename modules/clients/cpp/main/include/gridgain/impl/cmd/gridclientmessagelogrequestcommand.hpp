/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDLOG_REQUEST_COMMAND_HPP_INCLUDED
#define GRIDLOG_REQUEST_COMMAND_HPP_INCLUDED

#include <string>

#include "gridgain/impl/cmd/gridclientmessagecommand.hpp"

/**
 * Log request command.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridLogRequestCommand : public GridClientMessageCommand {
public:
      /**
       * Sets new value for 'from' line request parameter.
       *
       * @param from Number of line in the log file to start with.
       */
      void from(int from) {
          from_ = from;
      }

      /**
       * Sets new value for 'to' line request parameter.
       *
       * @param from Number of line in the log file to end with.
       */
      void to(int to) {
          to_ = to;
      }

      /**
       * Sets new value for 'path' request parameter.
       *
       * @param path Path to log file to read lines from.
       */
      void path(const std::string& path) {
          path_ = path;
      }

      /**
       * Current value for 'from' parameter.
       *
       * @return Line number to read log file from.
       */
      int from() const {
          return from_;
      }

      /**
       * Current value for 'to' parameter.
       *
       * @return Line number to read log file to.
       */
      int to() const {
          return to_;
      }

      /**
       * Current value for 'path' parameter.
       *
       * @return Path to log file.
       */
      std::string path() const {
          return path_;
      }
private:
     /** From line. */
     int from_;

     /** To line. */
     int to_;

     /** Path. */
     std::string path_;
};

#endif
