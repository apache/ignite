/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTEXCEPTION_HPP_INCLUDED
#define GRIDCLIENTEXCEPTION_HPP_INCLUDED

#include <string>
#include <exception>

#include <gridgain/gridconf.hpp>
#include <boost/exception/exception.hpp>

/**
 * Basic class for all exceptions in client.
 */
#ifdef _MSC_VER
class GRIDGAIN_API GridClientException : 
	public std::exception,
    public boost::exception_detail::clone_base {
public:
    /** Default constructor. */
    GridClientException() : exception("Unknown grid exception") {
    }

    /**
     * Constructor of exception with message text.
     *
     * @param what Exception text.
     */
    GridClientException(std::string what): exception(what.c_str()) {
    }

	/**
     * Clones this exception's instance.
     *
     * This method is implemented to support rethrowing this exception from
     * boost::packaged_task (which uses boost::current_exception()).
     *
     * @return A pointer to a newly created copy of this exception's instance.
     * Needs to be deleted afterwards.
     */
    virtual clone_base const * clone() const {
        return new GridClientException(what());
    }

    /**
     * Rethrows this exception.
     */
    virtual void rethrow() const {
        throw *this;
    }
};
#else
class GRIDGAIN_API GridClientException:
    public std::exception,
    public boost::exception_detail::clone_base {
public:
    /** Default constructor. */
    GridClientException()
            : what_("Unknown grid exception") {
    }

    /**
     * Constructor of exception with message text.
     *
     * @param what Exception text.
     */
    GridClientException(const std::string& what)
            : what_(what) {
    }

    ~GridClientException() throw () {
    }

    virtual const char* what() const throw () {
        return what_.c_str();
    }

    /**
     * Clones this exception's instance.
     *
     * This method is implemented to support rethrowing this exception from
     * boost::packaged_task (which uses boost::current_exception()).
     *
     * @return A pointer to a newly created copy of this exception's instance.
     * Needs to be deleted afterwards.
     */
    virtual clone_base const * clone() const {
        return new GridClientException(what());
    }

    /**
     * Rethrows this exception.
     */
    virtual void rethrow() const {
        throw *this;
    }

private:
    std::string what_;
};

#endif

/**
 * Exception thrown if server is not reachable.
 */
class GRIDGAIN_API GridServerUnreachableException: public GridClientException {
public:
    GridServerUnreachableException(const std::string& what)
            : GridClientException(what) {
    }

    virtual const char* what() const throw () {
        return "Server is not reachable";
    }
};

/**
 * Connection closed exception.
 */
class GRIDGAIN_API GridClientClosedException: public GridClientException {
public:
    virtual const char* what() const throw () {
        return "Connection closed";
    }

    /**
     * See parent.
     */
    virtual clone_base const * clone() const {
        return new GridClientClosedException();
    }

    /**
     * Rethrows this exception.
     */
    virtual void rethrow() const {
        throw *this;
    }
};

/**
 * Connection closed after idle exception.
 */
class GRIDGAIN_API GridConnectionIdleClosedException: public GridClientException {
public:
    virtual const char* what() const throw () {
        return "Connection closed after idle";
    }
};

/**
 * Connection reset exception.
 */
class GRIDGAIN_API GridClientConnectionResetException: public GridClientException {
public:
    /**
     * Constructor of exception with message text.
     *
     * @param what Exception text.
     */
    GridClientConnectionResetException(const std::string& what)
            : GridClientException(what) {
    }
};

/**
 * Command execution exception.
 */
class GRIDGAIN_API GridClientCommandException: public GridClientException {
public:
    /**
     * Constructor of exception with message text.
     *
     * @param what Exception text.
     */
    GridClientCommandException(const std::string& what)
            : GridClientException(what) {
    }
};

/**
 * Thrown when client future times out.
 *
 * This may happen when future->get(timeout) is called.
 */
class GRIDGAIN_API GridClientFutureTimeoutException: public GridClientException {
public:
    virtual const char* what() const throw () {
        return "Future timed out";
    }
};

#endif // GRIDCLIENTEXCEPTION_HPP_INCLUDED
