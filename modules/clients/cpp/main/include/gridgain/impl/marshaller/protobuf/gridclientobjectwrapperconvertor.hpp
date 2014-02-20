// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_OBJECT_WRAPPER_CONVERTOR_HPP_INCLUDED
#define GRID_CLIENT_OBJECT_WRAPPER_CONVERTOR_HPP_INCLUDED

#include "gridgain//impl/marshaller/protobuf/ClientMessages.pb.h"
#include "gridgain/gridclientvariant.hpp"

using namespace org::gridgain::grid::kernal::processors::rest::client::message;

/** Helper class for wrapping and unwrapping GridClientVariant to/from protobuf.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridClientObjectWrapperConvertor {
public:
    /**
     * Converts protobuf object wrapper to GridClientVariant.
     *
     * @param objWrapper Protobuf object.
     * @param var Value to fill.
     */
    static bool unwrapSimpleType(const ObjectWrapper& objWrapper, GridClientVariant& var);

    /**
     * Converts GridClientVariant to protobuf.
     *
     * @param var Variable of type GridClientVariant.
     * @param objWrapper Protobuf structure to fill.
     */
    static bool wrapSimpleType(const GridClientVariant& var, ObjectWrapper& objWrapper);
};

#endif
