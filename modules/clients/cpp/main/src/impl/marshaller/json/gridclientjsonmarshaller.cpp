// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include <iostream>
#include <sstream>
#include <iterator>
#include <algorithm>
#include <map>
#include <sstream>

#include <boost/foreach.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/asio.hpp>

#include "gridgain/impl/marshaller/json/gridclientjsonmarshaller.hpp"
#include "gridgain/impl/marshaller/gridnodemarshallerhelper.hpp"
#include "gridgain/impl/utils/gridclientbyteutils.hpp"
#include "gridgain/gridclientprotocolconfiguration.hpp"
#include "gridgain/gridclientnode.hpp"

#include "gridgain/impl/utils/gridclientlog.hpp"
#include "gridgain/impl/utils/gridclientbyteutils.hpp"

void GridClientJsonMarshaller::fillRequestHeader(const GridClientMessage& clientMsg, TRequestParams& params) {
    params["requestId"] = boost::lexical_cast<std::string>(clientMsg.getRequestId());
    params["clientId"] = clientMsg.getClientId().uuid();
    params["destinationId"] = clientMsg.getDestinationId().uuid();
}

void GridClientJsonMarshaller::fillResponseHeader(const TJson& json, GridClientMessageResult& resp)
    throw (GridClientException) {
    std::string error = json.get("error", "");

    if (!error.empty())
        throw GridClientException(error);

    resp.sessionToken(json.get("sessionToken", ""));

    resp.setStatus(GridClientMessageResult::STATUS_SUCCESS);
}

void GridClientJsonMarshaller::wrap(const GridLogRequestCommand& req, TRequestParams& params) {
    fillRequestHeader(req, params);

    params["cmd"] = "log";

    if (!req.path().empty())
        params["path"] = req.path();

    std::stringstream fss;

    fss << req.from();

    std::string from = fss.str();

    std::stringstream tss;

    tss << req.to();

    params["from"] = fss.str();
    params["to"] = tss.str();
}

void GridClientJsonMarshaller::unwrap(const TJson& json, GridClientMessageLogResult& resp) {
    fillResponseHeader(json, resp);

    if (!resp.isSuccess())
        return;

    std::vector<std::string> lines;

    BOOST_FOREACH(const TJson::value_type &j, json.get_child("response")) {
        lines.push_back(j.second.data());
    }

    resp.lines(lines);
}

void GridClientJsonMarshaller::wrap(const GridAuthenticationRequestCommand& req, TRequestParams& params) {
    fillRequestHeader(req, params);

    params["cmd"] = "top";
    params["attr"] = "false";
    params["mtr"] = "false";
    params["cred"] = req.credentials();
}

void GridClientJsonMarshaller::unwrap(const TJson& json, GridClientMessageAuthenticationResult& resp) {
    fillResponseHeader(json, resp);
}

void GridClientJsonMarshaller::wrap(const GridTopologyRequestCommand& req, TRequestParams& params) {
    fillRequestHeader(req, params);

    params["cmd"] = "top";
    params["attr"] = req.getIncludeAttributes() ? "true" : "false";
    params["mtr"] = req.getIncludeMetrics() ? "true" : "false";
}

void GridClientJsonMarshaller::unwrap(const TJson& json, GridClientMessageTopologyResult& resp) {
    fillResponseHeader(json, resp);

    if (!resp.isSuccess())
        return;

    TNodesList nodeBeans;

	boost::asio::io_service ioSrvc;
    boost::asio::ip::tcp::resolver resolver(ioSrvc);

    BOOST_FOREACH(const TJson::value_type &i, json.get_child("response")) {
        GridClientNode nb;
        GridNodeMarshallerHelper helper(nb);
        TGridClientVariantMap caches;
        std::vector<std::string> tcpAddrs;
        std::vector<std::string> jettyAddrs;

        TJson pt = i.second;

        helper.setNodeId(pt.get<std::string>("nodeId"));
        helper.setConsistentId(pt.get<std::string>("consistentId"));

        std::string defaultCacheMode = pt.get<std::string>("defaultCacheMode");
        helper.setDefaultCacheMode(defaultCacheMode);

        if (!defaultCacheMode.empty())
            caches[""] = defaultCacheMode;

        if (pt.count("jettyPort") > 0)
            helper.setJettyPort(pt.get("jettyPort", -1));

        if (pt.count("tcpPort") > 0)
            helper.setTcpPort(pt.get("tcpPort", -1));

        BOOST_FOREACH(const TJson::value_type &j, pt.get_child("caches"))
            caches[j.first.data()] = j.second.data();

		BOOST_FOREACH(const TJson::value_type &j, pt.get_child("tcpHostNames"))
		{
    		std::string newTCPAddressHostname = j.second.data();
			if (newTCPAddressHostname.size())
			{
				boost::asio::ip::tcp::resolver::query queryHostname(newTCPAddressHostname, boost::lexical_cast<std::string>(pt.get("tcpPort", -1)));
				boost::system::error_code ec;
				boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryHostname,ec);
				if (!ec)
					tcpAddrs.push_back(newTCPAddressHostname);
				else
					GG_LOG_ERROR("Error resolving hostname: %s, %s",newTCPAddressHostname.c_str(), ec.message().c_str());
			}
        }

        BOOST_FOREACH(const TJson::value_type &j, pt.get_child("tcpAddresses"))
        {
        	std::string newTCPAddressIp = j.second.data();

			if (newTCPAddressIp.size())
			{
				boost::asio::ip::tcp::resolver::query queryIp(newTCPAddressIp, boost::lexical_cast<std::string>(pt.get("tcpPort", -1)));
				boost::system::error_code ec;
				boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryIp,ec);
				if (!ec)
					tcpAddrs.push_back(newTCPAddressIp);
				else
					GG_LOG_ERROR("Error resolving hostname: %s, %s",newTCPAddressIp.c_str(), ec.message().c_str());
			}
        }

		BOOST_FOREACH(const TJson::value_type &j, pt.get_child("jettyHostNames"))
		{
    		std::string newJettyAddressHostname = j.second.data();
    		if (newJettyAddressHostname.size())
    		{
        	    boost::asio::ip::tcp::resolver::query queryHostname(newJettyAddressHostname, boost::lexical_cast<std::string>(pt.get("jettyPort", -1)));
        	    boost::system::error_code ec;
       	    	boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryHostname,ec);
       	    	if (!ec)
       	    		jettyAddrs.push_back(newJettyAddressHostname);
       	    	else
       	    		GG_LOG_ERROR("Error resolving hostname: %s, %s",newJettyAddressHostname.c_str(), ec.message().c_str());
    		}
        }

        BOOST_FOREACH(const TJson::value_type &j, pt.get_child("jettyAddresses"))
        {
        	std::string newJettyAddressIp = j.second.data();

    		if (newJettyAddressIp.size())
    		{
        	    boost::asio::ip::tcp::resolver::query queryIp(newJettyAddressIp, boost::lexical_cast<std::string>(pt.get("jettyPort", -1)));
        	    boost::system::error_code ec;
       	    	boost::asio::ip::tcp::resolver::iterator endpoint_iter = resolver.resolve(queryIp,ec);
       	    	if (!ec)
       	    		jettyAddrs.push_back(newJettyAddressIp);
       	    	else
       	    		GG_LOG_ERROR("Error resolving hostname: %s, %s",newJettyAddressIp.c_str(), ec.message().c_str());
    		}
		}

        helper.setCaches(caches);
        helper.setTcpAddresses(tcpAddrs);
        helper.setJettyAddresses(jettyAddrs);

        nodeBeans.push_back(nb);
    }

    resp.setNodes(nodeBeans);
}

std::string checkAndReadKey(const GridClientVariant& k) {
    if (!k.hasAnyValue())
        throw GridClientCommandException("Null keys are not allowed.");

    return k.toString();
}

std::string checkAndReadValue(const GridClientVariant& v) {
    if (!v.hasAnyValue())
        throw GridClientCommandException("Null values are not allowed.");

    return v.toString();
}

void GridClientJsonMarshaller::wrap(const GridCacheRequestCommand& req, TRequestParams& params) {
    fillRequestHeader(req, params);

    std::set<GridClientCacheFlag> flags = req.getFlags();

    if (!flags.empty())
        params["cacheFlags"] = boost::lexical_cast<std::string>(
            GridClientByteUtils::bitwiseOr(flags.begin(), flags.end(), 0));

    if (req.getOperation() == GridCacheRequestCommand::PUT) {
        params["cmd"] = "putall";

        std::string cacheName = req.getCacheName();

        if (!cacheName.empty())
            params["cacheName"] = cacheName;

        params["k1"] = checkAndReadKey(req.getKey());
        params["v1"] = checkAndReadValue(req.getValue());
    }
    else if (req.getOperation() == GridCacheRequestCommand::PUT_ALL) {
        params["cmd"] = "putall";

        std::string cacheName = req.getCacheName();

        if (!cacheName.empty())
            params["cacheName"] = cacheName;

        GridCacheRequestCommand::TKeyValueMap vals = req.getValues();

        int i = 1;

        for (auto iter = vals.begin(); iter != vals.end(); ++iter ) {
            std::stringstream ss;

            ss << i;

            params[std::string("k") + ss.str()] = checkAndReadKey(iter->first);
            params[std::string("v") + ss.str()] = checkAndReadValue(iter->second);

            i++;
        }
    }
    else if (req.getOperation() == GridCacheRequestCommand::GET) {
        params["cmd"] = "get";

        std::string cacheName = req.getCacheName();

        if (!cacheName.empty())
            params["cacheName"] = cacheName;

        params["key"] = checkAndReadKey(req.getKey());
    }
    else if (req.getOperation() == GridCacheRequestCommand::GET_ALL) {
        params["cmd"] = "getall";

        std::string cacheName = req.getCacheName();

        if (!cacheName.empty())
            params["cacheName"] = cacheName;

        GridCacheRequestCommand::TKeyValueMap vals = req.getValues();

        int i = 1;

        for (auto iter = vals.begin(); iter != vals.end(); ++iter ) {
            std::stringstream ss;

            ss << i;

            params[std::string("k") + ss.str()] = checkAndReadKey(iter->first);

            i++;
        }
    }
    else if (req.getOperation() == GridCacheRequestCommand::RMV) {
        params["cmd"] = "rmv";

        std::string cacheName = req.getCacheName();

        if (!cacheName.empty())
            params["cacheName"] = cacheName;

        params["key"] = checkAndReadKey(req.getKey());
    }
    else if (req.getOperation() == GridCacheRequestCommand::RMV_ALL) {
        params["cmd"] = "rmvall";

        std::string cacheName = req.getCacheName();

        if (!cacheName.empty())
            params["cacheName"] = cacheName;

        GridCacheRequestCommand::TKeyValueMap vals = req.getValues();

        int i = 1;

        for (auto iter = vals.begin(); iter != vals.end(); ++iter ) {
            std::stringstream ss;

            ss << i;

            params[std::string("k") + ss.str()] = checkAndReadKey(iter->first);

            i++;
        }
    }
    else if (req.getOperation() == GridCacheRequestCommand::REPLACE) {
        params["cmd"] = "rep";

        std::string cacheName = req.getCacheName();

        if (!cacheName.empty())
            params["cacheName"] = cacheName;

        params["key"] = checkAndReadKey(req.getKey());

        if (req.getValue().hasAnyValue())
            params["val"] = req.getValue().toString();
    }
    else if (req.getOperation() == GridCacheRequestCommand::CAS) {
        params["cmd"] = "cas";

        std::string cacheName = req.getCacheName();

        if (!cacheName.empty())
            params["cacheName"] = cacheName;

        params["key"] = checkAndReadKey(req.getKey());

        if (req.getValue().hasAnyValue())
            params["val1"] = req.getValue().toString();

        if (req.getValue2().hasAnyValue())
            params["val2"] = req.getValue2().toString();
    }
    else if (req.getOperation() == GridCacheRequestCommand::METRICS) {
        params["cmd"] = "cache";

        std::string cacheName = req.getCacheName();

        if (!cacheName.empty())
            params["cacheName"] = cacheName;

        if (req.getKey().hasAnyValue())
            params["key"] = req.getKey().toString();
    }
}

void GridClientJsonMarshaller::unwrap(const TJson& json, GridClientMessageCacheModifyResult& resp) {
    fillResponseHeader(json, resp);

    if (!resp.isSuccess())
        return;

    resp.setOperationResult(json.get<bool>("response"));
}

void GridClientJsonMarshaller::unwrap(const TJson& json, GridClientMessageCacheMetricResult& resp) {
    fillResponseHeader(json, resp);

    if (!resp.isSuccess())
        return;

    TJson res = json.get_child("response");

    TCacheMetrics m;

    m["createTime"] = res.get<int64_t>("createTime");
    m["readTime"] = res.get<int64_t>("readTime");
    m["writeTime"] = res.get<int64_t>("writeTime");

    m["hits"] = res.get<int32_t>("hits");
    m["misses"] = res.get<int32_t>("misses");
    m["reads"] = res.get<int32_t>("reads");
    m["writes"] = res.get<int32_t>("writes");

    resp.setCacheMetrics(m);
}

void GridClientJsonMarshaller::unwrap(const TJson& json, GridClientMessageCacheGetResult& resp) {
    fillResponseHeader(json, resp);

    if (!resp.isSuccess())
        return;

    TCacheValuesMap m;

    BOOST_FOREACH(const TJson::value_type &j, json.get_child("response"))
        m[j.first.data()] = j.second.data();

    if (m.size() == 0 && json.get<std::string>("response") != "null") {
        std::string response = json.get<std::string>("response");

        if (response != "null" && !response.empty())
            m["response"] = response;
    }

    resp.setCacheValues(m);
}

void GridClientJsonMarshaller::wrap(const GridTaskRequestCommand& req, TRequestParams& params) {
    fillRequestHeader(req, params);

    params["cmd"] = "exe";
    params["name"] = req.getTaskName();

    GridClientVariant arg = req.getArg();

    params["p1"] = arg.toString();
}

void GridClientJsonMarshaller::unwrap(const TJson& json, GridClientMessageTaskResult& resp) {
    fillResponseHeader(json, resp);

    if (!resp.isSuccess())
        return;

    resp.setTaskResult(json.get<std::string>("response.result"));
}
