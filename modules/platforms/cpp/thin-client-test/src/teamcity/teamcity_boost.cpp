/* Copyright 2011 JetBrains s.r.o.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * 
 * $Revision: 88625 $
*/

#define BOOST_TEST_MODULE IgniteThinClientTest

#include <sstream>

#include <boost/test/results_collector.hpp>
#include <boost/test/utils/basic_cstring/io.hpp>
#include <boost/test/unit_test_log.hpp>
#include <boost/test/execution_monitor.hpp>
#include <boost/test/unit_test_log_formatter.hpp>
#include <boost/test/unit_test.hpp>

// In 1.59.0, they changed the name of this enum value, so we have to this hacky thing...
#include <boost/version.hpp>
#if BOOST_VERSION >= 105900
    #define TUT_CASE_IDENTIFIER boost::unit_test::TUT_CASE
    #define CURRENT_TEST_NAME   boost::unit_test_framework::framework::current_test_case().full_name()
#else
    #define TUT_CASE_IDENTIFIER boost::unit_test::tut_case
    #define CURRENT_TEST_NAME   boost::unit_test_framework::framework::current_test_case().p_name
#endif

#include "teamcity/teamcity_messages.h"

namespace jetbrains { namespace teamcity {
const std::string ASSERT_CTX = "Assertion occurred in a following context:";
const std::string FAILURE_CTX = "Failure occurred in a following context:";

// Custom formatter for TeamCity messages
class TeamcityBoostLogFormatter: public boost::unit_test::unit_test_log_formatter {
    TeamcityMessages messages;
    std::string currentDetails;
    std::string currentContextDetails;
    std::string flowId;

public:
    TeamcityBoostLogFormatter(const std::string &_flowId);
    TeamcityBoostLogFormatter();

    virtual ~TeamcityBoostLogFormatter() {}

    virtual void log_start(std::ostream&, boost::unit_test::counter_t test_cases_amount);
    virtual void log_finish(std::ostream&);
    virtual void log_build_info(std::ostream&);

    virtual void test_unit_start(std::ostream&, boost::unit_test::test_unit const& tu);
    virtual void test_unit_finish(std::ostream&,
        boost::unit_test::test_unit const& tu,
        unsigned long elapsed);
    virtual void test_unit_skipped(std::ostream&, boost::unit_test::test_unit const& tu);
    virtual void test_unit_skipped(std::ostream&,
        boost::unit_test::test_unit const& tu,
        boost::unit_test::const_string reason);

    virtual void log_exception(std::ostream&,
        boost::unit_test::log_checkpoint_data const&,
        boost::unit_test::const_string explanation);
    virtual void log_exception_start(std::ostream&,
        boost::unit_test::log_checkpoint_data const&,
        const boost::execution_exception&);
    virtual void log_exception_finish(std::ostream&);

    virtual void log_entry_start(std::ostream & out,
        boost::unit_test::log_entry_data const & entry_data,
        log_entry_types let);
    virtual void log_entry_value(std::ostream&, boost::unit_test::const_string value);
    virtual void log_entry_finish(std::ostream&);

    virtual void entry_context_start(std::ostream&, boost::unit_test::log_level);
    virtual void log_entry_context(std::ostream&, boost::unit_test::const_string);
    virtual void entry_context_finish(std::ostream&);

#if BOOST_VERSION >= 106500
     // Since v1.65.0 the log level is passed to the formatters for the contexts
     // See boostorg/test.git:fcb302b66ea09c25f0682588d22fbfdf59eac0f7
     void log_entry_context(std::ostream& os, boost::unit_test::log_level, boost::unit_test::const_string ctx) override {
         log_entry_context(os, ctx);
     }
     void entry_context_finish(std::ostream& os, boost::unit_test::log_level) override {
         entry_context_finish(os);
     }
#endif

#if BOOST_VERSION >= 107000
     // Since v1.70.0 the second argument indicates whether build info should be logged or not
     // See boostorg/test.git:7e20f966dca4e4b49585bbe7654334f31b35b3db
    void log_build_info(std::ostream& os, bool log_build_info) override {
        if (log_build_info) this->log_build_info(os);
    }
#endif
};

// Fake fixture to register formatter
struct TeamcityFormatterRegistrar {
    TeamcityFormatterRegistrar() {
        if (underTeamcity()) {
            boost::unit_test::unit_test_log.set_formatter(new TeamcityBoostLogFormatter());
            boost::unit_test::unit_test_log.set_threshold_level(boost::unit_test::log_test_units);
        }
    }
};

BOOST_GLOBAL_FIXTURE(TeamcityFormatterRegistrar);

// Dummy method used to keep object file in case of static library linking
// See README.md and https://github.com/JetBrains/teamcity-cpp/pull/19
void TeamcityGlobalFixture() {}

TeamcityBoostLogFormatter::TeamcityBoostLogFormatter(const std::string &_flowId)
: flowId(_flowId)
{}

TeamcityBoostLogFormatter::TeamcityBoostLogFormatter()
: flowId(getFlowIdFromEnvironment())
{}

void TeamcityBoostLogFormatter::log_start(std::ostream &/*out*/, boost::unit_test::counter_t /*test_cases_amount*/)
{}

void TeamcityBoostLogFormatter::log_finish(std::ostream &/*out*/)
{}

void TeamcityBoostLogFormatter::log_build_info(std::ostream &/*out*/)
{}

void TeamcityBoostLogFormatter::test_unit_start(std::ostream &out, boost::unit_test::test_unit const& tu) {
    messages.setOutput(out);

    if (tu.p_type == TUT_CASE_IDENTIFIER) {
        messages.testStarted(tu.p_name, flowId);
    } else {
        messages.suiteStarted(tu.p_name, flowId);
    }

    currentDetails.clear();
}

void TeamcityBoostLogFormatter::test_unit_finish(std::ostream &out, boost::unit_test::test_unit const& tu, unsigned long elapsed) {
    messages.setOutput(out);

    boost::unit_test::test_results const& tr = boost::unit_test::results_collector.results(tu.p_id);
    if (tu.p_type == TUT_CASE_IDENTIFIER) {
        if(!tr.passed()) {
            if(tr.p_skipped) {
                messages.testIgnored(tu.p_name, "ignored", flowId);
            } else if (tr.p_aborted) {
                messages.testFailed(tu.p_name, "aborted", currentDetails, flowId);
            } else {
                messages.testFailed(tu.p_name, "failed", currentDetails, flowId);
            }
        }

        messages.testFinished(tu.p_name, elapsed / 1000, flowId);
    } else {
        messages.suiteFinished(tu.p_name, flowId);
    }
}

void TeamcityBoostLogFormatter::test_unit_skipped(std::ostream &/*out*/, boost::unit_test::test_unit const& /*tu*/)
{}

void TeamcityBoostLogFormatter::test_unit_skipped(std::ostream &out, boost::unit_test::test_unit const& tu, boost::unit_test::const_string reason)
{
    messages.testIgnored(tu.p_name, std::string(reason.begin(), reason.end()), flowId);
}

void TeamcityBoostLogFormatter::log_exception(std::ostream &out, boost::unit_test::log_checkpoint_data const &, boost::unit_test::const_string explanation) {
    out << explanation << std::endl;
    currentDetails.append(explanation.begin(), explanation.end());
    currentDetails += "\n";
}

void TeamcityBoostLogFormatter::log_exception_start(std::ostream &out, boost::unit_test::log_checkpoint_data const &data, const boost::execution_exception& excpt) {
    log_exception(out, data, excpt.what());
}

void TeamcityBoostLogFormatter::log_exception_finish(std::ostream &/*out*/) {}


void TeamcityBoostLogFormatter::log_entry_start(std::ostream & out, boost::unit_test::log_entry_data const & entry_data, log_entry_types /*let*/)
{
    std::stringstream ss(std::ios_base::out);

    out << entry_data.m_file_name << "(" << entry_data.m_line_num << "): ";
    ss  << entry_data.m_file_name << "(" << entry_data.m_line_num << "): ";

    currentDetails += ss.str();
}

void TeamcityBoostLogFormatter::log_entry_value(std::ostream &out, boost::unit_test::const_string value) {
    out << value;
    currentDetails.append(value.begin(), value.end());
}

void TeamcityBoostLogFormatter::log_entry_finish(std::ostream &out) {
    out << std::endl;
    currentDetails += "\n";
}

void TeamcityBoostLogFormatter::entry_context_start(std::ostream &out, boost::unit_test::log_level l) {
    const std::string& initial_msg = (l == boost::unit_test::log_successful_tests ? ASSERT_CTX : FAILURE_CTX);
    out << initial_msg;
    currentContextDetails = initial_msg;
}

void TeamcityBoostLogFormatter::log_entry_context(std::ostream &out, boost::unit_test::const_string ctx) {
    out << "\n " << ctx;
    currentContextDetails += "\n ";
    currentContextDetails.append(ctx.begin(), ctx.end());
}

void TeamcityBoostLogFormatter::entry_context_finish(std::ostream &out) {
    out.flush();
    messages.testOutput(CURRENT_TEST_NAME, currentContextDetails, flowId, TeamcityMessages::StdErr);
}

}}                                                          // namespace teamcity, jetbrains
