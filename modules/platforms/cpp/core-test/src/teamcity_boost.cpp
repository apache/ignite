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

#define BOOST_TEST_MODULE IgniteCoreTest

#include "teamcity_messages.h"

#include <boost/version.hpp>
#include <boost/test/execution_monitor.hpp>
#include <boost/test/results_collector.hpp>
#include <boost/test/utils/basic_cstring/io.hpp>
#include <boost/test/unit_test_log.hpp>
#include <boost/test/unit_test_log_formatter.hpp>
#include <boost/test/unit_test.hpp>

#include <sstream>

namespace jetbrains { namespace teamcity { namespace {
const std::string ASSERT_CTX = "Assertion occurred in a following context:";
const std::string FAILURE_CTX = "Failure occurred in a following context:";

// In 1.59.0, they changed the name of this enum value, so we have to this hacky thing...
#if BOOST_VERSION >= 105900
    #define UNIT_TEST_CASE boost::unit_test::TUT_CASE
    #define CURRENT_TEST_NAME   boost::unit_test_framework::framework::current_test_case().full_name()
#else
    #define UNIT_TEST_CASE boost::unit_test::tut_case
    #define CURRENT_TEST_NAME   boost::unit_test_framework::framework::current_test_case().p_name
#endif

// Formatter implementation
std::string toString(boost::unit_test::const_string bstr)
{
    std::stringstream ss(std::ios_base::out);
    ss << bstr;
    return ss.str();
}
}                                                           // anonymous namespace

/// Custom formatter for TeamCity messages
class TeamcityBoostLogFormatter : public boost::unit_test::unit_test_log_formatter
{
    TeamcityMessages messages;
    std::string currentDetails;
    std::string flowId;
    std::string currentContextDetails;

public:
    TeamcityBoostLogFormatter(const std::string& flowId);
    TeamcityBoostLogFormatter();

    virtual ~TeamcityBoostLogFormatter() {}

    virtual void log_start(std::ostream&, boost::unit_test::counter_t);
    virtual void log_finish(std::ostream&);
    virtual void log_build_info(std::ostream&);

#if BOOST_VERSION >= 107000
    virtual void log_build_info(std::ostream&, bool);
#endif

    virtual void test_unit_start(std::ostream&, const boost::unit_test::test_unit&);
    virtual void test_unit_finish(std::ostream&, const boost::unit_test::test_unit&, unsigned long);

    virtual void log_entry_start(std::ostream&, const boost::unit_test::log_entry_data&, log_entry_types);
    virtual void log_entry_value(std::ostream&, boost::unit_test::const_string);
    virtual void log_entry_finish(std::ostream&);


    virtual void log_exception(std::ostream&, const boost::unit_test::log_checkpoint_data&, boost::unit_test::const_string);
    virtual void test_unit_skipped(std::ostream&, const boost::unit_test::test_unit&);

    virtual void log_exception_start(std::ostream&,const boost::unit_test::log_checkpoint_data&, const boost::execution_exception&);
    virtual void test_unit_skipped(std::ostream&, const boost::unit_test::test_unit&, boost::unit_test::const_string);

    virtual void log_exception_finish(std::ostream&);
    virtual void entry_context_start(std::ostream&, boost::unit_test::log_level);

    virtual void log_entry_context(std::ostream&, boost::unit_test::const_string);
    virtual void entry_context_finish(std::ostream&);

#if BOOST_VERSION >= 106500
    // Since v1.65.0 the log level is passed to the formatters for the contexts
    // See boostorg/test.git:fcb302b66ea09c25f0682588d22fbfdf59eac0f7
    virtual void log_entry_context(std::ostream& os, boost::unit_test::log_level, boost::unit_test::const_string ctx) {
        log_entry_context(os, ctx);
    }
    virtual void entry_context_finish(std::ostream& os, boost::unit_test::log_level) {
        entry_context_finish(os);
    }
#endif
};

// Fake fixture to register formatter
struct TeamcityFormatterRegistrar
{
    TeamcityFormatterRegistrar()
    {
        if (underTeamcity())
        {
            boost::unit_test::unit_test_log.set_formatter(new TeamcityBoostLogFormatter());
            boost::unit_test::unit_test_log.set_threshold_level(boost::unit_test::log_test_units);
        }
    }
};

BOOST_GLOBAL_FIXTURE(TeamcityFormatterRegistrar);

// Dummy method used to keep object file in case of static library linking
// See README.md and https://github.com/JetBrains/teamcity-cpp/pull/19
void TeamcityGlobalFixture() {}

TeamcityBoostLogFormatter::TeamcityBoostLogFormatter(const std::string& id)
  : flowId(id)
{}

TeamcityBoostLogFormatter::TeamcityBoostLogFormatter()
  : flowId(getFlowIdFromEnvironment())
{}

void TeamcityBoostLogFormatter::log_start(std::ostream& /*out*/, boost::unit_test::counter_t /*test_cases_amount*/)
{}

void TeamcityBoostLogFormatter::log_finish(std::ostream& /*out*/)
{}

void TeamcityBoostLogFormatter::log_build_info(std::ostream& /*out*/)
{}

#if BOOST_VERSION >= 107000
 // Since v1.70.0 the second argument indicates whether build info should be logged or not
 // See boostorg/test.git:7e20f966dca4e4b49585bbe7654334f31b35b3db
void TeamcityBoostLogFormatter::log_build_info(std::ostream& os, bool log_build_info) {
    if (log_build_info) this->log_build_info(os);
}
#endif

void TeamcityBoostLogFormatter::test_unit_start(std::ostream& out, const boost::unit_test::test_unit& tu)
{
    messages.setOutput(out);

    if (tu.p_type == UNIT_TEST_CASE)
        messages.testStarted(tu.p_name, flowId);
    else
        messages.suiteStarted(tu.p_name, flowId);

    currentDetails.clear();
}

void TeamcityBoostLogFormatter::test_unit_finish(std::ostream& out, const boost::unit_test::test_unit& tu, unsigned long elapsed)
{
    messages.setOutput(out);

    const boost::unit_test::test_results& tr = boost::unit_test::results_collector.results(tu.p_id);
    if (tu.p_type == UNIT_TEST_CASE)
    {
        if (!tr.passed())
        {
            if (tr.p_skipped)
                messages.testIgnored(tu.p_name, "ignored", flowId);
            else if (tr.p_aborted)
                messages.testFailed(tu.p_name, "aborted", currentDetails, flowId);
            else
                messages.testFailed(tu.p_name, "failed", currentDetails, flowId);
        }

        messages.testFinished(tu.p_name, elapsed / 1000, flowId);
    }
    else
    {
        messages.suiteFinished(tu.p_name, flowId);
    }
}

void TeamcityBoostLogFormatter::log_entry_start(std::ostream& out, const boost::unit_test::log_entry_data& entry_data, log_entry_types /*let*/)
{
    std::stringstream ss(std::ios_base::out);

    out << entry_data.m_file_name << "(" << entry_data.m_line_num << "): ";
    ss  << entry_data.m_file_name << "(" << entry_data.m_line_num << "): ";

    currentDetails += ss.str();
}

void TeamcityBoostLogFormatter::log_entry_value(std::ostream& out, boost::unit_test::const_string value)
{
    out << value;
    currentDetails += toString(value);
}

void TeamcityBoostLogFormatter::log_entry_finish(std::ostream& out)
{
    out << std::endl;
    currentDetails += '\n';
}


void TeamcityBoostLogFormatter::log_exception(
    std::ostream& out
  , const boost::unit_test::log_checkpoint_data&
  , boost::unit_test::const_string explanation
  )
{
    out << explanation << std::endl;
    currentDetails += toString(explanation) + '\n';
}

void TeamcityBoostLogFormatter::test_unit_skipped(std::ostream& /*out*/, const boost::unit_test::test_unit& /*tu*/)
{}

void TeamcityBoostLogFormatter::log_exception_start(
    std::ostream& out
  , const boost::unit_test::log_checkpoint_data& data
  , const boost::execution_exception& excpt
  )
{
    log_exception(out, data, excpt.what());
}

void TeamcityBoostLogFormatter::test_unit_skipped(
    std::ostream&
  , const boost::unit_test::test_unit& tu
  , boost::unit_test::const_string reason
  )
{
    messages.testIgnored(tu.p_name, toString(reason), flowId);
}

void TeamcityBoostLogFormatter::log_exception_finish(std::ostream& /*out*/)
{}

void TeamcityBoostLogFormatter::entry_context_start(std::ostream& out, boost::unit_test::log_level l)
{
    const std::string& initial_msg = (l == boost::unit_test::log_successful_tests ? ASSERT_CTX : FAILURE_CTX);
    out << initial_msg;
    currentContextDetails = initial_msg;
}

void TeamcityBoostLogFormatter::log_entry_context(std::ostream& out, boost::unit_test::const_string ctx)
{
    out << "\n " << ctx;
    currentContextDetails += "\n " + toString(ctx);
}

void TeamcityBoostLogFormatter::entry_context_finish(std::ostream& out)
{
    out.flush();
    messages.testOutput(
        CURRENT_TEST_NAME
      , currentContextDetails
      , flowId
      , TeamcityMessages::StdErr
      );
}

}}                                                          // namespace teamcity, jetbrains
