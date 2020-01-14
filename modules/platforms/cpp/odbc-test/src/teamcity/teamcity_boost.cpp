/* Copyright 2011 JetBrains s.r.o.
 * Copyright 2015-2017 Alex Turbov <i.zaufi@gmail.com>
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

#define BOOST_TEST_MODULE IgniteOdbcTest

#include "teamcity/teamcity_messages.h"

#include <boost/version.hpp>
#if BOOST_VERSION >= 105900
# include <boost/test/execution_monitor.hpp>
#else                                                       // BOOST_VERSION < 105900
# include <boost/test/unit_test_suite_impl.hpp>
#endif                                                      // BOOST_VERSION < 105900
#include <boost/test/results_collector.hpp>
#include <boost/test/utils/basic_cstring/io.hpp>
#include <boost/test/unit_test_log.hpp>
#include <boost/test/unit_test_log_formatter.hpp>
#include <boost/test/unit_test.hpp>

#include <sstream>

namespace jetbrains { namespace teamcity { namespace {
const std::string ASSERT_CTX = "Assertion has occurred in a following context:";
const std::string FAILURE_CTX = "Failure has occurred in a following context:";
const boost::unit_test::test_unit_type UNIT_TEST_CASE =
#if BOOST_VERSION < 105900
    boost::unit_test::tut_case
#else                                                       // BOOST_VERSION >= 105900
    boost::unit_test::TUT_CASE
#endif                                                      // BOOST_VERSION >= 105900
  ;

// Formatter implementation
std::string toString(boost::unit_test::const_string bstr)
{
    std::stringstream ss(std::ios_base::out);
    ss << bstr;
    return ss.str();
}

std::string toString(const boost::execution_exception& excpt)
{
    std::stringstream ss(std::ios_base::out);
    ss << excpt.what();
    return ss.str();
}
}                                                           // anonymous namespace

/// Custom formatter for TeamCity messages
class TeamcityBoostLogFormatter : public boost::unit_test::unit_test_log_formatter
{
    TeamcityMessages messages;
    std::string currentDetails;
    std::string flowId;
#if BOOST_VERSION >= 105900
    std::string currentContextDetails;
#endif                                                      // BOOST_VERSION >= 105900

public:
    TeamcityBoostLogFormatter(const std::string& flowId);
    TeamcityBoostLogFormatter();

    virtual ~TeamcityBoostLogFormatter() {}

    virtual void log_start(std::ostream&, boost::unit_test::counter_t);
    virtual void log_finish(std::ostream&);
    virtual void log_build_info(std::ostream&);

    virtual void test_unit_start(std::ostream&, const boost::unit_test::test_unit&);
    virtual void test_unit_finish(std::ostream&, const boost::unit_test::test_unit&, unsigned long);

    virtual void log_entry_start(std::ostream&, const boost::unit_test::log_entry_data&, log_entry_types);
    virtual void log_entry_value(std::ostream&, boost::unit_test::const_string);
    virtual void log_entry_finish(std::ostream&);

#if BOOST_VERSION < 105900
    virtual void log_exception(std::ostream&, const boost::unit_test::log_checkpoint_data&, boost::unit_test::const_string);
    virtual void test_unit_skipped(std::ostream&, const boost::unit_test::test_unit&);
#else                                                       // BOOST_VERSION >= 105900
    virtual void log_exception_start(std::ostream&,const boost::unit_test::log_checkpoint_data&, const boost::execution_exception&);
    virtual void test_unit_skipped(std::ostream&, const boost::unit_test::test_unit&, boost::unit_test::const_string);

    virtual void log_exception_finish(std::ostream&);
    virtual void entry_context_start(std::ostream&, boost::unit_test::log_level);
# if BOOST_VERSION < 106500
    virtual void log_entry_context(std::ostream&, boost::unit_test::const_string);
    virtual void entry_context_finish(std::ostream&);
# else                                                      // BOOST_VERSION >= 106500
    virtual void log_entry_context(std::ostream&, boost::unit_test::log_level, boost::unit_test::const_string);
    virtual void entry_context_finish(std::ostream&, boost::unit_test::log_level);
# endif                                                     // BOOST_VERSION >= 106500
#endif                                                      // BOOST_VERSION >= 105900
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

#if BOOST_VERSION < 106500
    BOOST_GLOBAL_FIXTURE(TeamcityFormatterRegistrar);
#else                                                       // BOOST_VERSION >= 106500
    BOOST_TEST_GLOBAL_CONFIGURATION(TeamcityFormatterRegistrar);
#endif                                                      // BOOST_VERSION >= 106500

TeamcityBoostLogFormatter::TeamcityBoostLogFormatter(const std::string& id)
  : flowId(id)
{}

TeamcityBoostLogFormatter::TeamcityBoostLogFormatter()
  : flowId(getFlowIdFromEnvironment())
{}

void TeamcityBoostLogFormatter::log_start(std::ostream& out, boost::unit_test::counter_t /*test_cases_amount*/)
{
    messages.setOutput(out);
}

void TeamcityBoostLogFormatter::log_finish(std::ostream& /*out*/)
{}

void TeamcityBoostLogFormatter::log_build_info(std::ostream& /*out*/)
{}

void TeamcityBoostLogFormatter::test_unit_start(std::ostream& /*out*/, const boost::unit_test::test_unit& tu)
{
    if (tu.p_type == UNIT_TEST_CASE)
        messages.testStarted(tu.p_name, flowId);
    else
        messages.suiteStarted(tu.p_name, flowId);

    currentDetails.clear();
}

void TeamcityBoostLogFormatter::test_unit_finish(std::ostream& /*out*/, const boost::unit_test::test_unit& tu, unsigned long elapsed)
{
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
    out << '\n';
    currentDetails += '\n';
}

#if BOOST_VERSION < 105900

void TeamcityBoostLogFormatter::log_exception(
    std::ostream& out
  , const boost::unit_test::log_checkpoint_data&
  , boost::unit_test::const_string explanation
  )
{
    out << explanation << '\n';
    currentDetails += toString(explanation) + '\n';
}

void TeamcityBoostLogFormatter::test_unit_skipped(std::ostream& /*out*/, const boost::unit_test::test_unit& tu)
{
    messages.testIgnored(tu.p_name, "test ignored", flowId);
}

#else                                                       // BOOST_VERSION >= 105900

void TeamcityBoostLogFormatter::log_exception_start(
    std::ostream& out
  , const boost::unit_test::log_checkpoint_data&
  , const boost::execution_exception& excpt
  )
{
    const std::string what = toString(excpt);

    out << what << '\n';
    currentDetails += what + '\n';
}

void TeamcityBoostLogFormatter::test_unit_skipped(
    std::ostream& /*out*/
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

# if BOOST_VERSION < 106500
void TeamcityBoostLogFormatter::log_entry_context(std::ostream& out, boost::unit_test::const_string ctx)
# else                                                      // BOOST_VERSION >= 106500
void TeamcityBoostLogFormatter::log_entry_context(std::ostream& out, boost::unit_test::log_level, boost::unit_test::const_string ctx)
# endif                                                     // BOOST_VERSION >= 106500
{
    out << "\n " << ctx;
    currentContextDetails += "\n " + toString(ctx);
}

# if BOOST_VERSION < 106500
void TeamcityBoostLogFormatter::entry_context_finish(std::ostream& out)
# else                                                      // BOOST_VERSION >= 106500
void TeamcityBoostLogFormatter::entry_context_finish(std::ostream& out, boost::unit_test::log_level)
# endif                                                     // BOOST_VERSION >= 106500
{
    out.flush();
    messages.testOutput(
        boost::unit_test_framework::framework::current_test_case().full_name()
      , currentContextDetails
      , flowId
      , TeamcityMessages::StdErr
      );
}

#endif                                                      // BOOST_VERSION >= 105900
}}                                                          // namespace teamcity, jetbrains
