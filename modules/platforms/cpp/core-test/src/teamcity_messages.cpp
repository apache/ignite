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

#include "teamcity_messages.h"

#include <cstdlib>
#include <iostream>
#include <sstream>

namespace jetbrains { namespace teamcity { namespace {
/// Use RAII to write message open/close markers
class RaiiMessage
{
public:
    RaiiMessage(const char* const name, std::ostream& out)
      : m_out(out)
    {
        // endl for http://jetbrains.net/tracker/issue/TW-4412
        m_out << std::endl << "##teamcity[" << name;
    }
    ~RaiiMessage()
    {
        // endl for http://jetbrains.net/tracker/issue/TW-4412
        m_out << "]" << std::endl;
    }
    /// \todo Copying char-by-char is ineffective!
    std::string escape(const std::string &s)
    {
        std::string result;
        result.reserve(s.length());

        for (size_t i = 0; i < s.length(); i++)
        {
            char c = s[i];

            switch (c)
            {
            case '\n': result.append("|n"); break;
            case '\r': result.append("|r"); break;
            case '\'': result.append("|'"); break;
            case '|':  result.append("||"); break;
            case ']':  result.append("|]"); break;
            default:   result.append(&c, 1);
            }
        }

        return result;
    }

    void writeProperty(const std::string& name, const std::string& value)
    {
        m_out << ' ' << name << "='" << escape(value) << '\'';
    }

    void writePropertyIfNonEmpty(const char* const name, const std::string value)
    {
        if (!value.empty())
            writeProperty(name, value);
    }

private:
    std::ostream& m_out;
};
}                                                           // anonymous namespace

//BEGIN Public helper functions
std::string getFlowIdFromEnvironment()
{
#if (defined(WIN32) || defined(_WIN32) || defined(__WIN32)) && !defined(__CYGWIN__) && !defined(__MINGW32__)
    char* flowId = 0;
    size_t sz = 0;
    std::string result;
    if (!_dupenv_s(&flowId, &sz, "TEAMCITY_PROCESS_FLOW_ID"))
    {
        result = flowId != 0 ? flowId : std::string();
        free(flowId);
    }

    return result;
#else
    const char *flowId = getenv("TEAMCITY_PROCESS_FLOW_ID");
    return flowId == 0 ? std::string() : flowId;
#endif
}

bool underTeamcity()
{
#if (defined(WIN32) || defined(_WIN32) || defined(__WIN32)) && !defined(__CYGWIN__) && !defined(__MINGW32__)
    char* teamCityProjectName = 0;
    size_t sz = 0;
    bool result = false;
    if (!_dupenv_s(&teamCityProjectName, &sz, "TEAMCITY_PROJECT_NAME"))
    {
        result = teamCityProjectName != 0;
        free(teamCityProjectName);
    }

    return result;
#else
    return getenv("TEAMCITY_PROJECT_NAME") != 0;
#endif
}
//END Public helper functions

//BEGIN TeamcityMessages members

TeamcityMessages::TeamcityMessages()
  : m_out(&std::cout)
{}

void TeamcityMessages::setOutput(std::ostream& out)
{
    m_out = &out;
}

void TeamcityMessages::suiteStarted(const std::string& name, const std::string& flowId)
{
    RaiiMessage msg("testSuiteStarted", *m_out);
    msg.writeProperty("name", name);
    msg.writePropertyIfNonEmpty("flowId", flowId);
}

void TeamcityMessages::suiteFinished(const std::string& name, const std::string& flowId)
{
    RaiiMessage msg("testSuiteFinished", *m_out);
    msg.writeProperty("name", name);
    msg.writePropertyIfNonEmpty("flowId", flowId);
}

void TeamcityMessages::testStarted(const std::string& name, const std::string& flowId, const bool captureStandardOutput)
{
    RaiiMessage msg("testStarted", *m_out);
    msg.writeProperty("name", name);
    msg.writePropertyIfNonEmpty("flowId", flowId);

    if (captureStandardOutput)
        msg.writeProperty("captureStandardOutput", "true"); // false by default
}

void TeamcityMessages::testFinished(const std::string& name, const int durationMs, const std::string& flowId)
{
    RaiiMessage msg("testFinished", *m_out);

    msg.writeProperty("name", name);
    msg.writePropertyIfNonEmpty("flowId", flowId);

    if (durationMs >= 0)
    {
        /// \bug W/ some locales it is possible to get a number w/ a number separator(s)!
        /// \todo Make a test for that!
        std::stringstream out(std::ios_base::out);
        out << durationMs;
        msg.writeProperty("duration", out.str());
    }
}

void TeamcityMessages::testFailed(const std::string& name, const std::string& message, const std::string& details, const std::string& flowId)
{
    RaiiMessage msg("testFailed", *m_out);
    msg.writeProperty("name", name);
    msg.writeProperty("message", message);
    msg.writeProperty("details", details);
    msg.writePropertyIfNonEmpty("flowId", flowId);
}

void TeamcityMessages::testIgnored(const std::string& name, const std::string& message, const std::string& flowId)
{
    RaiiMessage msg("testIgnored", *m_out);
    msg.writeProperty("name", name);
    msg.writeProperty("message", message);
    msg.writePropertyIfNonEmpty("flowId", flowId);
}

void TeamcityMessages::testOutput(const std::string& name, const std::string& output, const std::string& flowId, const bool isStdError)
{
    RaiiMessage msg(isStdError ? "testStdErr" : "testStdOut", *m_out);
    msg.writeProperty("name", name);
    msg.writeProperty("out", output);
    msg.writePropertyIfNonEmpty("flowId", flowId);
}

//END TeamcityMessages members
}}                                                          // namespace teamcity, jetbrains
