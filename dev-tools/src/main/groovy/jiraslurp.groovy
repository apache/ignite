/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * Parsing a special filter from Apache Ignite JIRA and picking up latest by ID
 * attachments to process.
 */
final GIT_REPO = "https://git1-us-west.apache.org/repos/asf/incubator-ignite.git"
final ATTACHMENT_URL = "https://issues.apache.org/jira/secure/attachment"
final validated_filename = "${System.getProperty("user.home")}/validated-jira.txt"
final LAST_SUCCESSFUL_ARTIFACT = "guestAuth/repository/download/Ignite_PatchValidation_PatchChecker/.lastSuccessful/$validated_filename"

final def JIRA_CMD = System.getProperty('JIRA_COMMAND', 'jira.sh')
LinkedHashMap<String, String> jirasAttached = [:]

/**
 * Gets jiras for which test tasks were already triggered.
 *
 * @return List of pairs [jira,attachemntId].
 */
def readHistory = {
    final int MAX_HISTORY = 5000

    List validated_list = []

    // TODO do not use folder.
    def validated = new File(validated_filename)

    if (validated.exists()) {
        // TODO use commented way.
        validated_list = validated.text.split('\n')
    }

    // TODO use it way.
//    try {
//        def historyUrl = "http://${System.getenv('TC_URL')}/$LAST_SUCCESSFUL_ARTIFACT"
//
//        println "Reading history from $historyUrl"
//
//        validated_list = new URL(historyUrl).text.split('\n')
//
//        println "Got validated list=$validated_list"
//    }
//    catch (Exception e) {
//        println e.getMessage()
//
//    }

    // Let's make sure the preserved history isn't too long
    if (validated_list.size > MAX_HISTORY)
        validated_list = validated_list[validated_list.size - MAX_HISTORY..validated_list.size - 1]

    println "History=$validated_list"

    validated_list
}

/**
 * Accepting the <jira> XML element from JIRA stream
 * @return <code>null</code> or <code>JIRA-###,latest_attach_id</code>
 */
def getLatestAttachment = { jira ->
    def latestAttr = jira.attachments[0].attachment.list().sort {
        it.@id.toInteger()
    }.reverse()[0]

    String row = null

    if (latestAttr == null) {
        println "${jira.key} is in invalid state: patch is not available"
    }
    else {
        row = "${jira.key},${latestAttr.@id}"
    }
}

def checkForAttachments = {
    def JIRA_FILTER =
        "https://issues.apache.org/jira/sr/jira.issueviews:searchrequest-xml/12330308/SearchRequest-12330308.xml?tempMax=100&field=key&field=attachments"
    def rss = new XmlSlurper().parse(JIRA_FILTER)

    List list = readHistory {}

    rss.channel.item.each { jira ->
        String row = getLatestAttachment(jira)

        if (row != null && !list.contains(row)) {
            def pair = row.split(',')

            jirasAttached.put(pair[0] as String, pair[1] as String)

            list.add(row)
        }
    }

    // Write everything back to persist the list
    def validated = new File(validated_filename)

    if (validated.exists())
        validated.delete()

    validated << list.join('\n')
}

/**
 * Monitors given process and show errors if exist.
 */
def checkprocess = { process ->
    println process.text

    process.waitFor()

    if (process.exitValue() != 0) {
        println "Return code: " + process.exitValue()
        println "Errout:\n" + process.err.text

        assert process.exitValue() == 0 || process.exitValue() == 128
    }
}

/**
 * Applys patch from jira to given git state.
 */
def applyPatch = { jira, attachementURL ->
    def userEmail = System.getenv("env.GIT_USER_EMAIL");
    def userName = System.getenv("env.GIT_USER_NAME");

    println "Patch apllying with jira='$jira' and attachment='$ATTACHMENT_URL/$attachementURL/'."

    def patchFile = new File("${jira}-${attachementURL}.patch")

    try {
        patchFile << new URL("$ATTACHMENT_URL/$attachementURL/").text

        try {
            checkprocess "git branch".execute()

            checkprocess "git config user.email \"$userEmail\"".execute(null, new File("../"))
            checkprocess "git config user.name \"$userName\"".execute(null, new File("../"))

            // Create a new uniqueue branch to applying patch
            def newTestBranch = "test-branch-${jira}-${attachementURL}-${System.currentTimeMillis()}"
            checkprocess "git checkout -b ${newTestBranch}".execute(null, new File("../"))

            checkprocess "git branch".execute()

            checkprocess "git am dev-tools/${patchFile.name}".execute(null, new File("../"))

            println "Patch was applied successfully."
        }
        catch (Exception e) {
            println "Patch was not applied successfully. Aborting patch applying."

            checkprocess "git am --abort".execute(null, new File("../"))

            throw e;
        }
    }
    finally {
        assert patchFile.delete(), 'Could not delete patch file.'
    }
}

def JIRA_xml = { jiranum ->
    "https://issues.apache.org/jira/si/jira.issueviews:issue-xml/$jiranum/${jiranum}.xml"
}

/**
 * Runs all given test builds to validate last patch from given jira.
 */
def runAllTestBuilds = { builds, jiraNum ->
    assert jiraNum != 'null', 'Jira number should not be null.'
    assert jiraNum != null, 'Jira number should not be null.'

    if (jiraNum) {
        def tcURL = System.getenv('TC_URL')
        def user = System.getenv('TASK_RUNNER_USER')
        def pwd = System.getenv('TASK_RUNNER_PWD')

        builds.each {
            try {
                println "Triggering $it build for $jiraNum jira..."

                String postData =
                    "<build>" +
                        "  <buildType id='$it'/>" +
                        "  <properties>" +
                        "    <property name='JIRA_NUM' value='$jiraNum'/>" +
                        "  </properties>" +
                        "</build>";

                URL url = new URL("http://$tcURL:80/httpAuth/app/rest/buildQueue");

                HttpURLConnection conn = (HttpURLConnection)url.openConnection();

                String encoded = new sun.misc.BASE64Encoder().encode("$user:$pwd".getBytes());

                conn.setRequestProperty("Authorization", "Basic " + encoded);

                conn.setDoOutput(true);
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/xml");
                conn.setRequestProperty("Content-Length", String.valueOf(postData.length()));

                OutputStream os = conn.getOutputStream();
                os.write(postData.getBytes());
                os.flush();
                os.close();

                conn.connect();

                // Read response.
                print "Response: "

                BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));

                String line;
                while ((line = br.readLine()) != null)
                    println line

                br.close();
            }
            catch (Exception e) {
                e.printStackTrace()
            }
        }
    }
}

/**
 * Main.
 */
args.each {
    println "Arg=$it"

    def parameters = it.split(",")

    println parameters

    if (parameters.length == 2 && parameters[0] == "slurp" && parameters[1] != 'null') {
        def builds = parameters[1].split(' ');

        println "Running in 'slurp' mode. Test builds=${builds}"

        checkForAttachments()

        // For each ticket with new attachment, let's trigger remove build
        jirasAttached.each { k, v ->
            //  Trailing slash is important for download; only need to pass JIRA number
            println "Triggering the test builds for: $k = $ATTACHMENT_URL/$v/"

            runAllTestBuilds(builds,k)
        }
    }
    else if (parameters.length == 2 && parameters[0] == "patchApply" && parameters[1] ==~ /\w+-\d+/) {
        def jiraNum = parameters[1]

        println "Running in 'patch apply' mode with jira number '$jiraNum'"

        // Extract JIRA rss from the and pass the ticket element into attachment extraction
        def rss = new XmlSlurper().parse(JIRA_xml(parameters[1]))

        String row = getLatestAttachment(rss.channel.item)

        println "Got row: $row."

        if (row != null) {
            def pair = row.split(',')
            def jira = pair[0]
            def attachementURL = pair[1]

            applyPatch(jira, attachementURL)
        }
    }
    else if (parameters.length >= 2 && parameters[0] == "runAllBuilds" && parameters[1] ==~ /\w+-\d+/) {
        def jiraNum = parameters[1]

        def attachementURL=null

        if (parameters[2] ==~ /\d+/)
            attachementURL = parameters[2]

        println "Running in 'all builds' mode with jira number='$jiraNum' and attachment URL='$attachementURL'."

        runAllTestBuilds jiraNum attachmentURL
    }
}

/* Workflow:
  1. download an attachment if JIRA num's set; otherwise get all latest attachments not mentioned in the
     validated-jira.txt file from the last successful build
  2. trigger test build(s) parametrised by JIRA no.
  3. test build will download JIRA's latest attachment and apply it to currently checked out repo;
     - build will fail with comment on JIRA if that can not apply
     - build will post error/success comment depends on the test results
*/
// TODO
//   - TC's test job needs to send a comment to JIRA
//       $JIRA_CMD -a addComment -s https://issues.apache.org/jira -u ignite-ci -p ci-of-1gnit3 --issue IGNITE-495 --comment "Trying latest version of the jira-cli"
