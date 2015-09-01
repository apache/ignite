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

/*
 * Start of util methods.
 */

def envVariable = { name, defaultVar ->
    def res = System.getenv(name as String)

    if (res == 'null' || res == null)
        return defaultVar

    res
}

def envVariableAsList = { name, defaultList ->
    def list = System.getenv(name as String)?.split(' ') as List

    if (list == 'null' || list == null)
        return defaultList

    list
}

/**
 * Monitors given process and show errors if exist.
 */
def checkprocess = { process ->
    process.waitFor()

    if (process.exitValue() != 0) {
        println "Return code: " + process.exitValue()
//        println "Errout:\n" + process.err.text

        assert process.exitValue() == 0 || process.exitValue() == 128
    }
}

def exec = {command, envp, dir ->
    println "Executing command '$command'..."

    def ps = command.execute(envp, dir)

    try {
        println "Command output:"

        println ps.text
    }
    catch (Throwable e) {
        // Do nothing.
        println "Error: could not get caommand output."
    }

    checkprocess ps
}

def execGit = {command ->
    exec(command, null, new File("../"))
}

/**
 * Util method to send http request.
 */
def sendHttpRequest = { requestMethod, urlString, user, pwd, postData, contentType ->
    URL url = new URL(urlString as String);

    HttpURLConnection conn = (HttpURLConnection)url.openConnection();

    String encoded = new sun.misc.BASE64Encoder().encode("$user:$pwd".getBytes());

    conn.setRequestProperty("Authorization", "Basic " + encoded);

    conn.setDoOutput(true);
    conn.setRequestMethod(requestMethod);

    if (postData) {
        conn.setRequestProperty("Content-Type", contentType);
        conn.setRequestProperty("Content-Length", String.valueOf(postData.length()));

        OutputStream os = conn.getOutputStream();
        os.write(postData.getBytes());
        os.flush();
        os.close();
    }

    conn.connect();

    // Read response.
    BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));

    String response = "";
    String line;

    while ((line = br.readLine()) != null)
        response += line

    br.close();

    response
}

/**
 * Util method to send http POST request.
 */
def sendPostRequest = { urlString, user, pwd, postData, contentType ->
    sendHttpRequest("POST", urlString, user, pwd, postData, contentType);
}

/**
 * Util method to send http GET request.
 */
def sendGetRequest = { urlString, user, pwd->
    sendHttpRequest("GET", urlString, user, pwd, null, null);
}

/*
 * End of util methods.
 */

/**
 * Parsing a special filter from Apache Ignite JIRA and picking up latest by ID
 * attachments to process.
 */
final GIT_REPO = "https://git1-us-west.apache.org/repos/asf/ignite.git"
final JIRA_URL = "https://issues.apache.org"
final ATTACHMENT_URL = "$JIRA_URL/jira/secure/attachment"
final HISTORY_FILE = "${System.getProperty("user.home")}/validated-jira.txt"
final LAST_SUCCESSFUL_ARTIFACT = "guestAuth/repository/download/Ignite_PatchValidation_PatchChecker/.lastSuccessful/$HISTORY_FILE"
final NL = System.getProperty("line.separator")

final def JIRA_CMD = System.getProperty('JIRA_COMMAND', 'jira.sh')

// Envariement variables.
final def TC_PROJECT_NAME = envVariable("PROJECT_NAME", "Ignite")
final def TC_BUILD_EXCLUDE_LIST = envVariableAsList("BUILD_ID_EXCLUDES", ["Ignite_RunAllTestBuilds"])

final def JIRA_USER = System.getenv('JIRA_USER')
final def JIRA_PWD = System.getenv('JIRA_PWD')

final def CONTRIBUTORS = []

def contributors = {
    if (!CONTRIBUTORS) {
        def response = sendGetRequest(
            "$JIRA_URL/jira/rest/api/2/project/12315922/role/10010" as String,
            JIRA_USER,
            JIRA_PWD)

        println "Response on contributors request = $response"

        def json = new groovy.json.JsonSlurper().parseText(response)

        json.actors.each {
            CONTRIBUTORS.add(it.name)
        }

        println "Contributors list: $CONTRIBUTORS"
    }

    CONTRIBUTORS
}

/**
 * Gets jiras for which test tasks were already triggered.
 *
 * @return List of pairs [jira,attachemntId].
 */
def readHistory = {
    final int MAX_HISTORY = 5000

    List validated_list = []

    def validated = new File(HISTORY_FILE)

    if (validated.exists()) {
        validated_list = validated.text.split('\n')
    }

    // Let's make sure the preserved history isn't too long
    if (validated_list.size > MAX_HISTORY) {
        validated_list = validated_list[validated_list.size - MAX_HISTORY..validated_list.size - 1]

        validated.delete()
        validated << validated_list.join(NL)
    }

    println "History=$validated_list"

    validated_list
}

/**
 * Accepting the <jira> XML element from JIRA stream
 * @return <code>null</code> or <code>JIRA-###,latest_attach_id</code>
 */
def getLatestAttachment = { jira ->
    def attachment = jira.attachments[0].attachment.list()
        .sort { it.@id.toInteger() }
        .reverse()
        .find {
            def fName = it.@name.toString()

            contributors().contains(it.@author as String) &&
                (fName.endsWith(".patch") || fName.endsWith(".txt") || fName.endsWith(".diff"))
        }

    String row = null

    if (attachment == null) {
        println "${jira.key} is in invalid state: there was not found '.{patch/txt/diff}'-file from approved user."
    }
    else {
        row = "${jira.key},${attachment.@id}"
    }
}

/**
 * Checks all "Patch availiable" jiras on attached ".patch"-files from approved users.
 */
def findAttachments = {
    // See https://issues.apache.org/jira/issues/?filter=12330308 (the same).
    def JIRA_FILTER =
        "https://issues.apache.org/jira/sr/jira.issueviews:searchrequest-xml/12330308/SearchRequest-12330308.xml?tempMax=100&field=key&field=attachments"
    def rss = new XmlSlurper().parse(JIRA_FILTER)

    final List history = readHistory {}

    LinkedHashMap<String, String> attachments = [:]

    rss.channel.item.each { jira ->
        String row = getLatestAttachment(jira)

        if (row != null && !history.contains(row)) {
            def pair = row.split(',')

            attachments.put(pair[0] as String, pair[1] as String)
        }
    }

    attachments
}

/**
 * Store jira with attachment id to hostory.
 */
def addToHistory = {jira, attachmentId ->
    def validated = new File(HISTORY_FILE)

    assert validated.exists(), "History file does not exist."

    validated << NL + "$jira,$attachmentId"
}

def tryGitAmAbort = {
    try {
        checkprocess "git am --abort".execute(null, new File("../"))

        println "Succsessfull: git am --abort."
    }
    catch (Throwable e) {
        println "Error: git am --abort fails: "
    }
}

/**
 * Applys patch from jira to given git state.
 */
def applyPatch = { jira, attachementURL ->
    // Delete all old IGNITE-*-*.patch files.
    def directory = new File("./")

    println "Remove IGNITE-*-*.patch files in ${directory.absolutePath} and its subdirectories..."

    def classPattern = ~/.*IGNITE-.*-.*\.patch/

    directory.eachFileRecurse(groovy.io.FileType.FILES)
        { file ->
            if (file ==~ classPattern){
                println "Deleting ${file}..."

                file.delete()
            }
        }

    // Main logic.
    println "Patch apllying with jira='$jira' and attachment='$ATTACHMENT_URL/$attachementURL/'."

    def userEmail = System.getenv("env.GIT_USER_EMAIL");
    def userName = System.getenv("env.GIT_USER_NAME");

    def patchFile = new File("${jira}-${attachementURL}.patch")

    println "Getting patch content."

    def attachmentUrl = new URL("$ATTACHMENT_URL/$attachementURL/")

    HttpURLConnection conn = (HttpURLConnection)attachmentUrl.openConnection();
    conn.setRequestProperty("Content-Type", "text/x-patch;charset=utf-8");
    conn.setRequestProperty("X-Content-Type-Options", "nosniff");
    conn.connect();

    patchFile << conn.getInputStream()

    println "Got patch content."

    try {
        tryGitAmAbort()

        execGit "git branch"

        execGit "git config user.email \"$userEmail\""
        execGit "git config user.name \"$userName\""

        // Create a new uniqueue branch to applying patch
        def newTestBranch = "test-branch-${jira}-${attachementURL}-${System.currentTimeMillis()}"
        execGit "git checkout -b ${newTestBranch}"

        execGit "git branch"

        println "Trying to apply patch."

        execGit "git am dev-tools/${patchFile.name}"

        println "Patch was applied successfully."
    }
    catch (Throwable e) {
        println "Patch was not applied successfully. Aborting patch applying."

        tryGitAmAbort()

        throw e;
    }
}

def JIRA_xml = { jiranum ->
    "https://issues.apache.org/jira/si/jira.issueviews:issue-xml/$jiranum/${jiranum}.xml"
}

/**
 * Gets all builds from TC project.
 */
def getTestBuilds = { ->
    def tcURL = System.getenv('TC_URL')

    def project = new XmlSlurper().parse("http://$tcURL:80/guestAuth/app/rest/projects/id:$TC_PROJECT_NAME")

    def buildIds = []

    def count = Integer.valueOf(project.buildTypes.@count as String)

    for (int i = 0; i < count; i++) {
        def id = project.buildTypes.buildType[i].@id

        if (TC_BUILD_EXCLUDE_LIST == null || !TC_BUILD_EXCLUDE_LIST.contains(id))
            buildIds.add(id)
    }

    buildIds
}

/**
 * Adds comment to jira ticket.
 */
def addJiraComment = { jiraNum, comment ->
    try {
        println "Comment: $comment"

        def jsonComment = "{\n \"body\": \"${comment}\"\n}";

        def response = sendPostRequest(
            "$JIRA_URL/jira/rest/api/2/issue/$jiraNum/comment" as String,
            JIRA_USER,
            JIRA_PWD,
            jsonComment,
            "application/json")

        println "Response: $response"
    }
    catch (Exception e) {
        e.printStackTrace()
    }
}

/**
 * Runs all given test builds to validate last patch from given jira.
 */
def runAllTestBuilds = {builds, jiraNum ->
    def tcURL = System.getenv('TC_URL')
    def user = System.getenv('TASK_RUNNER_USER')
    def pwd = System.getenv('TASK_RUNNER_PWD')

    def triggeredBuilds = [:]

    builds.each {
        try {
            println "Triggering $it build for $jiraNum jira..."

            String postData

            if (jiraNum == 'null' || jiraNum == null) {
                postData = "<build>" +
                        "  <buildType id='$it'/>" +
                        "</build>";
            }
            else {
                postData = "<build>" +
                        "  <buildType id='$it'/>" +
                        "  <comment>" +
                        "    <text>Auto triggered build to validate last attached patch file at $jiraNum.</text>" +
                        "  </comment>" +
                        "  <properties>" +
                        "    <property name='env.JIRA_NUM' value='$jiraNum'/>" +
                        "  </properties>" +
                        "</build>";
            }

            println "Request: $postData"

            def response = sendPostRequest(
                "http://$tcURL:80/httpAuth/app/rest/buildQueue" as String,
                user,
                pwd,
                postData,
                "application/xml")

            println "Response: $response"

            def build = new XmlSlurper().parseText(response)

            triggeredBuilds.put(build.buildType.@name, build.@webUrl)
        }
        catch (Exception e) {
            e.printStackTrace()
        }
    }

    // Format comment for jira.
    def triggeredBuildsComment = "There was triggered next test builds for last attached patch-file:\\n"

    def n = 1;

    triggeredBuilds.each { name, url ->
        def prefix = n < 10 ? "0" : ""

        triggeredBuildsComment += "${prefix}${n}. ${url as String} - ${name as String}\\n"

        n++
    }

    addJiraComment(jiraNum, triggeredBuildsComment)
}

/**
 * Main.
 *
 * Modes:
 * 1. "slurp" mode - triggers all TC test builds for all jiras with valid attachment
 * (Jira in "patch availiable" state, there is attached file from approved user with "patch" extension)
 * 2. "patchApply" mode - gets last valid patch file from given jira number and applies it.
 * 3. "runAllBuilds" - triggers given jira number for all TC test builds.
 *
 * Main workflow:
 * 1. run in "slurp" mode
 * 2. each triggered build uses "patchApply" mode to apply latest valid patch.
 */
args.each {
    println "Arg=$it"

    def parameters = it.split(",")

    println parameters

    if (parameters.length >= 1 && parameters[0] == "slurp") {
        println "Running in 'slurp' mode."

        def builds = getTestBuilds()

        println "Test builds to be triggered=$builds"

        def attachments = findAttachments()

        // For each ticket with new attachment, let's trigger remove build
        attachments.each { k, v ->
            //  Trailing slash is important for download; only need to pass JIRA number
            println "Triggering the test builds for: $k = $ATTACHMENT_URL/$v/"

            runAllTestBuilds(builds, k)

            addToHistory(k, v)
        }
    }
    else if (parameters.length >= 1 && parameters[0] == "patchApply") {
        if (parameters.length < 2 || parameters[1] == 'null') {
            println "There is no jira number to apply. Exit."

            return
        }

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
    else if (parameters.length > 1 && parameters[0] == "runAllBuilds" ) {
        def jiraNum = parameters[1]

        println "Running in 'all builds' mode with jira number='$jiraNum'."

        def builds = getTestBuilds()

        println "Test builds to be triggered=$builds"

        runAllTestBuilds(builds, jiraNum)
    }
}
