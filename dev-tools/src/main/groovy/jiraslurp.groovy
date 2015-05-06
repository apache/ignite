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
final validated_filename = "validated-jira.txt"
final LAST_SUCCESSFUL_ARTIFACT = "guestAuth/repository/download/Ignite_PatchValidation_Main/.lastSuccessful/$validated_filename"

final def JIRA_CMD = System.getProperty('JIRA_COMMAND', 'jira.sh')
LinkedHashMap<String, String> jirasAttached = [:]

def readHistory = {
  final int MAX_HISTORY = 5000

  List validated_list = []
  def validated = new File(validated_filename)
  if (validated.exists()) {
    validated_list = validated.text.split('\n')
    validated.delete()
  } else {
    try {
      validated_list =
          new URL("http://204.14.53.152/$LAST_SUCCESSFUL_ARTIFACT").text.split('\n')
    } catch (Exception e) {
      println e.getMessage()
    }
  }
  // Let's make sure the preserved history isn't too long
  if (validated_list.size > MAX_HISTORY) {
    validated_list = validated_list[validated_list.size-MAX_HISTORY..validated_list.size-1]
  }
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
  } else {
    row = "${jira.key},${latestAttr.@id}"
  }
}

def checkForAttachments = {
  def JIRA_FILTER =
      "https://issues.apache.org/jira/sr/jira.issueviews:searchrequest-xml/12330308/SearchRequest-12330308.xml?tempMax=100&field=key&field=attachments"
  def rss = new XmlSlurper().parse(JIRA_FILTER)
  List list = readHistory{}

  rss.channel.item.each { jira ->
    String row = getLatestAttachment (jira)
    if (row != null && !list.contains(row)) {
      def pair = row.split(',')
      jirasAttached.put(pair[0] as String, pair[1] as String)
      list.add(row)
    }
  }

  // Write everything back to persist the list
  def validated = new File(validated_filename)
  validated << list.join('\n')
}

def checkprocess = { process ->
  process.waitFor()
  if (process.exitValue() != 0) {
    println "Return code: " + process.exitValue()
    println "Errout:\n" + process.err.text
    assert process.exitValue() == 0 || process.exitValue() == 128
  }
}

def create_gitbranch = {jira, attachementURL ->
  println jira
  GIT_REPO
  println "$ATTACHMENT_URL/$attachementURL/"

  def patchFile = new File("${jira}.patch")
  patchFile << new URL("$ATTACHMENT_URL/$attachementURL/").text
  checkprocess "git clone --depth 1 $GIT_REPO".execute()
  checkprocess "git checkout -b sprint-2 origin/sprint-2".execute(null, new File('incubator-ignite'))
  checkprocess "git am ../${patchFile.name}".execute(null, new File('incubator-ignite'))
  patchFile.delete()
}

def JIRA_xml = { jiranum ->
  "https://issues.apache.org/jira/si/jira.issueviews:issue-xml/$jiranum/${jiranum}.xml"
}

args.each {
  println it
  def parameters = it.split('=')

  if (parameters[0] == 'slurp') {
    checkForAttachments()
    // For each ticket with new attachment, let's trigger remove build
    jirasAttached.each { k, v ->
      //  Trailing slash is important for download; only need to pass JIRA number
      println "Triggering the build for: $k = $ATTACHMENT_URL/$v/"
    }
  } else if (parameters.length == 2 && parameters[0] == 'JIRA_NUM' && parameters[1] ==~ /\w+-\d+/) {
    // Extract JIRA rss from the and pass the ticket element into attachment extraction
    def rss = new XmlSlurper().parse(JIRA_xml(parameters[1]))
    String row = getLatestAttachment(rss.channel.item)
    if (row != null) {
      def pair = row.split(',')
      create_gitbranch(pair[0], pair[1])
    }
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
