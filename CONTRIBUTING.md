# Contributing to Apache Ignite
## Joining Community and it's communication channels
- Sign-in to Apache JIRA https://issues.apache.org/jira/
- (To contribute code patches) Sing-in to [Apache Ignite](https://ignite.apache.org/) Continuous Integration server https://ci.ignite.apache.org/
- (To contribute documentation to the wiki) Sing-in to [Apache Wiki](https://cwiki.apache.org/confluence/display/IGNITE)
- Subscribe to both Apache [User List - user@ignite.apache.org](https://lists.apache.org/list.html?user@ignite.apache.org)
and [Dev List - dev@ignite.apache.org](https://lists.apache.org/list.html?dev@ignite.apache.org) lists with your personal (non-corporate) email address.
 Optionally you can subscribe to [Notifications List - notifications@ignite.apache.org](https://lists.apache.org/list.html?notifications@ignite.apache.org).

 To subscribe to any list in the [Apache Software Foundation](https://www.apache.org/foundation/) you can send email to `list_name`-subscribe@ignite.apache.org, e.g., to dev-subscribe@ignite.apache.org and follow instructions.

- Send a welcome message to the dev list to introduce yourself to the community and saying that you're going to contribute. 
Request access to [Apache Ignite JIRA](https://issues.apache.org/jira/) in the same email sharing your JIRA ID.
Example of message:
```
Hello Ignite Community!

My name is Aristarkh. I want to contribute to Apache Ignite and want to start with this issue - IGNITE-NNNNN, my JIRA username Aristarkh. Any help on this will be appreciated.

Thanks!
```

## Needed Contributions
[Apache Ignite](https://ignite.apache.org/) Community values any type of contributions from [CoPDoC](https://community.apache.org/contributors/#contributing-a-project-copdoc). Community member can contibute to any areas from (Co)mmunity, (P)roject, (Do)cumentation, and (C)ode.

Community values code contributions, but project value non-code contributions too, from writers, editors, testers, etc.
We value user support on the list, as well as providing a summary of [user list](https://lists.apache.org/list.html?user@ignite.apache.org) threads to Apache Ignite developers.

## How To Contribute
Detailed guidelines on [How To Contribute](https://cwiki.apache.org/confluence/display/IGNITE/How+to+Contribute) can be found in [Apache Ignite wiki](https://cwiki.apache.org/confluence/display/IGNITE/How+to+Contribute)

Apache Ignite follows [Apache Code of Conduct](https://www.apache.org/foundation/policies/conduct.html). You can also cover
[Etiquette Guide](http://community.apache.org/contributors/etiquette)

Apache Ignite prefer to use [consensus to make decisions](http://community.apache.org/committers/consensusBuilding.html), but in case something is going wrong please see [Escalation Guide](https://www.apache.org/board/escalation)

## Contributing Documentation
Documentation can be contributed to
 - End-User documentation https://apacheignite.readme.io/ . Use Suggest Edits. See also [How To Document](https://cwiki.apache.org/confluence/display/IGNITE/How+to+Document).
 - Developer documentation, design documents, IEPs [Apache Wiki](https://cwiki.apache.org/confluence/display/IGNITE). Ask at [Dev List](https://lists.apache.org/list.html?dev@ignite.apache.org) to be added as editor.
 - Markdown files, visible at GitHub, e.g. README.md; drawings explaining Apache Ignite & product internals.
 - Javadocs for packages (package-info.java), classes, methods, etc.

## Blogs
You can also blog about the product. It helps users to understand how to use Apache Ignite and helps spreading ideas.

Feel free to share link to your blog with
 [User](https://lists.apache.org/list.html?user@ignite.apache.org) & [Dev](https://lists.apache.org/list.html?dev@ignite.apache.org) lists.
Blogs are reffered from [Apache Ignite Blogs](https://ignite.apache.org/blogs.html).

## Contributing Code
### Project Initial Setup
Create [Apache Ignite code](https://github.com/apache/ignite) fork using GitHub interface.
Download sources locally and setup project according to [Project Setup wiki](https://cwiki.apache.org/confluence/display/IGNITE/Project+Setup)

### Code inspections, styles and abbreviation rules.
Project code style is specified Apache Ignite [Coding Guidelines](https://cwiki.apache.org/confluence/display/IGNITE/Coding+Guidelines).

Please install following components for development using IntelliJ IDEA
* Install [Abbreviation Plugin](https://cwiki.apache.org/confluence/display/IGNITE/Abbreviation+Rules#AbbreviationRules-IntelliJIdeaPlugin).
* Code Inspection  [Code Inspection Profile](https://cwiki.apache.org/confluence/display/IGNITE/Coding+Guidelines#CodingGuidelines-C.CodeInspection).
Inspection profile is placed to IDEA [Project_Default](.idea/inspectionProfiles/Project_Default.xml), and it should be applied automatically.
* Configure [IDEA Codestyle](https://cwiki.apache.org/confluence/display/IGNITE/Coding+Guidelines#CodingGuidelines-A.ConfigureIntelliJIDEAcodestyle).

### Building project
Usually all Maven builds are to be run with skipped tests:
```
mvn -DskipTests
```
See also [DEVNOTES.txt](DEVNOTES.txt)
