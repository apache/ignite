# NOTES

---

## Build Instructions

### Build Requirements

- JDK 8
- Maven 3.6.3+

### Build all Extensions

```shell
# Run from the Ignite Extension project root
mvn clean install -DskipTests -Pcheckstyle
```

### Build an Extension

```shell
# Run from the Ignite Extension project root
mvn clean install -f modules/spring-boot-ext -Pcheckstyle -DskipTests
```

or

```shell
# Run from the Ignite Extension project root
mvn clean install -pl :ignite-aws-ext -am -Pcheckstyle -DskipTests
```

## Release Instructions

### Prerequisites

- Personal PGP Key Pair (see [Generating a Key Pair][1])  
- GnuGP installed (see [Installing GnuPG][2])
- Repository credentials configured (see [scripts/settings.xml][3])
- You're using an encrypted your ASF password (see [Maven Password Encryption Guide][4])

You can refer to the [Release Publishing Guide][5] and [Publishing Maven Releases to Maven Central Repository][6] 
of the Apache Software Foundation release process for better understanding the whole process.

[1]: <https://central.sonatype.org/publish/requirements/gpg/#generating-a-key-pair> "Generating a Key Pair"
[2]: <https://central.sonatype.org/publish/requirements/gpg/#installing-gnupg> "Installing GnuPG"
[3]: <https://github.com/apache/ignite-extensions/blob/master/scripts/settings.xml> "Extensions settings.xml"
[4]: <https://maven.apache.org/guides/mini/guide-encryption.html> "Maven Encryption Guide"
[5]: <https://infra.apache.org/release-publishing.html#distribution> "Apache Software Foundation the Release Publishing Guide"
[6]: <https://infra.apache.org/publishing-maven-artifacts.html> "Publishing Maven Releases to Maven Central Repository"

### Prepare a new Release Candidate

- Create and push an extension release branch with the following branch name format: `release/[extension-project-name]-[extension-version]`.

   ```shell
   git remote set-url origin https://github.com/apache/ignite-extensions.git
   git checkout master
   git checkout -b release/ignite-aws-ext-1.0.0
   git push origin release/ignite-aws-ext-1.0.0
   ```

- Update Extension parent reference version and the extension module version using the `scripts/update-versions.sh`.

   ```shell
   # Usage: scripts/update-versions.sh [<ignite-parent-version>] <module-path> <module-release-version>
   scripts/update-versions.sh [2.13.0] modules/asw-ext/ 1.0.0
   ```
  
- Run the [Extension Prepare Release Candidate][7] GitHub Action using the release branch as job source 
this job will also create a rc-tag which points to the last commit in the release branch.
- From the execution job result download the `zip` artifact containing all the stuff required for 
signing and deploying release artifacts.
- Run the `vote_[mvn][pgp]_jar_deploy.sh` to sign and deploy extensions jar's to Maven Central.
- Run the `vote_[pgp]_sign_dist.sh` to sign the extension binary and source zip-archives.
- Run the `vote_[svn]_upload_dist.sh` to upload signed zip-archives.
- Run the [Extension Check Release Candidate][8] GitHub Action to verify the papered release candidate.


[7]: <https://github.com/apache/ignite-extensions/actions/workflows/prepare-rc.yml> "Extension Prepare Release Candidate"
[8]: <https://github.com/apache/ignite-extensions/actions/workflows/release-checker.yml> "Extension Check Release Candidate"

## Development Instructions

### Running GitHub Actions Locally

Configure the `act` command line utility. When you run `act` it reads projects GitHub Actions 
from `.github/workflows/` and determines the set of actions that need to be run on Docker image. 

Use the following installation guide to install the `act` command:
https://github.com/nektos/act/blob/master/README.md#installation

#### Run

```shell
act --job check --eventpath event.json -s GITHUB_TOKEN=[your_fork_github_token]
```

The `event.json`:

```json
{
  "action": "workflow_dispatch",
  "inputs": {
    "extension-name": "ignite-zookeeper-ip-finder-ext",
    "release-version": "1.0.0"
  }
}
```

#### Troubleshooting

The `act` command executes the workflow in a docker container. Some docker images may not have 
the `mvn` command pre-installed. Thus, you have to install it in the docker container manually
as an action step. Use the step below to install Maven into container:

```yaml
- name: Download Maven
  run: |
    curl -sL https://www-eu.apache.org/dist/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.zip -o maven.zip
    apt-get update
    apt-get -y install unzip
    unzip -d /usr/share maven.zip
    rm maven.zip
    ln -s /usr/share/apache-maven-3.6.3/bin/mvn /usr/bin/mvn
    echo "M2_HOME=/usr/share/apache-maven-3.6.3" | tee -a /etc/environment
```
