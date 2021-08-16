# Apache Ignite RPM package build


### Prerequisites

* **Operation system**: CentOS 8

* **Packages**: rpm-build

* **Assembled and prepared Apache Ignite**:
```bash
cd <project root>
mvn clean package -Dmaven.test.skip
cp -rfv modules/cli/target/ignite \
        deliveries/rpm/
```

* **Exported package version variable**
```bash
cd deliveries/rpm
PACKAGE_VERSION="$(grep -E '^\*' apache-ignite.spec | head -1 | sed -r 's|.*\s-\s||')"
```

<br/>


### Building
Run build script (from RPM build directory):
```bash
bash build.sh "${PACKAGE_VERSION}"
```
Built RPM package will be in RPM build directory

<br/>

### Building in Docker
Run from RPM build directory:
```bash

docker build . --pull \
               --build-arg PACKAGE_VERSION="${PACKAGE_VERSION}" \
               -t apache-ignite-rpm:${PACKAGE_VERSION}
docker run --rm \
           --entrypoint \
           cat apache-ignite-rpm:${PACKAGE_VERSION} /tmp/apache-ignite-${PACKAGE_VERSION}.noarch.rpm > apache-ignite-${PACKAGE_VERSION}.noarch.rpm
```
Built RPM package will be in RPM build directory

