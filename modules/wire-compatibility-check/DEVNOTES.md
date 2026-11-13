# Wire Format Descriptions

The module generates and collects JSON files to describe the wire format of Ignite messages and help reviewers see changes that may affect compatibility.

Run from the project root:

```bash
./mvnw clean test-compile -pl modules/wire-compatibility-check -am -DskipTests
```

Generated JSON files, including `ids.json`, are saved in:

`modules/wire-compatibility-check/target/classes/META-INF/ignite-wire/`

Do not commit these files to Git.
