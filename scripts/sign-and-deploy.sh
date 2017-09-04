#!/usr/bin/env bash

function print_help()
{
    echo
    echo "Script for signing artifacts and uploading locally staged Apache-Ignite repository to the remote staging repository."
    echo
    echo "Usage:"
    echo "Place the script next to the \"org\" directory of your locally staged repository."
    echo "Run: sign-and-deploy.sh "
    echo
    echo "You need to add \"gpg\" profile to your maven settings.xml file:"
    echo "<settings>"
    echo "..."
    echo "    <profiles>"
    echo "    ..."
    echo "        <profile>"
    echo "           <id>gpg</id>"
    echo "           <properties>"
    echo "                <gpg.keyname>your_key_name</gpg.keyname>"
    echo "                <gpg.passphrase>your_pass_phrase</gpg.passphrase>"
    echo "                <gpg.useagent>false</gpg.useagent>"
    echo "            </properties>"
    echo "        </profile>"
    echo "    </profiles>"
    echo "</settings>"
    echo
    echo "Also you need to configure \"servers\" section in your maven settings.xml file:"
    echo "<settings>"
    echo "..."
    echo "    <servers>"
    echo "    ..."
    echo "        <server>"
    echo "            <id>apache.releases.https</id>"
    echo "            <username>your_login</username>"
    echo "            <password>your_password</password>"
    echo "        </server>"
    echo "    </servers>"
    echo "</settings>"
}

for arg in $@
do
    if [[ $arg == "-h"  || $arg == "--help" ]]
    then
        print_help
        exit 0
    else
        echo "Unknown argument: ${arg}"
        print_help
        exit 1
    fi
done

server_url="https://repository.apache.org/service/local/staging/deploy/maven2"
server_id="apache.releases.https"

now=$(date +'%H%M%S')

logname="upload-${now}.log"

list=$(find ./org -type d -name "ignite-*")

alldirs=$(find ./org -type d -name "ignite-*" | wc -l)

cnt=0

cd ./org/apache/ignite/ignite-core/

ignite_version=$(ls -d */ | cut -f1 -d'/')

cd ../../../..

for dir in $list
do
    main_file=$(find $dir -name "*${ignite_version}.jar")

    pom=$(find $dir -name "*.pom")

    javadoc=$(find $dir -name "*javadoc.jar")

    sources=$(find $dir -name "*sources.jar")

    tests=$(find $dir -name "*tests.jar")

    features=$(find $dir -name "*features.xml")

    adds=""

    cnt=$((cnt+1))

    echo "Uploading ${dir} (${cnt} of ${alldirs})."

    if [[ $javadoc == *javadoc* ]]
    then
        adds="${adds} -Djavadoc=${javadoc}"
    fi

    if [[ $sources == *sources* ]]
    then
        adds="${adds} -Dsources=${sources}"
    fi

    if [[ $tests == *tests* ]]
    then
        adds="${adds} -Dfiles=${tests} -Dtypes=jar -Dclassifiers=tests"
    fi

    if [[ $features == *features* ]]
    then
        main_file=$pom adds="${adds} -Dpackaging=pom -Dfiles=${features} -Dtypes=xml -Dclassifiers=features"
    fi

    if [[ ! -n $main_file && ! -n $features ]]
    then
        main_file=$pom
        adds="-Dpackaging=pom"
    fi

    echo "Directory:" >> ./$logname
    echo $dir >> ./$logname
    echo "File:" >> ./$logname
    echo $main_file >> ./$logname
    echo "Adds:" >> ./$logname
    echo $adds >> ./$logname
    echo "Features:" >> ./$logname
    echo $features >> ./$logname

    mvn gpg:sign-and-deploy-file -Pgpg -Dfile=$main_file -Durl=$server_url -DrepositoryId=$server_id -DpomFile=$pom ${adds} >> ./$logname
done

result="Uploaded OK."

while IFS='' read -r line || [[ -n "$line" ]]; do
    if [[ $line == *ERROR* ]]
    then
        result="Uploading failed. Please check log file: ${logname}."
    fi
done < ./$logname

echo $result