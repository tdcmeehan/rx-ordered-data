#!/bin/bash

set -e

if [[ $TRAVIS_BRANCH == 'master' && $TRAVIS_REPO_SLUG == "Conductor/rx-ordered-data" && $TRAVIS_PULL_REQUEST == 'false' ]]; then
    echo "<settings><servers><server><id>sonatype</id><username>\${env.SONATYPE_USERNAME}</username><password>\${env.SONATYPE_PASSWORD}</password></server></servers></settings>" > ~/settings.xml
    if [ -n "TRAVIS_TAG" ]; then
        mvn release:clean --batch-mode release:prepare -Prelease -Dgpg.passphrase=${GPG_PHRASE} -Dgpg.keyname=5976652B
        mvn release:perform
        RETVAL=$?

        if [ $RETVAL -eq 0 ]; then
            echo 'Completed publish!'
        else
            echo 'Publish failed.'
            return 1
        fi
    else
        mvn clean deploy
        RETVAL=$?

        if [ $RETVAL -eq 0 ]; then
            echo 'Completed publish!'
        else
            echo 'Publish failed.'
            return 1
        fi
    fi
else
    echo "Not on master branch, so not publishing"
    echo "TRAVIS_BRANCH: $TRAVIS_BRANCH"
    echo "TRAVIS_REPO_SLUG: $TRAVIS_REPO_SLUG"
    echo "TRAVIS_PULL_REQUEST: $TRAVIS_PULL_REQUEST"
fi