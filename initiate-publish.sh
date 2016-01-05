#!/bin/bash

set -e

if [[ $TRAVIS_BRANCH == 'master' && $TRAVIS_REPO_SLUG == "Conductor/rx-ordered-data" && $TRAVIS_PULL_REQUEST == 'false' ]]; then
    ./gradlew uploadArchives -PnexusUsername="${NEXUS_USERNAME}" -PnexusPassword="${NEXUS_PASSWORD}"
    RETVAL=$?

    if [ $RETVAL -eq 0 ]; then
        echo 'Completed publish!'
    else
        echo 'Publish failed.'
        return 1
    fi

else
    echo "Not on master branch, so not publishing"
    echo "TRAVIS_BRANCH: $TRAVIS_BRANCH"
    echo "TRAVIS_REPO_SLUG: $TRAVIS_REPO_SLUG"
    echo "TRAVIS_PULL_REQUEST: $TRAVIS_PULL_REQUEST"
fi