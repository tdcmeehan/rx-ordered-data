#!/bin/bash

set -e

if [[ $TRAVIS_BRANCH == 'master' && $TRAVIS_REPO_SLUG == "Conductor/rx-ordered-data" && $TRAVIS_PULL_REQUEST == 'false' ]]; then

    ./gradlew javadoc

    # Get to the Travis build directory, configure git and clone the repo
    cd $HOME
    git config --global user.email "travis@travis-ci.org"
    git config --global user.name "travis-ci"
    git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/Conductor/rx-ordered-data gh-pages > /dev/null

    # Commit and Push the Changes
    cd gh-pages
    git rm -rf ./javadoc
    cp -Rf $TRAVIS_BUILD_DIR/build/docs/javadoc ./javadoc
    git add -f .
    git commit -m "Lastest javadoc on successful travis build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"
    git push -fq origin gh-pages > /dev/null
else
    echo "Not on master branch, so not publishing"
    echo "TRAVIS_BRANCH: $TRAVIS_BRANCH"
    echo "TRAVIS_REPO_SLUG: $TRAVIS_REPO_SLUG"
    echo "TRAVIS_PULL_REQUEST: $TRAVIS_PULL_REQUEST"
fi