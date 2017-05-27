#!/bin/bash
if [ "$TRAVIS_REPO_SLUG" == "ZuInnoTe/hadoopcryptoledger" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_BRANCH" == "master" ]; then

echo -e "Publishing javadoc...\n"

# copy to home
mkdir -p $HOME/inputformat/javadoc-latest
cp -R inputformat/build/docs/javadoc $HOME/inputformat/javadoc-latest
mkdir -p $HOME/hiveserde/javadoc-latest
cp -R hiveserde/build/docs/javadoc $HOME/hiveserde/javadoc-latest
mkdir -p $HOME/hiveudf/javadoc-latest
cp -R hiveudf/build/docs/javadoc $HOME/hiveudf/javadoc-latest
mkdir -p $HOME/flinkdatasource/javadoc-latest
cp -R flinkdatasource/build/docs/javadoc $HOME/flinkdatasource/javadoc-latest

# Get to the Travis build directory, configure git and clone the repo
cd $HOME
git config --global user.email "travis@travis-ci.org"
git config --global user.name "travis-ci"
git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/ZuInnoTe/hadoopcryptoledger gh-pages > /dev/null


# Commit and Push the Changes
cd gh-pages
git rm -rf ./javadoc/inputformat
mkdir -p ./javadoc/inputformat
cp -Rf $HOME/inputformat/javadoc-latest ./javadoc/inputformat
git rm -rf ./javadoc/hiveserde
mkdir -p ./javadoc/hiveserde
cp -Rf $HOME/hiveserde/javadoc-latest ./javadoc/hiveserde
git rm -rf ./javadoc/hiveudf
mkdir -p ./javadoc/hiveudf
cp -Rf $HOME/hiveudf/javadoc-latest ./javadoc/hiveudf
mkdir -p ./javadoc/flinkdatasource
cp -Rf $HOME/flinkdatasource/javadoc-latest ./javadoc/flinkdatasource
git add -f .
git commit -m "Lastest javadoc on successful travis build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"
git push -fq origin gh-pages > /dev/null

fi
