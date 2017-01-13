if [ "$TRAVIS_REPO_SLUG" == "ZuInnoTe/hadoopcryptoledger" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_BRANCH" == "master" ]; then

echo -e "Publishing test results...\n"

# copy to home
mkdir -p $HOME/inputformat/tests-latest
cp -R fileformat/build/reports/tests $HOME/fileformat/tests-latest
mkdir -p $HOME/hiveserde/tests-latest
cp -R hiveserde/build/reports/tests $HOME/hiveserde/tests-latest
mkdir -p $HOME/hiveudf/tests-latest
cp -R hiveudf/build/reports/tests $HOME/hiveudf/tests-latest

# Get to the Travis build directory, configure git and clone the repo
cd $HOME
git config --global user.email "travis@travis-ci.org"
git config --global user.name "travis-ci"
git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/ZuInnoTe/hadoopcryptoledger gh-pages > /dev/null

# Commit and Push the Changes
cd gh-pages
git rm -rf ./tests/inputformat
mkdir -p ./tests/inputformat
cp -Rf $HOME/inputformat/tests-latest ./tests/inputformat
git rm -rf ./tests/hiveserde
mkdir -p ./tests/hiveserde
cp -Rf $HOME/hiveserde/tests-latest ./tests/hiveserde
git rm -rf ./tests/hiveudf
mkdir -p ./tests/hiveudf
cp -Rf $HOME/hiveudf/tests-latest ./tests/hiveudf
git add -f .
git commit -m "Lastest javadoc on successful travis build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"
git push -fq origin gh-pages > /dev/null

fi
