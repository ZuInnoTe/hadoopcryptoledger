if [ "$TRAVIS_REPO_SLUG" == "ZuInnoTe/hadoopcryptoledger" ] && [ "$TRAVIS_PULL_REQUEST" == "false" ] && [ "$TRAVIS_BRANCH" == "master" ]; then

echo -e "Publishing OWASP depdency analyzer results...\n"

# copy to home
mkdir -p $HOME/inputformat/dependencycheck
cp -R inputformat/build/reports/dependency-check-report.html $HOME/inputformat/dependencycheck
#mkdir -p $HOME/hiveserde/dependencycheck
#cp -R hiveserde/build/reports/dependency-check-report.html $HOME/hiveserde/dependencycheck
#mkdir -p $HOME/hiveudf/dependencycheck
#cp -R hiveudf/build/reports/dependency-check-report.html $HOME/hiveudf/dependencycheck

# Get to the Travis build directory, configure git and clone the repo
cd $HOME
git config --global user.email "travis@travis-ci.org"
git config --global user.name "travis-ci"
git clone --quiet --branch=gh-pages https://${GH_TOKEN}@github.com/ZuInnoTe/hadoopcryptoledger gh-pages > /dev/null

# Commit and Push the Changes
cd gh-pages
git rm -rf ./dependencycheck/inputformat
mkdir -p ./dependencycheck/inputformat
cp -Rf $HOME/inputformat/dependencycheck ./dependencycheck/inputformat
#git rm -rf ./dependencycheck/hiveserde
#mkdir -p ./dependencycheck/hiveserde
#cp -Rf $HOME/hiveserde/dependencycheck ./dependencycheck/hiveserde
#git rm -rf ./dependencycheck/hiveudf
#mkdir -p ./dependencycheck/hiveudf
#cp -Rf $HOME/hiveudf/dependencycheck ./dependencycheck/hiveudf
git add -f .
git commit -m "Lastest OWASP dependency check results on successful travis build $TRAVIS_BUILD_NUMBER auto-pushed to gh-pages"
git push -fq origin gh-pages > /dev/null

fi
