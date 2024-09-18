VERSION=0.0.65

#change version in the pyproject.toml
deactivate 
pip install toml
python <<EOF
import toml
f = 'pyproject.toml'
d = toml.load(open(f, 'r'))
d['project']['version'] = "${VERSION}"
toml.dump(d, open(f, 'w'))
EOF

# push the bumped version to git
git add .vscode/bump_version.sh
git add pyproject.toml
git commit -m "Bumped version to ${VERSION}"
git push origin

# add tag to git
git tag -a ${VERSION} -m "${VERSION}"
git push origin ${VERSION}

# remove a tag local and remote
# git tag -d ${VERSION}
# git push origin :refs/tags/${VERSION}