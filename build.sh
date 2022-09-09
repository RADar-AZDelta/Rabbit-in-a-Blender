# install required packages
#python3 -m pip install --upgrade pip twine

# build and publish
rm -fr ./dist # cleanup
python -m build # build
python3 -m twine upload --verbose --repository testpypi dist/* # choose between repo pypi and testpypi


# install the package
pip uninstall -y Rabbit-in-a-Blender
#pip install -i https://test.pypi.org/simple/ Rabbit-in-a-Blender # install from test repo
pip install --no-cache-dir Rabbit-in-a-Blender