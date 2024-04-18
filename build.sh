# install required packages
#python3 -m pip install --upgrade pip twine build

# build and publish
rm -fr ./dist && python -m build # cleanup and build
# CHANGE THE VERSION in the pyproject.toml file
python3 -m twine upload --verbose --repository pypi dist/* # choose between repo pypi and testpypi


# uninstall the package
pip uninstall -y Rabbit-in-a-Blender
# wait a moment
sleep 10
#pip install -i https://test.pypi.org/simple/ Rabbit-in-a-Blender # install from test repo
pip install --no-cache-dir Rabbit-in-a-Blender==0.0.49


#git tag -a 0.0.49 -m "0.0.49"
#git tag
#git push origin 0.0.49
