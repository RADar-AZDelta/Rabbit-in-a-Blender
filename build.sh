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
pip install --no-cache-dir Rabbit-in-a-Blender==0.0.54


#git tag -a 0.0.54 -m "0.0.54"
#git tag
#git push origin 0.0.54


# buildah build \
#   --format oci \
#   -t "radar/riab:0.0.54" \
#   -f ./Dockerfile

# podman run \
#   --rm \
#   -it \
#   -v ./riab.ini:/riab.ini \
#   -v .:/cdm_folder \
#   -e RIAB_CONFIG=/riab.ini \
#   localhost/radar/riab:0.0.54 -r /cdm_folder -t cdm_source

# podman run \
#   --rm \
#   -it \
#   -v ./riab.ini:/riab.ini \
#   -v .:/cdm_folder \
#   -e RIAB_CONFIG=/riab.ini \
#   --entrypoint sh \
#   localhost/radar/riab:0.0.54