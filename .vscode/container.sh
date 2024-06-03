buildah build \
  --format oci \
  -t "radar/riab:0.0.58" \
  -f ../Dockerfile

podman run \
  --rm \
  -it \
  -v ./riab.ini:/riab.ini \
  -v .:/cdm_folder \
  -e RIAB_CONFIG=/riab.ini \
  localhost/radar/riab:0.0.58 -r /cdm_folder -t cdm_source

podman run \
  --rm \
  -it \
  -v ./riab.ini:/riab.ini \
  -v .:/cdm_folder \
  -e RIAB_CONFIG=/riab.ini \
  --entrypoint sh \
  localhost/radar/riab:0.0.58