Add as submodule:

```bash
git submodule add --name CommonDataModel --branch main --force ssh://git@github.com/OHDSI/CommonDataModel.git src/riab/libs/CommonDataModel
```

Set to specific tag:

```bash
VERSION=v5.4.1
pushd src/riab/libs/CommonDataModel
git checkout ${VERSION}
popd
git add libs/CommonDataModel
git commit -m "moved CommonDataModel submodule to ${VERSION}"
git push
```