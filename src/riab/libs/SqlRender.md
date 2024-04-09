Add as submodule:

```bash
git submodule add --name SqlRender --branch main --force ssh://git@github.com/OHDSI/SqlRender.git src/riab/libs/SqlRender
```

Set to specific tag:

```bash
VERSION=v1.16.1
cd src/riab/libs/SqlRender
git checkout ${VERSION}
cd ../../../..
git add libs/SqlRender
git commit -m "moved SqlRender submodule to ${VERSION}"
git push
```