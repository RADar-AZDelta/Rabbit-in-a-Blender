Add as submodule:

```bash
git submodule add --name SqlRender --branch main --force ssh://git@github.com/OHDSI/SqlRender.git libs/SqlRender
```

Set to specific tag:

```bash
VERSION=v1.10.0
cd libs/SqlRender
git checkout ${VERSION}
cd ../..
git add libs/SqlRender
git commit -m "moved SqlRender submodule to ${VERSION}"
git push
```