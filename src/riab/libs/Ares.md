Add as submodule:

```bash
git submodule add --name Ares --branch main --force ssh://git@github.com/OHDSI/Ares.git src/riab/libs/Ares
```

Set to specific tag:

```bash
VERSION=v0.3-beta
cd src/riab/libs/Ares
git checkout ${VERSION}
cd ../../../..
git add src/riab/libs/Ares
git commit -m "moved Ares submodule to ${VERSION}"
git push
```