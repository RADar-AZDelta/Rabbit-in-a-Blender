Add as submodule:

```bash
git submodule add --name Achilles --branch main --force ssh://git@github.com/OHDSI/Achilles.git src/riab/libs/Achilles
```

Set to specific tag:

```bash
VERSION=v1.7.2
cd src/riab/libs/Achilles
git checkout ${VERSION}
cd ../../../..
git add libs/Achilles
git commit -m "moved Achilles submodule to ${VERSION}"
git push
```