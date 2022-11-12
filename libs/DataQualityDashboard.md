Add as submodule:

```bash
git submodule add --name DataQualityDashboard --branch main --force ssh://git@github.com/OHDSI/DataQualityDashboard.git libs/DataQualityDashboard
```

Set to specific tag:

```bash
VERSION=v1.4.1
cd libs/DataQualityDashboard
git checkout ${VERSION}
cd ../..
git add libs/DataQualityDashboard
git commit -m "moved DataQualityDashboard submodule to ${VERSION}"
git push
```