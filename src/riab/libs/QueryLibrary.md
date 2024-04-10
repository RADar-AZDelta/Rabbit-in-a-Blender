Add as submodule:

```bash
git submodule add --name QueryLibrary --branch main --force ssh://git@github.com/OHDSI/QueryLibrary.git src/riab/libs/QueryLibrary
```

Get the latest sourcecode from main branch (no version tags available)

```bash
cd src/riab/libs/QueryLibrary
git pull origin main
cd ../../../..
git add src/riab/libs/QueryLibrary
git commit -m "moved QueryLibrary submodule to master"
git push
```