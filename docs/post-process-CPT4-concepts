# Explanation
When we download the vocabularies from Athena, the concepts of the CPT4 vocabulary are not present in the CONCEPT.csv, while they are present in the other vocabulary csv's. Along with the concept files, there are scripts (cpt.sh, cpt.bat, cpt4.jar) to process the CONCEPT_CPT4.csv into the CONCEPT.csv file. Depending on your operating system, you can use the appropriate script. We work on a linux system, so we use the cpt.sh script. The other scripts do exactly the same. The CONCEPT_CPT4.csv is almost entirely the same as the CONCEPT.csv, but there are no concept_names filled in, which is necessary in the concept table! Therefore, we need credentials for UMLS through which the script can link the concepts in the CONCEPT_CPT4.csv to the corresponding concepts in the UMLS database. Afterwards, the CPT4 concepts are added to the CONCEPT.csv file.


# Steps

1. Open a terminal like git bash

2. Copy your downloaded zip file to your operating system (vocabulary_${CURRENT_DATE}_no_CPT4.zip). 

3. Get your UMLS credentials (API_KEY)

4. Run the commands below in the terminal on your operating system, for linux operating system we use the bash cpt.sh script.

```bash
CURRENT_DATE=20240329
unzip vocabulary_${CURRENT_DATE}_no_CPT4.zip -d vocabulary_${CURRENT_DATE}_no_CPT4
cd vocabulary_${CURRENT_DATE}
API_KEY=.....
bash cpt.sh ${API_KEY} 
```

Now wait a bit, this takes a while. Also, it happens that the first run, not all concepts are processed. This will be mentioned in the logfiles and you will have to run the script again.
Normally, all concepts are processed after the second run.

5. ZIP the files to make the folder ready for RiaB

```bash
cd vocabulary_${CURRENT_DATE}_no_CPT4
zip -r ../vocabulary_${CURRENT_DATE}.zip ./*
```

