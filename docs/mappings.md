# Notes on Usagi mappings

The majority of your source concepts can be mapped to **standard concepts** of existing vocabularies accepted by OMOP (e.g. SNOMED, LOINC, RXNorm, ...), but if no suiting standard concept exisits for your source concept, a **custom concept** needs to be created.

## Mapping input file

Before you start mapping your source codes and concepts, you need a csv file containing 3 columns: sourceCode, sourceName and sourceFrequency.

<img width="422" alt="image" src="https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98480071/a400ae3a-d1a3-4875-a2fb-0cd59e68bc8f">


This file is your input for Usagi or [Keun](https://github.com/RADar-AZDelta/Keun), go to the documentation of [Keun](https://github.com/RADar-AZDelta/Keun) for more information on how to map your concepts. 

## Follow the mapping flow to choose an appropriate concept

RADar has created mapping guidelines to select the correct filters and relevant domains and classes.
The mapping tool can run a term similarity approach or a small translation model to map source codes to OMOP Vocabulary concepts (autosuggestion).

Leverage the mapping interface to check suggested mappings or create maps. Preferably an individual who has experience with the coding system and medical terminology should be assigned as reviewer.

![image](https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98580512/38a93303-6dba-4ef3-9cc5-780b33477014)


Export the final mapping, share with a reviewer to reach an 'APPROVED' mapping status and place in correct subfolder. 

Example of an output doctor_usagi.csv:

![image](https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98580512/c7951073-37ed-4b74-8b8e-300bd17f47b6)

For some tables like condition_occurrence, automatic mapping (automapping), is integrated in the 'Usagi source CSV' queries as input for the mapping tool as verification.

**TO DO: LINK TO MAPPING FLOW CHART!!**


## Custom concepts


#### Create custom concept in Keun

The custom concepts are mapped from the usagi.csv. The mapping tool [Keun](https://radar-azdelta.github.io/Keun/) provides the option to create a custom concept, an option which is not yet implemented in Usagi.

<img width="1023" alt="image" src="https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98480071/839bcfb1-0b7b-4d6a-81a1-2a09906ab676">


#### source_to_concept_map 

The **custom concepts** get added to the **source_to_concept_map** table, cfr. the standard mapped concepts. The concept_code is used as the source_code, the newly assigned (>2.000.000.000) concept_id is used as the target_concept_id.

<img width="867" alt="image" src="https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98480071/03945ee1-05af-4cb2-a17e-461939214ac9">


#### usagi table 

The custom concepts are also added to the **usagi table**, using the concept_code as sourceCode and the newly assigned concept_id as the conceptId.

<img width="707" alt="image" src="https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98480071/4a3ef579-eeb1-4c8c-80f9-2202e3b3389c">


## Finalizing the mapping

Once you are done mapping your input file, an _usagi.csv and _concept.csv (if custom concepts were created) is exported. The _usagi.csv should be copied directly under the _concept_id folder where your mapping input file and query to create your mapping input file is stored. The _concept.csv holds your custom concepts, which should be placed in the 'custom' folder under the _concept_id folder that your concepts will be transformed to in OMOP. Make sure your custom mappings  have an empty first concept_id column since RiaB will create an appropriate integer for the concept_id column. Finally, your folder structure should look like this:

<img width="215" alt="image" src="https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98480071/f4adbc83-bc97-450e-ba43-b21c12bb8217">

The sequels of your files and the location where you store these files are very important since RiaB only takes correctly placed and named files during the ETL!

## Updating existing mapping files

You will need to run the cleanup command when concept mappings change in your existing Usagi CSV's. The cleanup is not necessary when you add new queries or add additional Usagi mappings.
If your custom concepts changed (prequel, domain, class_id, deleted concepts, ...), you also need to add the flag **--clear-auto-generated-custom-concept-ids**.
