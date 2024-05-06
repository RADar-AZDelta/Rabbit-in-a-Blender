# Notes on Usagi mappings

The majority of your source concepts can be mapped to **standard concepts** of existing vocabularies accepted by OMOP (e.g. SNOMED, LOINC, RXNorm, ...), but if no suiting standard concept exisits for your source concept, a **custom concept** needs to be created.

## Mapping input file

Before you start mapping your source codes and concepts, you need a csv file containing 3 columns: sourceCode, sourceName and sourceFrequency.

<img width="422" alt="image" src="https://github.com/RADar-AZDelta/Rabbit-in-a-Blender/assets/98480071/a400ae3a-d1a3-4875-a2fb-0cd59e68bc8f">


This file is your input for Usagi or [Keun](https://github.com/RADar-AZDelta/Keun), go to the documentation of [Keun](https://github.com/RADar-AZDelta/Keun) for more information on how to map your concepts. 

## Follow the mapping flow to choose an appropriate concept

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


## Updating existing mapping files

You will need to run the cleanup command when concept mappings change in your existing Usagi CSV's. The cleanup is not necessary when you add new queries or add additional Usagi mappings.
If your custom concepts changed (prequel, domain, class_id, deleted concepts, ...), you also need to add the flag **--clear-auto-generated-custom-concept-ids**.
