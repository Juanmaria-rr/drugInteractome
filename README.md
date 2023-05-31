# drugInteractome
PySpark workflow to analyse interactome's drug of targets using EnsemblID, IntAct database and OT data

## Drug Target Discovery: Integrated Database Analysis

This repository contains code for a drug target discovery project aimed at integrating databases of target interactions and disease information to identify potential drug targets for specific diseases. The script provided enables the exploration of target interactors with available drugs, including their clinical phases, and facilitates the discovery of new therapeutic options.

## Key Questions Addressed

1. 🔬 **How are target interactions and disease information integrated?**
   The code combines datasets of target-molecular interactions and disease/phenotype information to establish connections between targets and diseases, providing insights into potential therapeutic relationships.

2. 💊 **What drug-related information is gathered for the targets?**
   The code retrieves comprehensive drug information, including drug names, maximum clinical trial phases, and indications, from the ChEMBL database for the identified targets.

3. ⚙️ **How does the code identify interactors of a target with available drugs for specific diseases?**
   By utilizing the IntAct database and applying specific scoring thresholds, the code filters and analyzes the interactors of a target. It identifies interactors with available drugs for specific diseases, considering disease associations and clinical trial phases.

4. 📊 **How are drug annotations and association scores incorporated into the dataset?**
   The code annotates the dataset with drug-related information, including therapeutic areas and maximum clinical trial phases. Additionally, it incorporates association scores to quantify the strength of target-disease relationships.

5. 🚀 **What can be done with the generated dataset?**
   The code produces a curated dataset with drug annotations for target interactors, indicating the availability of drugs for specific diseases. Researchers and practitioners can leverage this dataset to explore potential drug targets, identify novel therapeutic options, and advance drug target discovery efforts.

## Getting Started

To get started with the code, follow the instructions below:

1. 🔗 Clone the repository:
