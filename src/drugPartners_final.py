#### ####
import findspark

findspark.init("/opt/homebrew/Cellar/apache-spark/3.3.0/libexec")

import pyspark
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
import pandas as pd

from psutil import virtual_memory
from pyspark import SparkFiles
from pyspark.conf import SparkConf


def detect_spark_memory_limit():
    """Spark does not automatically use all available memory on a machine. When working on large datasets, this may
    cause Java heap space errors, even though there is plenty of RAM available. To fix this, we detect the total amount
    of physical memory and allow Spark to use (almost) all of it."""
    mem_gib = virtual_memory().total >> 30
    return int(mem_gib * 0.9)


spark_mem_limit = detect_spark_memory_limit()
spark_conf = (
    SparkConf()
    .set("spark.driver.memory", f"{spark_mem_limit}g")
    .set("spark.executor.memory", f"{spark_mem_limit}g")
    .set("spark.driver.maxResultSize", "0")
    .set("spark.debug.maxToStringFields", "2000000000")
    .set("spark.sql.execution.arrow.maxRecordsPerBatch", "500000")
    .set("spark.sql.execution.arrow.pyspark.enabled", "true")
    .set("spark.ui.showConsoleProgress", "false")
)


spark = (
    SparkSession.builder.config(conf=spark_conf)
    .master("local[*]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.driver.host", "localhost")  ### Run locally
    .getOrCreate()
)


### Load datasets (target, interact db and molecule) with the last downloaded version (december 2022)

### https://platform.opentargets.org/downloads/data:

### Target - molecular interactions dataset
interactors_path = "pathToInteraction"
interact_db = spark.read.parquet(interactors_path)

### Disease/Phenotype dataset
disease_path = "pathToDisease"
diseases = spark.read.parquet(disease_path)

### Target dataset
target_path = "pathToTarget"
target = spark.read.parquet(target_path)

### Drug dataset
molecule_path = "pathToMolecule"
molecule = spark.read.parquet(molecule_path)

### Drug - indications dataset
indication_path = "pathToIndication"
indication = spark.read.parquet(indication_path)

### load symbols to complement EnsemblID
symbol = target.select("id", "approvedSymbol")

### take diseases id with therapeutic areas
diseases_name = diseases.select(
    F.col("id").alias("efoDisease"),
    F.col("name").alias("diseaseName"),
    F.col("therapeuticAreas"),  ### eliminar therapeutic areas
)

### Load Direct Score data
overallDirecAssocScore_path = "pathToAssociationByOverallDirect"
overallDirecAssocScore = spark.read.parquet(overallDirecAssocScore_path)
### annotate Direct score
oaDirectScore = overallDirecAssocScore.select(
    F.col("targetId").alias("targetIdoaDirect"),
    F.col("diseaseId").alias("diseaseIdDirect"),
    F.col("score").alias("scoreDirect"),
)

### Load Direct overall
direcAssoc_path = "pathToAssociationByDatasourceDirect/"
direcAssoc = spark.read.parquet(direcAssoc_path)
### annotate which are the supporting datasourceId
targetDirectAssoc = (
    direcAssoc.groupBy("targetId", "diseaseId")
    .agg(F.collect_set("datasourceId").alias("datasourceId"))
    .select(
        F.col("targetId").alias("targetIdDirect"),
        F.col("diseaseId").alias("diseaseIdDirect"),
        F.col("datasourceId").alias("datasourceIdDirect"),
    )
)

##### Dataset format curation #####
### Read original dataset with all target-trait pairs per study
path = "pathToMRDataset"
df = spark.read.csv(path, sep=r"\t", header=True)

### convert from string to array
convert = [
    "adverse_effects",
    "adverse_effects_studies",
    "adverse_effects_bxy",
    "mech",
    "mech_studies",
    "mech_coloc_h4",
    "mech_bxy",
]
### transform from multiple string to one
transform = ["adverse_effects_coloc_h4", "outcome_trait"]
df = df.withColumn("index", F.monotonically_increasing_id())

### cleaning some columns with messy format
df_clean = (
    df.select(
        *[F.split(col, ";").alias(col) if col in convert else col for col in df.columns]
    )
    .select(
        *[
            F.concat(F.lit('"'), som, F.lit('"')).alias(som)
            if som in transform
            else som
            for som in df.columns
        ]
    )
    .drop("id")
)

### Dataframe of target-efo_terms to get annotations
queryset = (
    df_clean.select(
        F.col("curated_ensid").alias("ensid"),
        "index",
        "mergedOutcomeTraitEfo2",
        "protein_datasets",
        "outcome_datasets",
    )
    ### Build efo terms as array to explode later
    .withColumn("efo_array", F.split(F.col("mergedOutcomeTraitEfo2"), ", "))
    .withColumn("efo_ensid", F.explode_outer(F.col("efo_array")))
    .join(
        diseases_name,
        F.col("efoDisease") == F.col("efo_ensid"),
        "left",
    )
    ### add supporting direct association datasources
    .join(
        targetDirectAssoc,
        (targetDirectAssoc.targetIdDirect == F.col("ensid"))
        & (targetDirectAssoc.diseaseIdDirect == F.col("efo_ensid")),
        "left",
    )
    ### add direct association scores
    .join(
        oaDirectScore,
        (F.col("ensid") == oaDirectScore.targetIdoaDirect)
        & (F.col("efo_ensid") == oaDirectScore.diseaseIdDirect),
        "left",
    )
    .groupBy(F.col("index"), F.col("ensid"))
    .agg(
        F.collect_set(F.col("datasourceIdDirect")).alias("datasourceIdDirect"),
        F.collect_set(F.col("scoreDirect")).alias("scoreDirect"),
        F.collect_set(F.col("efo_ensid")).alias("efo_ensid"),
    )
).repartition(20)

### get maxClinPhase for every indication
indicationsToJoin = indication.select(
    "id", F.explode_outer(F.col("indications")).alias("indications")
).select(
    "id",
    F.col("indications.disease").alias("indicatedDisease"),
    F.col("indications.efoName").alias("indicatedEfoName"),
    F.col("indications.maxPhaseForIndication").alias("indicatedMaxPhaseIndication"),
)
### obtaining targets related to chembl from chembl
tar_group = (
    molecule.select(
        F.col("id").alias("chemblIdTargetB"),
        F.col("name").alias("drugNameTargetB"),
        F.col("maximumClinicalTrialPhase").alias("maxClinTrialPhaseTargetB"),
        F.col("linkedDiseases"),
        F.explode_outer("linkedTargets.rows").alias("chemblLinkedTargetB"),
    )
    .select(
        F.col("chemblIdTargetB"),
        F.col("drugNameTargetB"),
        F.col("maxClinTrialPhaseTargetB"),
        F.col("chemblLinkedTargetB"),
        F.explode_outer("linkedDiseases.rows").alias("diseaseLinkedChemblTargetB"),
    )
    .join(
        indicationsToJoin,
        (indicationsToJoin.id == F.col("chemblIdTargetB"))
        & (indicationsToJoin.indicatedDisease == F.col("diseaseLinkedChemblTargetB")),
        "left",
    )
    .groupBy(
        "chemblLinkedTargetB",
        "diseaseLinkedChemblTargetB",
        "drugNameTargetB",
        F.col("indicatedDisease").alias("indicatedDiseaseB"),
        "maxClinTrialPhaseTargetB",  ### remove
        F.col("indicatedMaxPhaseIndication").alias("indicatedMaxPhaseIndicationB"),
    )
    .agg(F.count("chemblLinkedTargetB"))
)

### Get interactors using IntAct database
# filter by 0.42 score & add linked CHEMBL (tar_group) of the partners
def drug_partners(df_clean, interact_db, queryset, tar_group, symbol):
    drugPartners = (
        interact_db.filter(F.col("sourceDatabase") == "intact")
        .select("sourceDatabase", "targetA", "targetB", "scoring")
        .filter(F.col("scoring") > "0.42")
        ### get partners (targetB) of ensid(targetA)
        .join(
            queryset.select("index", "ensid", "efo_ensid"),
            queryset.ensid == F.col("targetA"),
            "right",
        )
        ### get indications with clin phases for targetB
        .join(tar_group, F.col("targetB") == tar_group.chemblLinkedTargetB, "left")
        ### targetB's names
        .join(
            symbol.select("id", F.col("approvedSymbol").alias("approvedSymbolPartner")),
            F.col("targetB") == symbol.id,
            "left",
        )
        .drop("id")
        ### if disease partner is the same as disease of targetA, get partner names,
        ### drug name, max clin phase for the indication and indication name
        .withColumn(
            "drugFromB_efo",
            F.when(
                (F.col("ensid") != F.col("targetB"))
                & (F.col("targetB").isNotNull())
                & (F.col("diseaseLinkedChemblTargetB").isNotNull())
                & (
                    F.array_contains(
                        F.col("efo_ensid"), F.col("diseaseLinkedChemblTargetB")
                    )
                ),
                F.concat_ws(
                    "_",
                    F.col("approvedSymbolPartner"),
                    F.col("drugNameTargetB"),
                    F.col("indicatedMaxPhaseIndicationB"),
                    F.col("diseaseLinkedChemblTargetB"),
                ),
            ).otherwise(F.lit(None)),
        )
        .groupBy(
            ### group by target & index (can be only by index) and take
            # all partners drug annotations
            F.col("ensid"),
            F.col("index").alias("indexJoin"),
        )
        .agg(
            F.collect_set(F.col("drugFromB_efo")).alias("drugFromB_trait"),
        )
        ### add to query list
        .join(queryset, queryset.index == F.col("indexJoin"), "right")
        .drop("index")
        ### add drug annotations to original cleaned dataframe
        .join(df_clean, df_clean.index == F.col("indexJoin"), "right")
    )
    return drugPartners.toPandas().to_csv("drugPartnersMR.csv")


drug_partners(df_clean, interact_db, queryset, tar_group, symbol)
