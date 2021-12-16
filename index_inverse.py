import json

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

conf = pyspark.SparkConf().setAppName('appName').setMaster('local')
sc = pyspark.SparkContext.getOrCreate(conf=conf)
sc.setLogLevel("ERROR")
spark = SparkSession(sc)


# Creation de la RDD
def createRdd():
    conf = SparkConf().setAppName("Test_RDD").setMaster("local")
    sc = SparkContext.getOrCreate(conf=conf)

    # Alternatively, a DataFrame can be created for a JSON dataset represented by
    # an RDD[String] storing one JSON object per string
    json_path = "monsters.json"
    json_file = open(json_path)
    json_object = json.load(json_file)
    monsterRdd = sc.parallelize(json_object)
    monster = spark.read.option( "multiLine",True).json(monsterRdd)
    monster.show()
    return monster


monster = createRdd()

#Affichage des la rdd
for ele in monster.collect():
    print(ele)


#Fonction pour la création de tupes (sort, nom créature)
def fun1(x):
    nomcreature=x[0]
    sorts = x[1]
    tuples = []
    for sort in sorts:
        tuples.append( ( sort.strip(),nomcreature.strip()))
    return tuples


print("----------------------")
output = monster.rdd.flatMap(lambda x: fun1(x))\
    .groupByKey()

spells = []
for elt in sorted(output.collect()):
    monster = {}
    monsters = []
    for elt2 in elt[1]:
        monsters.append(elt2)
    monster["name"] = elt[0]
    monster["monsters"] = monsters
    spells.append(monster)

with open("spells2.json", "w") as jsonFile:
    json.dump(spells, jsonFile, indent = 4)
