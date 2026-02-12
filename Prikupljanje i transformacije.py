# Databricks notebook source
# MAGIC %md
# MAGIC # Cuvanje transfomisanih datasetova u DBFS da bi se posle ucitali u drugom notebook-u
# MAGIC Celije ispod se pokrecu kao POSLEDNJE CELIJE u notebook-u!

# COMMAND ----------

# DBTITLE 1,CUVANJE - Fix DBFS error
#CUVANJE
path = 'dbfs:/moj_data/'
csv_name = path + 'dfsrb.csv'
dfsrb.write.csv(csv_name, header=True)

# COMMAND ----------

#CUVANJE
path = 'dbfs:/moj_data/'
csv_name = path + 'df.csv'
df.write.csv(csv_name, header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC # Učitavanje dataseta
# MAGIC Data set 1: Car Crashes, prikupljen sa Kaggle na linku: https://www.kaggle.com/datasets/joebeachcapital/car-crashes   
# MAGIC U pitanju je dataset koji opisuje saobraćajne nesreće na području New York City-ja  
# MAGIC Imena kolona pre transformacija: CRASH DATE, CRASH TIME, BOROUGH, ZIPCODE, LATITUDE, LONGITUDE, LOCATION, ON STREET NAME, OFF STREET NAME, CROSS STREET NAME...   
# MAGIC Data set 2: Saobraćajne nesreće na teritoriji Srbije, prikupljen sa linka: https://data.gov.rs/sr/datasets/podatsi-o-saobratshajnim-nezgodama-po-politsijskim-upravama-i-opshtinama/  
# MAGIC U pitanju je data set koji opisuje saobraćajne nesreće na teritoriji Srbije  
# MAGIC Imena kolona pre transformacija: ID NEZGODE, POLICIJSKA UPRAVA, OPSTINA, DATUM I VREME, GEOLOKACIJA, VRSTA SAOBRACAJNE NEZGODE,  TIP SAOBRACAJNE NEZGODE, BROJ VOZILA, STATUS LICA, DETALJNI OPIS SAOBRACAJNE NEZGODE
# MAGIC

# COMMAND ----------

import pandas as pd
from pyspark.sql.functions import to_date, date_format, col, to_timestamp, concat, lit, when, length, udf
from pyspark.sql.types import DateType, StringType
from random import randint

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sledece dve celije ucitavaju navedene data setove iz DBFS u dataframe-ove pomocu sparka
# MAGIC Koristio sam opciju da databricks sam generise ovaj kod tj. da tabelu napravi u notebook-u, te je sledeci kod automatski generisan
# MAGIC
# MAGIC Promenljive koje sadrže ove datasetove su respektivno df i dfsrb

# COMMAND ----------

# File location and type
file_location = "/Volumes/workspace/default/data/crashes_data.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "true"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(df)

# COMMAND ----------

# File location and type
file_location = "/Volumes/workspace/default/data/nez-opendata-2022-20230125.csv"
file_type = "csv"

# CSV options
infer_schema = "false"
first_row_is_header = "false"
delimiter = ","

# The applied options are for CSV files. For other file types, these will be ignored.
dfsrb = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("sep", delimiter) \
  .load(file_location)

display(dfsrb)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformacije prvog dataseta

# COMMAND ----------

# MAGIC %md
# MAGIC Prvo cu krenuti sa transformacijama imena kolona tj. headera

# COMMAND ----------

#DATASET1

#1 Promena imena svih kolona radi lakse sintakse, pre promene sva imena kolona su bili UPPER CASE sa whitespace-ovima izmedju, posle promene lowercase sa "_" kao delimiterom

for old_col in df.columns:
    new_col = old_col.lower().replace(' ', '_')
    df = df.withColumnRenamed(old_col, new_col)
#df = df.withColumnRenamed("CRASH DATE","crash_date")

#display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Koristim funkciju to_date da pretvorim kolonu crash_date koja je tretnutno samo string u datetype kolonu.

# COMMAND ----------

#transformacija 2
df = df.withColumn("crash_date", to_date(col("crash_date"), "MM/dd/yyyy"))
df.select('crash_date').show(5)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Kolonu crash_time cu morati da modifikujem da bi mogla posle da se pretvori u kompletam timestamp tip.

# COMMAND ----------

#transfocmacija 3 potrebna da bi se uskladio format kako bi se pretvorio u timestamp u sledecoj celiji
df = df.withColumn(
    "crash_time",
    when(length(col("crash_time")) < 5, concat(lit("0"), col("crash_time"))).otherwise(col("crash_time"))
)
df.select("crash_time").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Uz pomoc concat() funkcije spajam kolone crash_date i crash_time u jednu _timestamp_ kolonu sa prosledjenim formatom

# COMMAND ----------

#transformacija 4
df = df.withColumn("timestamp", to_timestamp(concat(col("crash_date"), lit(" "), col("crash_time")), "yyyy-MM-dd HH:mm"))
#df = df.withColumn("crash_time", to_timestamp(col("crash_time"), "HH:mm"))
df.select('crash_time').show(5)
df.select('timestamp').show(5)
df

# COMMAND ----------

# MAGIC %md
# MAGIC Kastovanje kolona kako bih postigao zeljenu shemu.

# COMMAND ----------

#Kastovanje ostalih kolona jer po defaultu su sve stringovi 5
df = df.withColumn('zip_code', col('zip_code').cast('integer'))
df = df.withColumn('latitude', col('latitude').cast('float'))
df = df.withColumn('longitude', col('longitude').cast('float'))
df = df.withColumn('number_of_persons_injured', col('number_of_persons_injured').cast('integer'))
df = df.withColumn('number_of_persons_killed', col('number_of_persons_killed').cast('integer'))
df = df.withColumn('number_of_pedestrians_injured', col('number_of_pedestrians_injured').cast('integer'))
df = df.withColumn('number_of_pedestrians_killed', col('number_of_pedestrians_killed').cast('integer'))
df = df.withColumn('number_of_cyclist_injured', col('number_of_cyclist_injured').cast('integer'))
df = df.withColumn('number_of_cyclist_killed', col('number_of_cyclist_killed').cast('integer'))
df = df.withColumn('number_of_motorist_injured', col('number_of_motorist_injured').cast('integer'))
df = df.withColumn('number_of_motorist_killed', col('number_of_motorist_killed').cast('integer'))
df = df.withColumn('collision_id', col('collision_id').cast('long'))
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Popunjavanje null vrednosti koristeci funkcije sa vezbi 8

# COMMAND ----------

#Popunjavanje null vrednosti 6
df = df.na.fill('-', ['on_street_name', 'cross_street_name', 'off_street_name'])
df = df.na.fill('-', ['borough', 'contributing_factor_vehicle_1', 'contributing_factor_vehicle_2', 'contributing_factor_vehicle_3', 'contributing_factor_vehicle_4', 'contributing_factor_vehicle_5'])
df = df.na.fill('-', ['vehicle_type_code_1', 'vehicle_type_code_2', 'vehicle_type_code_3','vehicle_type_code_4', 'vehicle_type_code_5'])
df.select("on_street_name").show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC Registrovanje User Defined Funkcije za spark  
# MAGIC Ova funkcija sredjuje string vrednosti u kolonama koje opisuju ulice na kojima se desio sudar

# COMMAND ----------

#moja UDF funkcija koju cu proslediti sparku za sredjivanje string vrednosti u kolonama koje opisuju ulice na kojima se desio sudar TRANS: 7
def formatiraj(s):
    if s != '-':
        words = s.split()
        result = []
        for word in words:
            if word.isdigit():
                result.append(word)
            else:
                x = word[0].upper() + word[1:].lower()
                result.append(x)
        return ' '.join(result)
    return s

#registrovanje User Defined funkcije za spark
formatirajUDF = udf(formatiraj, StringType())

# COMMAND ----------

# MAGIC %md
# MAGIC Upotreba moje UDF funckije na 4 kolone

# COMMAND ----------

#Koristim moju UDF funkciju 7

#df = df.withColumn("on_street_name", formatirajUDF(col("on_street_name")))
df = df.withColumn("on_street_name", when(col("on_street_name").isNotNull(), formatirajUDF(col("on_street_name"))).otherwise(col("on_street_name")))
df = df.withColumn("cross_street_name", when(col("cross_street_name").isNotNull(), formatirajUDF(col("cross_street_name"))).otherwise(col("cross_street_name")))
df = df.withColumn("off_street_name", when(col("off_street_name").isNotNull(), formatirajUDF(col("off_street_name"))).otherwise(col("off_street_name")))
df = df.withColumn("borough", when(col("borough").isNotNull(), formatirajUDF(col("borough"))).otherwise(col("borough")))

df.select("on_street_name").show(5)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC Formiranje kolona koje ce nositi ukupno brojcano stanje povredjenih i poginulih u sobracajnim nesrecama

# COMMAND ----------

#Pravim novu kolonu koja ce imati ukupan broj umrlih kao i povredjenih u saobracajnoj nesreci 8
df = df.withColumn("Total_fatalities", col("number_of_persons_killed") + col("number_of_pedestrians_killed") + col("number_of_cyclist_killed") + col('number_of_motorist_killed'))
df = df.withColumn("Total_injuries", col("number_of_persons_injured") + col("number_of_pedestrians_injured") + col("number_of_cyclist_injured") + col('number_of_motorist_injured'))
display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Posto nisam video usecase za kolonu _zipcode_ ovu kolonu cu odbaciti

# COMMAND ----------

# Dropujem kolonu zip_code 9
df = df.drop('zip_code')
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Transformacije drugog dataseta

# COMMAND ----------

# MAGIC %md
# MAGIC Promena defaultnih imena kolona koje sam dobio prilikom ucitavanja fajla

# COMMAND ----------

#1
dfsrb = dfsrb.withColumnRenamed('_c0', 'id')
dfsrb = dfsrb.withColumnRenamed('_c1', 'policijska_uprava')
dfsrb = dfsrb.withColumnRenamed('_c2', 'opstina')
dfsrb = dfsrb.withColumnRenamed('_c3', 'crash_date')
dfsrb = dfsrb.withColumnRenamed('_c4', 'longitude')
dfsrb = dfsrb.withColumnRenamed('_c5', 'latitude')
dfsrb = dfsrb.withColumnRenamed('_c6', 'damage')
dfsrb = dfsrb.withColumnRenamed('_c7', 'broj_vozila')
dfsrb = dfsrb.withColumnRenamed('_c8', 'descr')
display(dfsrb)
dfsrb.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Koristim funkcije to_date() i to_timestamp() za promenu tipa odgovarajucih kolona

# COMMAND ----------

#
dfsrb = dfsrb.withColumn('timestamp' , to_timestamp(col('crash_date'), "dd.MM.yyyy,HH:mm"))
dfsrb = dfsrb.withColumn("crash_date", to_date(col("crash_date"), "dd.MM.yyyy,HH:mm"))
dfsrb.select('crash_date').show(5)
display(dfsrb)

# COMMAND ----------

distinct_values = dfsrb.select("broj_vozila").distinct()
distinct_values.show()

# COMMAND ----------

# MAGIC %md
# MAGIC Kastovanje kolona da bih postigao zeljenu schemu.

# COMMAND ----------

#3 Kastovanje ostalih kolona
dfsrb = dfsrb.withColumn("id", col('id').cast('long'))
dfsrb = dfsrb.withColumn("policijska_uprava", col('policijska_uprava').cast('string'))
dfsrb = dfsrb.withColumn("opstina", col('opstina').cast('string'))
dfsrb = dfsrb.withColumn("longitude", col('longitude').cast('float'))
dfsrb = dfsrb.withColumn("latitude", col('latitude').cast('float'))
dfsrb = dfsrb.withColumn("damage", col('damage').cast('string'))
dfsrb = dfsrb.withColumn("broj_vozila", col('broj_vozila').cast('string'))
dfsrb.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Koristim prethodno definisanu UDF funckiju da sredim string kolone i u ovom datasetu.

# COMMAND ----------

dfsrb = dfsrb.withColumn("policijska_uprava", when(col("policijska_uprava").isNotNull(), formatirajUDF(col("policijska_uprava"))).otherwise(col("policijska_uprava")))
dfsrb = dfsrb.withColumn("opstina", when(col("opstina").isNotNull(), formatirajUDF(col("opstina"))).otherwise(col("opstina")))
display(dfsrb)

# COMMAND ----------

# MAGIC %md
# MAGIC Kolonu koja daje grub opis saobracajne nezgode cu odbaciti

# COMMAND ----------

# 5 dropujem kolonu za opis saobracajne nezgode
dfsrb = dfsrb.drop('descr')
display(dfsrb)

# COMMAND ----------

display(dfsrb)

# COMMAND ----------

