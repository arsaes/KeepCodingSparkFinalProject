// Databricks notebook source
// MAGIC %md
// MAGIC # Lectura de datos
// MAGIC Cargamos los datos infiriendo su schema.

// COMMAND ----------

val dfHappiness2021 = spark.read.option("header", "true")
  .option("inferSchema", "true")
  .format("csv").load("dbfs:/FileStore/proyectofinal/data/world_happiness_report_2021.csv")

val dfHappiness = spark.read.option("header", "true")
  .option("inferSchema", "true")
  .format("csv").load("dbfs:/FileStore/proyectofinal/data/world_happiness_report.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC Comprobamos que los tipos de las columnas se han inferido correctamente.

// COMMAND ----------

display(dfHappiness2021.limit(5))

// COMMAND ----------

dfHappiness2021.printSchema

// COMMAND ----------

display(dfHappiness.limit(5))

// COMMAND ----------

dfHappiness.printSchema

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio 1
// MAGIC **¿Cuál es el país más "feliz" del 2021 según la data? (considerar la columna "Ladder score")**

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val result1 = dfHappiness2021.select(
  $"Country name",
  $"Ladder score"
).orderBy(desc("Ladder score"))
  .limit(1)

result1.cache
result1.show

// COMMAND ----------

// MAGIC %md
// MAGIC El país más feliz del 2021 fue Finlandia.

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio 2
// MAGIC **¿Cuál es el país más "feliz" del 2021 por continente según la data?**

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos un DF asignando cada región a su continente.

// COMMAND ----------

val dfContinents = Seq(("Western Europe", "Europe"), ("North America and ANZ", "America"),
  ("Middle East and North Africa", "Africa"), ("Latin America and Caribbean", "America"),
  ("Central and Eastern Europe", "Europe"), ("East Asia", "Asia"),
  ("Southeast Asia", "Asia"), ("Commonwealth of Independent States", "Asia"),
  ("Sub-Saharan Africa", "Africa"), ("South Asia","Asia")).toDF("Regional Indicator", "Continent")

dfContinents.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Hacemos join con el DF del 2021 y el de regiones para obtener el continente de cada entrada.
// MAGIC Comprobamos que no se hayan omitido registros.

// COMMAND ----------

val dfJoin2021 = dfHappiness2021.join(dfContinents, Seq("Regional Indicator"))
display(dfJoin2021.limit(5))

// COMMAND ----------

println("Antes del join: " + dfHappiness2021.count)
println("Después del join: " + dfJoin2021.count)

// COMMAND ----------

// MAGIC %md
// MAGIC Usamos una ventana para agrupar por continente y obtener el país con mayor puntuación.

// COMMAND ----------

import org.apache.spark.sql.expressions.Window

// COMMAND ----------

val windowByContinent2021 = Window.partitionBy("Continent").orderBy(desc("Ladder score"))

val result2 = dfJoin2021.withColumn("Rank", rank().over(windowByContinent2021))
  .filter($"Rank" === 1)
  .select(
    "Continent",
    "Country name",
    "Ladder score"
  )

result2.cache
result2.show(false)

// COMMAND ----------

// MAGIC %md
// MAGIC Podemos ver que Finlandia es el país más feliz en Europa, Israel en África, Nueva Zelanda en América y Taiwan en Asia.

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio 3
// MAGIC **¿Cuál es el país que más veces ocupó el primer lugar en todos los años?**

// COMMAND ----------

// MAGIC %md
// MAGIC Comprobamos qué años están contenidos en el DF general.

// COMMAND ----------

dfHappiness.select("year").distinct.orderBy("year").show

// COMMAND ----------

// MAGIC %md
// MAGIC Tenemos desde el 2005 hasta el 2020, para añadir el 2021 daremos el mismo formato al dataframe de ese año y los uniremos.

// COMMAND ----------

val dfHappinessYears = dfHappiness.select($"Country name", $"year", $"Life Ladder")
dfHappinessYears.show(5)

// COMMAND ----------

val dfHappinessYear2021 = dfHappiness2021.select($"Country name", lit(2021).as("year"), $"Ladder score".as("Life Ladder"))
dfHappinessYear2021.show(5)

// COMMAND ----------

val dfUnionHappiness = dfHappinessYears.union(dfHappinessYear2021)
println("Suma de los registros: " + (dfHappinessYears.count + dfHappinessYear2021.count))
println("Registros de la unión: " + dfUnionHappiness.count)

// COMMAND ----------

// MAGIC %md
// MAGIC Ahora disponemos de todos los datos en un mismo DF. Agrupamos por año y obtenemos el país con más puntuación en cada año. Luego contamos las veces que aparece cada país en el DF resultante.

// COMMAND ----------

val windowByYearHappiness = Window.partitionBy("year").orderBy(desc("Life Ladder"))

val result3 = dfUnionHappiness.withColumn("Rank", rank().over(windowByYearHappiness))
  .filter($"Rank" === 1)
  .groupBy("Country name").count().withColumnRenamed("count", "Times first")
  .orderBy(desc("Times first"))
  .limit(2)

result3.cache
result3.show

// COMMAND ----------

// MAGIC %md
// MAGIC Tanto Finlandia como Dinamarca empatan en primer lugar como los que más veces han sido considerados más felices, con 7 victorias cada uno.

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio 4
// MAGIC **¿Qué puesto de Felicidad tiene el país con mayor GDP del 2020?**

// COMMAND ----------

// MAGIC %md
// MAGIC Hacemos ranking para cada año según la felicidad y el GDP. Filtramos el de 2020 con mayor GDP y obtenemos el resultado.

// COMMAND ----------

val windowByYearGDP = Window.partitionBy("year").orderBy(desc("Log GDP per capita"))

val result4 = dfHappiness.withColumn("GDP Rank", rank().over(windowByYearGDP))
  .withColumn("Life Ladder Rank", rank().over(windowByYearHappiness))
  .filter($"year" === 2020 && $"GDP Rank" === 1)
  .select("Country name", "Life Ladder Rank", "Log GDP per capita")

result4.cache
result4.show

// COMMAND ----------

// MAGIC %md
// MAGIC El país con mayor GDP del 2020, que fue Irlanda, fue el número 13 en el ranking de los más felices.

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio 5
// MAGIC **¿En qué porcentaje ha variado a nivel mundial el GDP promedio del 2020 respecto al 2021? ¿Aumentó o disminuyó?**

// COMMAND ----------

// MAGIC %md
// MAGIC Calculamos la media para ambos años, que están en DF distintos.

// COMMAND ----------

val dfAvgGDP2020 = dfHappiness.filter($"year" === 2020).agg(
  avg($"Log GDP per capita")
).withColumnRenamed("avg(Log GDP per capita)", "Avg GDP 2020")

// COMMAND ----------

val dfAvgGDP2021 = dfHappiness2021.agg(
  avg($"Logged GDP per capita")
).withColumnRenamed("avg(Logged GDP per capita)", "Avg GDP 2021")

// COMMAND ----------

// MAGIC %md
// MAGIC Juntamos los DF, calculamos el porcentaje de cambio y si ha aumentado o disminuido.

// COMMAND ----------

val result5 = dfAvgGDP2020.join(dfAvgGDP2021)
  .withColumn("Difference percentage", (($"Avg GDP 2020" - $"Avg GDP 2021") / $"Avg GDP 2021") * 100)
  .withColumn("Type of change", when($"Difference percentage" > 0, "Increased").when($"Difference percentage" === 0, "Unchanged").otherwise("Decreased"))

result5.cache
result5.show

// COMMAND ----------

// MAGIC %md
// MAGIC Con respecto al 2021, el GDP aumentó un 3.38% en el año 2020.

// COMMAND ----------

// MAGIC %md
// MAGIC # Ejercicio 6
// MAGIC **¿Cuál es el país con mayor expectativa de vida ("Healthy life expectancy at birth")? ¿Y cuánto tenía en ese indicador en el 2019?**
// MAGIC
// MAGIC *Evaluar para el último año disponible y el promedio de los últimos 5 años.*

// COMMAND ----------

// MAGIC %md
// MAGIC Primero creamos un DF unificado con el general y el de 2021.

// COMMAND ----------

val dfExpectancy = dfHappiness.select($"year", $"Country name", $"Healthy life expectancy at birth")
val dfExpectancy2021 = dfHappiness2021.select(lit(2021), $"Country name", $"Healthy life expectancy".as("Healthy life expectancy at birth"))
val dfUnionExpectancy = dfExpectancy.union(dfExpectancy2021)
dfUnionExpectancy.show(5)

// COMMAND ----------

// MAGIC %md
// MAGIC ## En el último año

// COMMAND ----------

// MAGIC %md
// MAGIC Obtenemos el último año.

// COMMAND ----------

val lastYear = dfUnionExpectancy.agg(max(col("year")))
  .limit(1)
  .take(1)(0).getAs[Int]("max(year)")

// COMMAND ----------

// MAGIC %md
// MAGIC Creamos una ventana que agrupe por año y ordene por la esperanza de vida, obtenemos el mayor en el último año.

// COMMAND ----------

val windowByYearExpectancy = Window.partitionBy("year").orderBy(desc("Healthy life expectancy at birth"))

val dfRankedByYearExpectancy = dfUnionExpectancy.withColumn("Rank", rank().over(windowByYearExpectancy))
dfRankedByYearExpectancy.cache

val result6a = dfRankedByYearExpectancy
  .filter($"Rank" === 1 && $"year" === lastYear)
  .drop("Rank")

result6a.cache
result6a.show

// COMMAND ----------

// MAGIC %md
// MAGIC Singapur tenía mayor esperanza de vida en el 2021, el último año del que disponemos datos.

// COMMAND ----------

// MAGIC %md
// MAGIC ## En los últimos 5 años

// COMMAND ----------

// MAGIC %md
// MAGIC Filtramos los últimos 5 años, agrupamos por país y obtenemos el de mayor valor.

// COMMAND ----------

val result6b = dfUnionExpectancy.filter($"year".between(lastYear - 4, lastYear))
  .groupBy("Country name").avg("Healthy life expectancy at birth")
  .withColumnRenamed("avg(Healthy life expectancy at birth)", "Average life expectancy")
  .orderBy(desc("Average life expectancy"))
  .limit(1)

result6b.cache
result6b.show

// COMMAND ----------

// MAGIC %md
// MAGIC Singapur también tuvo mayor esperanza de vida media en los últimos 5 años de los que disponemos datos.

// COMMAND ----------

// MAGIC %md
// MAGIC ## En 2019

// COMMAND ----------

// MAGIC %md
// MAGIC Por un lado obtenemos un DF con el país del último año con mayor puntuación.
// MAGIC Por otro lado obtenemos las entradas del 2019. Hacemos join de ambos por el país y nos queda la entrada del que nos interesa.

// COMMAND ----------

val result6c = result6a.select("Country name")
  .join(dfRankedByYearExpectancy.filter($"year" === 2019), Seq("Country name"))
  .drop("Rank").drop("year")

result6c.cache
dfRankedByYearExpectancy.unpersist
result6c.show

// COMMAND ----------

// MAGIC %md
// MAGIC La media de esperanza de vida en Singapur para el año 2019 es de 77,1 años.

// COMMAND ----------

// MAGIC %md
// MAGIC # Almacenamiento de los resultados
// MAGIC Guardaremos los DF obtenidos en los distintos ejercicios para poder disponer de ellos más adelante.

// COMMAND ----------

val outputDirectory = "dbfs:/FileStore/proyectofinal/output/"

// Ejercicio 1
result1.write.option("header", true)
  .mode(SaveMode.Ignore)
  .csv(outputDirectory + "ejercicio1")
result1.unpersist

// Ejercicio 2
result2.write.option("header", true)
  .mode(SaveMode.Ignore)
  .csv(outputDirectory + "ejercicio2")
result2.unpersist

// Ejercicio 3
result3.write.option("header", true)
  .mode(SaveMode.Ignore)
  .csv(outputDirectory + "ejercicio3")
result3.unpersist

// Ejercicio 4
result4.write.option("header", true)
  .mode(SaveMode.Ignore)
  .csv(outputDirectory + "ejercicio4")
result4.unpersist

// Ejercicio 5
result5.write.option("header", true)
  .mode(SaveMode.Ignore)
  .csv(outputDirectory + "ejercicio5")
result5.unpersist

// Ejercicio 6
result6a.write.option("header", true)
  .mode(SaveMode.Ignore)
  .csv(outputDirectory + "ejercicio6a")
result6a.unpersist

result6b.write.option("header", true)
  .mode(SaveMode.Ignore)
  .csv(outputDirectory + "ejercicio6b")
result6b.unpersist

result6c.write.option("header", true)
  .mode(SaveMode.Ignore)
  .csv(outputDirectory + "ejercicio6c")
result6c.unpersist

// COMMAND ----------

// MAGIC %md
// MAGIC # Comprobación del almacenamiento
// MAGIC Leemos los datos guardados para asegurarnos de que se han almacenado bien.

// COMMAND ----------

val dfRead1 = spark.read.option("header", "true")
  .option("inferSchema", "true")
  .format("csv").load(outputDirectory + "ejercicio1")

dfRead1.show

// COMMAND ----------

val dfRead2 = spark.read.option("header", "true")
  .option("inferSchema", "true")
  .format("csv").load(outputDirectory + "ejercicio2")

dfRead2.show(false)

// COMMAND ----------

val dfRead3 = spark.read.option("header", "true")
  .option("inferSchema", "true")
  .format("csv").load(outputDirectory + "ejercicio3")

dfRead3.show

// COMMAND ----------

val dfRead4 = spark.read.option("header", "true")
  .option("inferSchema", "true")
  .format("csv").load(outputDirectory + "ejercicio4")

dfRead4.show

// COMMAND ----------

val dfRead5 = spark.read.option("header", "true")
  .option("inferSchema", "true")
  .format("csv").load(outputDirectory + "ejercicio5")

dfRead5.show

// COMMAND ----------

val dfRead6a = spark.read.option("header", "true")
  .option("inferSchema", "true")
  .format("csv").load(outputDirectory + "ejercicio6a")

dfRead6a.show

// COMMAND ----------

val dfRead6b = spark.read.option("header", "true")
  .option("inferSchema", "true")
  .format("csv").load(outputDirectory + "ejercicio6b")

dfRead6b.show

// COMMAND ----------

val dfRead6c = spark.read.option("header", "true")
  .option("inferSchema", "true")
  .format("csv").load(outputDirectory + "ejercicio6c")

dfRead6c.show

// COMMAND ----------

// MAGIC %md
// MAGIC Podemos comprobar que todos los DF de los resultados se han guardado correctamente y los hemos podido leer.

// COMMAND ----------


