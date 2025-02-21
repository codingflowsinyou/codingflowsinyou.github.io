## DataFrames

#### ¿Qué es?
Es una colección de datos organizados en columnas, de manera análoga a la base de datos relacional.

Su origen proviene de un objeto experimental introducido en Apache Spark 1.0 llamado *SchemaRDD* 

Para los usuarios familiarizados con R o Python, un Spark DataFrame es un concepto similar que permite que todos los usuarios trabajen fácilmente con estructuras de datos como tablas.

Debido a la estructura que tienen, permite a los usuarios hacer queries en Spark SQL.

Nota1: en antiguas versiones de Spark, ejecutar queries en Python resultaba muy costoso computacionalmente debido a la comunicación entre JVM y Py4j.

Nota2: En antiguas versiones de Spark, para ejecutar queries se utilizaba *SQL Context*, ahora se ha unificado *HiveContext*, *SQLContext*, *StreamingContext* y *SparkContext* en *SparkSession*. Más información How to use SparkSession in Apache Spark 2.0(http://bit.ly/2br0Fr1).


#### Python to RDD communications
En el siguiente diagrama se muestra que en el driver de PySpark, Spark Context utiliza Py4j para lanzar la JVM utilizando JavaSparkContext. Cualquier transformación de RDD son inicialmente PythonRDD objetos en Java.

Una vez esas tasks abandonan los workers, objetos PythonRDD lanzan subprocesos utilizando pipes para enviar tanto código como datos y así ser procesados dentro de Python.
(diagrama hoja papel)

<img src="python_spark.png">


Mientras que esta aproximación permite a PySpark distribuir el procesamiento de los datos en múltiples Python subprocesses en multiple workers, como se puede observar, hay muchísima comunicación intercambiandose entre Python y JVM.

Spark performance beyond the JVM: http://bit.ly/2bx89bn.

#### Spark SQL

Spark SQL es un módulo de Spark para el procesamiento de datos estructurados. A diferencia de la API básica de Spark RDD, las interfaces proporcionadas por Spark SQL brindan a Spark más información sobre la estructura tanto de los datos como del cálculo que se está realizando. Internamente, Spark SQL usa esta información adicional para realizar optimizaciones adicionales. Hay varias formas de interactuar con Spark SQL, incluidas SQL y la API de conjunto de datos. Al calcular un resultado, se utiliza el mismo motor de ejecución, independientemente de qué API / lenguaje esté utilizando para expresar el cálculo. Esta unificación significa que los desarrolladores pueden alternar fácilmente entre diferentes API en función de cuál proporciona la forma más natural de expresar una transformación determinada.

Un uso de Spark SQL es ejecutar consultas SQL. Spark SQL también se puede utilizar para leer datos de una instalación de Hive existente.  Cuando se ejecuta SQL desde otro lenguaje de programación, los resultados se devolverán como un Dataset / DataFrame.

A Spark SQL se le llama **Catalyst Optimizer** 
+ info en Cost-based Optimizer Framework at https://issues.apache.org/jira/browse/SPARK-16026
the Design Specification of Spark Cost-Based Optimization at http://bit.ly/2li1t4T.

Falta desarrollo.
**Proyect Tungsten** hay una gran cantidad de mejoras de la performance generando a byte code (code generation or codegen) en vez de interpretar cada row de los datos. 
El optimizador está basado en constructores de programación funcional y fue diseñado con dos propósitos en mente: facilitar la adición de nuevas técnicas de optimización y características de Spark SQL, y permitir a desarrolladores externos extender y mejorar el optimizador.
Structuring Spark: SQL DataFrames, Datasets, and Streaming at http://bit.ly/2cJ508x.]]**




# Creating DataFrames



```python
#### arreglos
import os
os.environ['PYSPARK_PYTHON'] = '/opt/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON']='/opt/anaconda3/bin/python'
```


```python
# inicializar
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()
```

#### Generar JSON data
Generaremos inicialmente un `stringJSONRDD`


```python
stringJSONRDD = sc.parallelize(("""
  { "id": "123",
"name": "Katie",
"age": 19,
"eyeColor": "brown"
  }""",
"""{
"id": "234",
"name": "Michael",
"age": 22,
"eyeColor": "green"
  }""", 
"""{
"id": "345",
"name": "Simone",
"age": 23,
"eyeColor": "blue"
  }""")
)


```

#### Creating a DataFrame


```python
swimmersJSON = spark.read.json(stringJSONRDD)
```

#### Creating a temporary table



```python
# desaparecerá si la sesión se cierra
swimmersJSON.createOrReplaceTempView("swimmersJSON")
```

Para entender mejor el funcionamiento visualmente:
Understanding Your Apache Spark Application Through Visualization at http://bit.ly/2cSemkv.

Es importante entender que `paralellize`, `map`y `mapPartitions` son todos RDD transformaciones. En este caso, es importante entender que `spark.read.json` no son solo RDDs transformations, también son acciones para convertir el RDD en dataframes. Esto es importante, ya que si estás ejecutando una DataFrame operation, para debug tus operaciones necesitarás entender que estarás haciendo RDD operaciones dentro de Spark UI.

#### Simple DataFrame queries
Una vez tenemos creado el swimmersJSON, podremos ejecutar el DataFrame API 


```python
# Running the .show() method will default to present the first 10 rows
# DataFrame API
swimmersJSON.show()
```

    +---+--------+---+-------+
    |age|eyeColor| id|   name|
    +---+--------+---+-------+
    | 19|   brown|123|  Katie|
    | 22|   green|234|Michael|
    | 23|    blue|345| Simone|
    +---+--------+---+-------+
    


#### SQL query
Se puede escribir en SQL statements


```python
spark.sql("SELECT * FROM swimmersJSON").collect()
```




    [Row(age=19, eyeColor='brown', id='123', name='Katie'),
     Row(age=22, eyeColor='green', id='234', name='Michael'),
     Row(age=23, eyeColor='blue', id='345', name='Simone')]



#### Interoperating with RDDs
##### 1. Inferring the schema using reflection
En el proceso de construir un DataFrame y hacer queries, olvidemonos de la idea de que el esquema del DataFrame estaba automáticamente definido. Inicialmente, las rows objects están construidas pasando una lista de key/value pairs como ** kwars . Entonces, SparkSQL convierte este RDD de row en un DataFrame donde las keys son las columnas y los datatypes están inferidos by sampling the data.


```python
# print the schema
swimmersJSON.printSchema()
```

    root
     |-- age: long (nullable = true)
     |-- eyeColor: string (nullable = true)
     |-- id: string (nullable = true)
     |-- name: string (nullable = true)
    


##### 2. Programmatically specifying the schema
Vamos a programar el esquema con ayuda de los SparkSQL types.


```python
# Import types
from pyspark.sql.types import *
```


```python
# Generate comma delimited
stringCSVRDD = sc.parallelize([
(123, 'Katie', 19, 'brown'), 
(234, 'Michael', 22, 'green'), 
(345, 'Simone', 23, 'blue')
])

```

Primero, we will encode the schema as a string, per the [schema] variable below. Then we will define the schema using `StructType` and `StructField`.



```python
# Specify schema
schema = StructType([
StructField("id", LongType(), True),    
StructField("name", StringType(), True),
StructField("age", LongType(), True),
StructField("eyeColor",
StringType(), True)
])
```

Notar que `StructField` class se divide en términos de:
- name: el nombre del campo
- dataType: the dataType of this field.
- nullable: Indica si el campo puede ser nulo o no.


```python
# Apply the schema to the RDD and Create DataFrame
swimmers = spark.createDataFrame(stringCSVRDD, schema)
```


```python
# Creates a temporary view using the DataFrame
swimmers.createOrReplaceTempView("swimmers")

```


```python
swimmers.printSchema()
```

    root
     |-- id: long (nullable = true)
     |-- name: string (nullable = true)
     |-- age: long (nullable = true)
     |-- eyeColor: string (nullable = true)
    


#### Querying with the DataFrame API
##### Number of rows
Utilizamos `count()` method.


```python
swimmers.count()
```




    3



##### Running filter statements
Utilizamos `filter` method.


```python
# Get the id, age where age = 22
swimmers.select("id", "age").filter("age = 22").show()
```

    +---+---+
    | id|age|
    +---+---+
    |234| 22|
    +---+---+
    



```python
# Another way to write the above query is below
swimmers.select(swimmers.id, swimmers.age).filter(swimmers.age == 22).show()
```

    +---+---+
    | id|age|
    +---+---+
    |234| 22|
    +---+---+
    



```python
# Get the name, eyeColor where eyeColor like 'b%'
swimmers.select("name", "eyeColor").filter("eyeColor like 'b%'").show()
```

    +------+--------+
    |  name|eyeColor|
    +------+--------+
    | Katie|   brown|
    |Simone|    blue|
    +------+--------+
    


#### Querying with SQL
##### Number of rows


```python
spark.sql("select count(1) from swimmers").show()
```

    +--------+
    |count(1)|
    +--------+
    |       3|
    +--------+
    


##### Running filter statements using the where Clauses



```python
# Get the id, age where age = 22 in SQL
spark.sql("select id, age from swimmers where age = 22").show()



```

    +---+---+
    | id|age|
    +---+---+
    |234| 22|
    +---+---+
    



```python
spark.sql("select name, eyeColor from swimmers where eyeColor like 'b%'").show()
```

    +------+--------+
    |  name|eyeColor|
    +------+--------+
    | Katie|   brown|
    |Simone|    blue|
    +------+--------+
    


Nota importante: si alguna vez queremos guardar una tabla y que preserve las condiciones iniciales lo mejor es guardarlo en `Parquet files`
For more information, please refer to the latest Spark SQL Programming Guide > Parquet Files at: http://spark.apache.org/docs/latest/sql-programming-guide.html#parquet-files.

Automatic Partition Discovery and Schema Migration for Parquet at https://databricks.com/blog/2015/03/24/spark-sql-graduates-from-alpha-in-spark-1-3.html and How Apache Spark performs a fast count using the parquet metadata at https://github.com/dennyglee/databricks/blob/master/misc/parquet-count-metadata-explanation.md.


#### DataFrame scenario - on time flight performance
Analizamos Airline On-Time Performance and Causes of Flight Delays: On-Time Data
bit.ly/2ccJPPM), and join this with the airports dataset, obtained from the Open Flights Airport, airline, and route data (http://bit.ly/2ccK5hw), to better understand the variables associated with flight delays.


```python
# Set File Paths
flightPerfFilePath = "./learningPySpark/Data/departuredelays.csv"
airportsFilePath = "./learningPySpark/Data/airport-codes-na.txt"

# Obtain Airports dataset
airports = spark.read.csv(airportsFilePath, header='true', inferSchema='true', sep='\t')
airports.createOrReplaceTempView("airports")

# Obtain Departure Delays dataset
flightPerf = spark.read.csv(flightPerfFilePath, header='true')
flightPerf.createOrReplaceTempView("FlightPerformance")

# Cache the Departure Delays dataset ? esto que quiere decir ?
flightPerf.cache()
```




    DataFrame[date: string, delay: string, distance: string, origin: string, destination: string]



##### Joining flight performance and airports
Una de las tareas más comunes es hacer joins entre dataframes. 


```python
# Query Sum of Flight Delays by City and Origin Code (for Washington State)
spark.sql("select a.City, f.origin, sum(f.delay) as Delays from FlightPerformance f join airports a on a.IATA = f.origin where a.State = 'WA' group by a.City, f.origin order by sum(f.delay) desc").show()

```

    +-------+------+--------+
    |   City|origin|  Delays|
    +-------+------+--------+
    |Seattle|   SEA|159086.0|
    |Spokane|   GEG| 12404.0|
    |  Pasco|   PSC|   949.0|
    +-------+------+--------+
    


#### + Ejemplos


```python
# Select everybody, but increment the age by 1
swimmers.select(swimmers['name'], swimmers['age'] + 1).show()

```

    +-------+---------+
    |   name|(age + 1)|
    +-------+---------+
    |  Katie|       20|
    |Michael|       23|
    | Simone|       24|
    +-------+---------+
    



```python
# Select people older than 21
swimmers.filter(swimmers['age'] > 21).show()
```

    +---+-------+---+--------+
    | id|   name|age|eyeColor|
    +---+-------+---+--------+
    |234|Michael| 22|   green|
    |345| Simone| 23|    blue|
    +---+-------+---+--------+
    



```python
# Count people by age
swimmers.groupBy("age").count().show()

```

    +---+-----+
    |age|count|
    +---+-----+
    | 19|    1|
    | 22|    1|
    | 23|    1|
    +---+-----+
    



```python
# eliminar duplicados
df = spark.createDataFrame([(1, 144.5, 5.9, 33, 'M'),
        (2, 167.2, 5.4, 45, 'M'),
        (3, 124.1, 5.2, 23, 'F'),
        (4, 144.5, 5.9, 33, 'M'),
        (5, 133.2, 5.7, 54, 'F'),
        (3, 124.1, 5.2, 23, 'F'),
        (5, 129.2, 5.3, 42, 'M'),], ['id', 'weight', 'height', 'age', 'gender'])

print('Count of rows: {0}'.format(df.count()))
print('Count of distinct rows: {0}'.format(df.distinct().count()))



```

    Count of rows: 7
    Count of distinct rows: 6



```python
df = df.dropDuplicates()
print('Count of rows: {0}'.format(df.count()))
print('Count of distinct rows: {0}'.format(df.distinct().count()))
```

    Count of rows: 6
    Count of distinct rows: 6



```python
# por Ids
print('Count of ids: {0}'.format(df.count()))
print('Count of distinct ids: {0}'.format(
    df.select([
        c for c in df.columns if c != 'id'
    ]).distinct().count())
)
```

    Count of ids: 6
    Count of distinct ids: 5



```python
df = df.dropDuplicates(subset=[c for c in df.columns if c != 'id'])

df.show()
```

    +---+------+------+---+------+
    | id|weight|height|age|gender|
    +---+------+------+---+------+
    |  5| 133.2|   5.7| 54|     F|
    |  1| 144.5|   5.9| 33|     M|
    |  2| 167.2|   5.4| 45|     M|
    |  3| 124.1|   5.2| 23|     F|
    |  5| 129.2|   5.3| 42|     M|
    +---+------+------+---+------+
    



```python
# agregar funciones
import pyspark.sql.functions as fn
    
df.agg(fn.count('id').alias('count'),
    fn.countDistinct('id').alias('distinct')
).show()


```

    +-----+--------+
    |count|distinct|
    +-----+--------+
    |    5|       4|
    +-----+--------+
    



```python
df.withColumn('new_id', fn.monotonically_increasing_id()).show()
```

    +---+------+------+---+------+-------------+
    | id|weight|height|age|gender|       new_id|
    +---+------+------+---+------+-------------+
    |  5| 133.2|   5.7| 54|     F|  25769803776|
    |  1| 144.5|   5.9| 33|     M| 171798691840|
    |  2| 167.2|   5.4| 45|     M| 592705486848|
    |  3| 124.1|   5.2| 23|     F|1236950581248|
    |  5| 129.2|   5.3| 42|     M|1365799600128|
    +---+------+------+---+------+-------------+
    



