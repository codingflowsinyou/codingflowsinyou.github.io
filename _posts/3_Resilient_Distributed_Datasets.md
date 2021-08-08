# Resilient Distributed Datasets
RDDs son unas colecciones de objetos JVM inmutables que te permiten hacer calculos muy rápidos. Son la columna vertebral de Apache Spark.

Como el nombre sugiere, los datasets están distribuidos; se cortan en chunks basados en alguna clave y se distribuyen por los executor nodes. Además los RDDs hacen un seguimiento de todas las transformaciones aplicadas a cada chunk para mejorar la velocidad computacional y hacer retrocesos en el caso de que algo vaya mal. Si hay alguna parte de los datos que se ha perdido los RDDs se pueden volver a regenerar sin depender de réplicas.

## Trabajo interno de los RDDs
RDDs operan en paralelo. Esto es la mayor ventaja de trabajar en Spark: toda transformación es ejecutada en paralelo para mejorar la velocidad.
Las transformaciones (que no acciones) son lentas. Esto significa que cualquier transformación es sólo ejecutada si una acción es llamada. Esto ayuda a Spark a optimizar la ejecución. Pongamos un ejemplo:
    
    En un dataframe debemos realizas las siguientes acciones:
            1. Contar las ocurrencias de los distintos valores de una cierta columna.
            2. Seleccionar aquellas que empiezan por A.
            3. Printear los resultados en la pantalla
Si sólo los valores que empiezan por A son de nuestro interes no tiene sentido contar valores que empiezan por otras letras. Esto es, en vez de seguir las instrucciones marcadas por los puntos precedentes, Spark sólo contará aquellos items que empiecen por A y entonces los mostrará en pantalla.

#### Inicializando Spark
En primer lugar, debemos inicializar el objeto `SparkContext`, el cual le indica a Spark cómo entrar al clúster. Para crearlo primero necesitamos construir un `SparkConf` que es el objeto que contiene información sobre tu aplicación.


```python
#### arreglos
import os
os.environ['PYSPARK_PYTHON'] = '/opt/anaconda3/bin/python'
os.environ['PYSPARK_DRIVER_PYTHON']='/opt/anaconda3/bin/python'
```


```python
# Inicializando Spark
from pyspark.conf import SparkConf
from pyspark.context import SparkContext

# configuración
conf = SparkConf()
conf.setMaster("local").setAppName("My app")

print('Node Master:',conf.get("spark.master"))
print('Name App:',conf.get("spark.app.name"))

# SparkContext
sc = SparkContext(conf=conf)
```

    Node Master: local
    Name App: My app


### Crear RDDs
Como ya hemos mencionado anteriormente, los RDDs operan en paralelo. Hay dos maneras de crearlos:

1. `paralellize(...)` una colección (listas o arrays):

Es representado por un objeto tipo ParalellCollectionRDD.


```python
# Ejemplo 1
data1 = [1, 2, 3, 4, 5]
distData = sc.parallelize(data1)
```

Una vez creado, los datos distribuidos (distData) pueden ser operados en paralelo. Por ejemplo, pocemos crear .reduce(lambda a,b: a+b) para agregar elementos a la lista.


```python
data2  = sc.parallelize([('Amber',22),('Alfred',23),('Skye',4),('Albert',12),('Amber',9)])
```

Un parámetro importante para paralelizar coleccions es el número de particiones para cortar el dataset. Spark ejecutará una tarea por partición en el clúster. Normalmente, queremos 2-4 particiones por cada CPU en nuestro clúster. Spark intentará establecer el número de particiones automáticamente basado en tu clúster. Sin embargo, también se puede establecer manualmente pasándole un segundo parámetro a la función `sc.paralellize(...,n)`. 

2. `textFile(...,n)` donde n es el número de particiones del dataset. 

Es representado por un objeto tipo MapPartitionsRDD.
Este comando tiene un opcional argumento para controlar el número de particiones de cada archivo. Por defecto, Spark crea una particion por cada bloque del archivo (128MB por defecto en HDFS).


```python
data_from_file = sc.textFile('VS14MORT.txt',4)
```

### Schema
RDDs son estructuras sin esquema (al contrario que los DataFrame). Por tanto, los elementos de la colección no afecta a la manera de paralelizar. Por ejemplo:


```python
data_heterogeneous = sc.parallelize([('Ferrari','fast'),{'Porsche':1000},['Spain','visited',4504]]).collect()
```

Por lo que podemos mezclar casi todo, una tupla, un diccionario o una lista. 


```python
data_heterogeneous[1]['Porsche']
```




    1000



Este método devuelve todos los elementos de la RDD al driver donde se serializa como una lista.

### Reading from files
Cuando leemos desde un archivo de texto, cada fila del archivo forma un elemento de la RDD.


```python
data_from_file.take(1)
```




    ['                   1                                          2101  M1087 432311  4M4                2014U7CN                                    I64 238 070   24 0111I64                                                                                                                                                                           01 I64                                                                                                  01  11                                 100 601']



### Operaciones con RDDs
Se podrán hacer dos tipos de operaciones con RDDs:
    - transformaciones, las cuales crean un nuevo dataset a partir de uno existente.
    - acciones, el cual devuelve un valor del driver program despues de ejecutar la computación sobre el dataset.

Por ejemplo, `map` es una transformación que pasa a cada elemento del dataset una función y devuelve un nuevo RDD representado los resultados. Por otro lado, `reduce` es una acción que agrega todos los elementos de la RDD utilizando alguna función y devuelve un resultado final al driver program.
Todas las transformaciones en Spark son muy lentas, en el sentido de que no computan ningún resultado inmediatamente. En vez de esto, sólo se ejecutan cuando una acción es requerida para devolver un resultado. Esto permite a Spark ser mucho más eficiente.
Por defecto, toda transformación en la RDD debe ser rehecha cada vez que una acción es ejecutada. Sin embargo, se podría guardar el RDD en memoria utilizando la caché (`persist()`), de manera que Spark guardará lo selementos en el clúster para mucho más rápido acceso la próxima vez que ejecutes una query. También pueden estar en disco o replicados a través de múltiples nodos.


```python
lineLengths = data2.map(lambda s: len(s))
totalLength = lineLengths.reduce(lambda a, b: a + b)
# si quisieramos utilizar lineLengths luego lo guardamos en caché.
lineLengths.persist()
```




    PythonRDD[7] at RDD at PythonRDD.scala:53




```python
def extractInformation(row):
    # función para darle sentido a cada elemento de la lista anterior
    import re
    import numpy as np

    selected_indices = [
         2,4,5,6,7,9,10,11,12,13,14,15,16,17,18,
         19,21,22,23,24,25,27,28,29,30,32,33,34,
         36,37,38,39,40,41,42,43,44,45,46,47,48,
         49,50,51,52,53,54,55,56,58,60,61,62,63,
         64,65,66,67,68,69,70,71,72,73,74,75,76,
         77,78,79,81,82,83,84,85,87,89
    ]

    '''
        Input record schema
        schema: n-m (o) -- xxx
            n - position from
            m - position to
            o - number of characters
            xxx - description
        1. 1-19 (19) -- reserved positions
        2. 20 (1) -- resident status
        3. 21-60 (40) -- reserved positions
        4. 61-62 (2) -- education code (1989 revision)
        5. 63 (1) -- education code (2003 revision)
        6. 64 (1) -- education reporting flag
        7. 65-66 (2) -- month of death
        8. 67-68 (2) -- reserved positions
        9. 69 (1) -- sex
        10. 70 (1) -- age: 1-years, 2-months, 4-days, 5-hours, 6-minutes, 9-not stated
        11. 71-73 (3) -- number of units (years, months etc)
        12. 74 (1) -- age substitution flag (if the age reported in positions 70-74 is calculated using dates of birth and death)
        13. 75-76 (2) -- age recoded into 52 categories
        14. 77-78 (2) -- age recoded into 27 categories
        15. 79-80 (2) -- age recoded into 12 categories
        16. 81-82 (2) -- infant age recoded into 22 categories
        17. 83 (1) -- place of death
        18. 84 (1) -- marital status
        19. 85 (1) -- day of the week of death
        20. 86-101 (16) -- reserved positions
        21. 102-105 (4) -- current year
        22. 106 (1) -- injury at work
        23. 107 (1) -- manner of death
        24. 108 (1) -- manner of disposition
        25. 109 (1) -- autopsy
        26. 110-143 (34) -- reserved positions
        27. 144 (1) -- activity code
        28. 145 (1) -- place of injury
        29. 146-149 (4) -- ICD code
        30. 150-152 (3) -- 358 cause recode
        31. 153 (1) -- reserved position
        32. 154-156 (3) -- 113 cause recode
        33. 157-159 (3) -- 130 infant cause recode
        34. 160-161 (2) -- 39 cause recode
        35. 162 (1) -- reserved position
        36. 163-164 (2) -- number of entity-axis conditions
        37-56. 165-304 (140) -- list of up to 20 conditions
        57. 305-340 (36) -- reserved positions
        58. 341-342 (2) -- number of record axis conditions
        59. 343 (1) -- reserved position
        60-79. 344-443 (100) -- record axis conditions
        80. 444 (1) -- reserve position
        81. 445-446 (2) -- race
        82. 447 (1) -- bridged race flag
        83. 448 (1) -- race imputation flag
        84. 449 (1) -- race recode (3 categories)
        85. 450 (1) -- race recode (5 categories)
        86. 461-483 (33) -- reserved positions
        87. 484-486 (3) -- Hispanic origin
        88. 487 (1) -- reserved
        89. 488 (1) -- Hispanic origin/race recode
     '''

    record_split = re\
        .compile(
            r'([\s]{19})([0-9]{1})([\s]{40})([0-9\s]{2})([0-9\s]{1})([0-9]{1})([0-9]{2})' + 
            r'([\s]{2})([FM]{1})([0-9]{1})([0-9]{3})([0-9\s]{1})([0-9]{2})([0-9]{2})' + 
            r'([0-9]{2})([0-9\s]{2})([0-9]{1})([SMWDU]{1})([0-9]{1})([\s]{16})([0-9]{4})' +
            r'([YNU]{1})([0-9\s]{1})([BCOU]{1})([YNU]{1})([\s]{34})([0-9\s]{1})([0-9\s]{1})' +
            r'([A-Z0-9\s]{4})([0-9]{3})([\s]{1})([0-9\s]{3})([0-9\s]{3})([0-9\s]{2})([\s]{1})' + 
            r'([0-9\s]{2})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' + 
            r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' + 
            r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' + 
            r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' + 
            r'([A-Z0-9\s]{7})([\s]{36})([A-Z0-9\s]{2})([\s]{1})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' + 
            r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' + 
            r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' + 
            r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' + 
            r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([\s]{1})([0-9\s]{2})([0-9\s]{1})' + 
            r'([0-9\s]{1})([0-9\s]{1})([0-9\s]{1})([\s]{33})([0-9\s]{3})([0-9\s]{1})([0-9\s]{1})')
    try:
        rs = np.array(record_split.split(row))[selected_indices]
    except:
        rs = np.array(['-99'] * len(selected_indices))
    return rs
    
```

Nota: definir métodos puros en Python puede ralentizar nuestra aplicación. Siempre que se pueda hay que construir los métodos en funciones de Spark.

Extraemos la información:


```python
# .map
# el método aplicará a cada elemento de la RDD la función al mismo tiempo
# en cada partición.
data_from_file_conv = data_from_file.map(extractInformation)
```


```python
data_from_file_conv.take(1)
```




    [array(['1', '  ', '2', '1', '01', 'M', '1', '087', ' ', '43', '23', '11',
            '  ', '4', 'M', '4', '2014', 'U', '7', 'C', 'N', ' ', ' ', 'I64 ',
            '238', '070', '   ', '24', '01', '11I64  ', '       ', '       ',
            '       ', '       ', '       ', '       ', '       ', '       ',
            '       ', '       ', '       ', '       ', '       ', '       ',
            '       ', '       ', '       ', '       ', '       ', '01',
            'I64  ', '     ', '     ', '     ', '     ', '     ', '     ',
            '     ', '     ', '     ', '     ', '     ', '     ', '     ',
            '     ', '     ', '     ', '     ', '     ', '     ', '01', ' ',
            ' ', '1', '1', '100', '6'], dtype='<U40')]



### Transformations

Todas las transformaciones en la documentación:
https://spark.apache.org/docs/latest/rdd-programming-guide.html#accumulators

#### `.map(...)`
Este método aplica a cada elemento de la RDD. Por ejemplo, crearemos un nuevo dataset que convierta el año en numérico.


```python
data_2014 = data_from_file_conv.map(lambda row: int(row[16]))
```


```python
data_2014.take(10)
```




    [2014, 2014, 2014, 2014, 2014, 2014, 2014, 2014, 2014, -99]




```python
data_2014_2 = data_from_file_conv.map(
    lambda row: (row[16], int(row[16])))
```


```python
data_2014_2.take(5)

```




    [('2014', 2014),
     ('2014', 2014),
     ('2014', 2014),
     ('2014', 2014),
     ('2014', 2014)]



#### `.filter(...)`
Nos permite seleccionar elementos en función de un determinado criterio.

Veamos cuánta gente ha muerto en 2014.


```python
data_filtered = data_from_file_conv.filter(
    lambda row: row[16] == '2014' and row[21] == '0')
data_filtered.count()
 
```




    22



#### `.flatMap(...)`
Similar a map, pero el output devuelve una secuencia en lugar de un single item.


```python
data_2014_flat = data_from_file_conv.flatMap(lambda row: (row[16], int(row[16]) + 1))
data_2014_flat.take(10)

```




    ['2014', 2015, '2014', 2015, '2014', 2015, '2014', 2015, '2014', 2015]



#### `.distinct(...)`
Este método devuelve una lista de distintos únicos valores para una columna específica. Veamos qué generos hay en la lista:


```python
distinct_gender = data_from_file_conv.map(
    lambda row: row[5]).distinct()
distinct_gender.collect()
# método muy caro computacionalmente--> usar poco
```


    ---------------------------------------------------------------------------

    KeyboardInterrupt                         Traceback (most recent call last)

    <ipython-input-18-96870c130881> in <module>
          1 distinct_gender = data_from_file_conv.map(
          2     lambda row: row[5]).distinct()
    ----> 3 distinct_gender.collect()
          4 # método muy caro computacionalmente--> usar poco


    /opt/anaconda3/lib/python3.7/site-packages/pyspark/rdd.py in collect(self)
        947         """
        948         with SCCallSiteSync(self.context) as css:
    --> 949             sock_info = self.ctx._jvm.PythonRDD.collectAndServe(self._jrdd.rdd())
        950         return list(_load_from_socket(sock_info, self._jrdd_deserializer))
        951 


    /opt/anaconda3/lib/python3.7/site-packages/py4j/java_gateway.py in __call__(self, *args)
       1301             proto.END_COMMAND_PART
       1302 
    -> 1303         answer = self.gateway_client.send_command(command)
       1304         return_value = get_return_value(
       1305             answer, self.gateway_client, self.target_id, self.name)


    /opt/anaconda3/lib/python3.7/site-packages/py4j/java_gateway.py in send_command(self, command, retry, binary)
       1031         connection = self._get_connection()
       1032         try:
    -> 1033             response = connection.send_command(command)
       1034             if binary:
       1035                 return response, self._create_connection_guard(connection)


    /opt/anaconda3/lib/python3.7/site-packages/py4j/java_gateway.py in send_command(self, command)
       1198 
       1199         try:
    -> 1200             answer = smart_decode(self.stream.readline()[:-1])
       1201             logger.debug("Answer received: {0}".format(answer))
       1202             if answer.startswith(proto.RETURN_MESSAGE):


    /opt/anaconda3/lib/python3.7/socket.py in readinto(self, b)
        587         while True:
        588             try:
    --> 589                 return self._sock.recv_into(b)
        590             except timeout:
        591                 self._timeout_occurred = True


    KeyboardInterrupt: 


#### `.sample(...)`
Devuelve un sampleo random del dataset. El primer parámetro especifica si el sampleo debe ir con reemplazamiento o no y el segundo define la fracción de datos a devolver, el tercero es la semilla.


```python
fraction = 0.1
data_sample = data_from_file_conv.sample(False, fraction, 666)

```


```python
print('Original dataset: {0}, sample: {1}'\
.format(data_from_file_conv.count(), data_sample.count()))
 
```

#### `.leftOuterJoin(...)`
Como en SQL.


```python
rdd1 = sc.parallelize([('a', 1), ('b', 4), ('c',10)])
rdd2 = sc.parallelize([('a', 4), ('a', 1), ('b', '6'), ('d', 15)])
rdd3 = rdd1.leftOuterJoin(rdd2) 
```


```python
rdd3.collect()
```




    [('b', (4, '6')), ('c', (10, None)), ('a', (1, 4)), ('a', (1, 1))]



#### `.repartition(...)`
Cambia el número de particiones en los que el dataset está dividido. Muy costoso, evitar usar. 


```python
rdd1 = rdd1.repartition(4)
len(rdd1.glom().collect())
```




    4



### Actions
Las acciones, al contrario que las transformaciones, ejecutan el scheduled task on the dataset, que pueden incluir transformaciones o no. 

#### `.take(n)`
Este método es preferente a .collect(), y sólo devuelve las n primeras rows de cada partición. Al contrario que .collect() que devuelve todo el RDD. Esto es especialmente importante si tu trabajas con largos datasets.


```python
data_first = data_from_file_conv.take(1) 
```


```python
data_take_sampled = data_from_file_conv.takeSample(False, 1, 667)

```

#### `.collect(...)`
Este método devuelve los elementos del RDD to the driver. 

#### `.reduce(...) method `
The .reduce(...) method reduces the elements of an RDD using a specified method. You can use it to sum the elements of your RDD:

We first create a list of all the values of the rdd1 using the .map(...) transformation, and then use the .reduce(...) method to process the results. The reduce(...) method, on each partition, runs the summation method (here expressed as a lambda) and returns the sum to the driver node where the final aggregation takes place.


```python
rdd1.map(lambda row: row[1]).reduce(lambda x, y: x + y) 
```




    15




```python
rdd1.take(10)
```




    [('a', 1), ('b', 4), ('c', 10)]



A word of caution is necessary here. The functions passed as a reducer need to be associative, that is, when the order of elements is changed the result does not, and commutative, that is, changing the order of operands does not change the result either. The example of the associativity rule is (5 + 2) + 3 = 5 + (2 + 3), and of the commutative is 5 + 2 + 3 = 3 + 2 + 5. Thus, you need to be careful about what functions you pass to the reducer. If you ignore the preceding rule, you might run into trouble (assuming your code runs at all). For example, let's assume we have the following RDD (with one partition only!):


```python
data_reduce = sc.parallelize([1, 2, .5, .1, 5, .2], 1) 
```


```python

```
