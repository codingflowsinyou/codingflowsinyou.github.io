---
layout: post
title: Entendiendo Spark - parte 1
image: https://raw.githubusercontent.com/codingflowsinyou/codingflowsinyou.github.io/master/assets/img/posts/2021-07-31-My-First-Post/spark.jpeg
published: true
---

## Introducción

Apache Spark es un motor open-source originalmente desarrollado por **Matei Zaharia** como parte de su tesis en UC Berkeley. La primera versión de Spark fue lanzada en 2012. Desde entonces, en 2013, Zaharia se convirtió en el CTO de Databricks; también fue profesor en Stanford. Al mismo tiempo fue donado a *Apache Software Foundation (^)* y se ha convertido en su proyecto emblemático, de ahí nació *Apache Spark*.

Apache Spark es rápido, fácil de usar y te permite resolver una amplia variedad de problemas de datos semiestructurados, estructurados, en streaming y/o machine learning o data science. También es una de las comunidades open-source de big data más grandes del mundo con más de 1.000 contribuidores y más de 250 organizaciones.

(^) Apache Software Foundation es una organización sin fines de lucro creada para dar soporte a los proyectos de software bajo la denominación Apache. Es una comunidad descentralizada de desarrolladores que trabajan cada uno en sus propios proyectos de código abierto. Los proyectos Apache se caracterizan por un modelo de desarrollo basado en el consenso y la colaboración y en una licencia de software abierta y pragmática.


## ¿Qué es Apache Spark?
Apache Spark consta de las siguientes características:
* Es un motor *open-source* y de procesamiento de datos distribuido.
* Proporciona flexibilidad y extensibilidad de MapReduce(*) pero con un significante aumento de la velocidad: 100 veces más rápido que Apache Hadoop(*) cuando los datos son almacenados en memoria y 10 veces más rápidos están en el disco
* Permite al usuario leer, transformar y agregar datos para entrenarlos y desarrollar sofisticados modelos estadísticos con facilidad. 
* Las APIs de Spark son accesibles en Java, Scala, Python, R y SQL.
* Puede ser utilizado para construir aplicaciones para ser desarrolladas en un clúster o en notebooks (jupyter notebooks, databricks notebooks...). También expone una gran variedad de librerías para data scientist, data analysts o investigadores quienes trabajan con entornos como Python o R.
* Puede ejecutarse fácilmente en un ordenador, en standalone mode (modo independiente, no necesita una entidad externa para gestionar recursos), sobre YARN, o Apache Mesos, Kubernetes. Puede leer de diferentes data sources incluido HDFS, Apache Cassandra, Apache HBase... 

(*) Apache Hadoop fue otro proyecto de la fundación Apache Software Foundation anterior a Apache Spark. Se trata de un framework de software que permite programar aplicaciones distribuidas, es decir, aplicaciones capaces de trabajar con enormes cantidades de nodos en red y de datos. MapReduce es la columna vertebral de Apache Hadoop. Éste consiste en una técnica de procesamiento de datos que aplica dos tipos de funciones sobre los mismos: función map y función reduce.  
La función map toma los archivos almacenados en el HDFS, y crea varios conjuntos de datos a partir de ellos. Después, la función reduce procesa esos conjuntos de datos y crea a partir de ellos conjuntos todavía más pequeños, los cuales se vuelven a almacenar en el HDFS. La secuencia de ambas funciones es la que facilita el procesamiento paralelo en lo que es Hadoop, ya que permite que el código se pueda ejecutar en múltiples nodos. Para contextualizarlo, Apache Hadoop es el padre de Apache Spark.

(*) HDFS: Hadoop Distributed File System, es el sistema de ficheros distribuido de Hadoop. El calificativo «distribuido» expresa la característica más significativa de este sistema de ficheros, la cual es su capacidad para almacenar los archivos en un clúster de varias máquinas.

<img src="ecosistema_spark.png">

(source: Apache is the smartphone of Big Data https://insidebigdata.com/2015/11/09/apache-spark-is-the-smartphone-of-big-data/) 
Artículo interesante con dos claves básicas: mejoró el coste computacional trabajando en memoria RAM en vez de en disco & se centró en el tratamiento de los datos y no en la tecnología.

## Spark Jobs and APIs

#### Glosario
| Term            | Meaning |  
|-----------------|----------|
| Aplicación     | Programa construido en Spark. Consiste en un driver program y ejecutores en el clúster.|
| Application jar | A jar containing the user's Spark application. In some cases users will want to create an "uber jar" containing their application along with its dependencies. The user's jar should never include Hadoop or Spark libraries, however, these will be added at runtime.|
| Driver program  | Procesos que ejecutan la `main() function` de la aplicación y crean el SparkContext|
| Cluster manager | Un servicio externo para adquirir recursos(memoria & CPU) en el cluster (standalone, Mesos, YARN) |
| Deploy mode     | Distinguishes where the driver process runs. In "cluster" mode, the framework launches the driver inside of the cluster. In "client" mode, the submitter launches the driver outside of the cluster.|
| Worker node     | Cualquier nodo que pueda ejecutar aplicaciones en el cluster |
| Executor        | Un proceso lanzado para una aplicación en un worker node que ejecuta tareas y guarda los datos en memoria o en disco. Cada aplicación tiene sus propios ejecutores.|
| Task            | Una unidad de trabajo que se enviará a un executor |
| Job             | Computación en paralela que consiste en múltiples tareas que se generan en respuesta a una acción de Spark (ejemplo: .collect())|
| Stage           | Cada job se divide en grupos de tareas más pequeñas llamadas stages que dependen unas de otras (similar a map & reduce)|

#### Componentes
Las aplicaciones de Spark se ejecutan en procesos independientes dentro de un cluster, coordinados por el `SparkContext` en el `main program` (también llamado driver program).

Especificamente, para runnear un cluster, SparkContext puede conectarse con varios tipos de clusters managers, los cuales asignan recursos entre aplicaciones. Una vez conectados, Spark adquiere executors en nodos en el clustes, los cuales son procesos que ejecutan cálculos y almacenan datos para tus aplicaciones. Lo siguiente, envía el código de tus aplicaciones (definido en JAR o en Python pasado al SparkContext) a los executors. Por último, SparkContext envía tareas que los executors las ejecuten.
<img src="cluster_spark.png">

Hay varias cosas útiles que remarcar sobre esta arquitectura:
1. Cada aplicación tiene su propio proceso de executors, los cuales permanecen activos durante toda la aplicación y runnean tasks en subprocesos. Esto permite aislar las aplicaciones unas de otras, tanto en el lado del scheduling (cada driver programa sus propias tasks) como en el lado de los executors (tareas de diferentes aplicaciones se ejecutan en diferentes JVMs). Sin embargo, esto significa que los datos no pueden compartirse a través de diferentes aplicaciones de Spark sin escribirlos en un sistema de almacenamiento externo.

2. Spark es independiente del cluster manager. Siempre que pueda adquirirá procesos executor y estos se comunican entre sí. Así es relativamente fácil ejecutarlo incluso en un cluster manager que también admita otras aplicaciones (Mesos/YARN)

3. El programa driver debe escuchar y aceptar conexiones de sus executors durante toda su vida. Como tal, el programa driver debe ser direccionable a la red desde los worker nodes.

4. Debido a que el driver programa tareas en el clúster, debe ejecutarse cerca de los worker nodes, preferiblemente en la misma area de red local.

#### Tipos de Cluster Manager
* Standalone: un simple cluster manager incluido con Spark a través del cual es mjuy fácil levantar un clúster.
* Apache Mesos: un cluster manager general que también puede ejecutar Hadoop MapReduce y servicios de aplicaciones.
* Hadoop YARN -  Administrador de recursos en Hadoop 2.
* Kubernetes - un sistema open-source para automatizar desarrollos y aplicaciones escalables y en contenedores.

#### Monitoreo
Cada driver program tiene una web, tipicamente en el puerto 4040, que muestra la información sobre las tareas, ejecutores y almacenamiento. http://<driver-node>:4040. + info en https://spark.apache.org/docs/latest/monitoring.html


#### Job Scheduling
Spark brinda un control sobre la asignación de recursos en las aplicaciones. + info en https://spark.apache.org/docs/latest/job-scheduling.html

Un Spark Job está asociado con una cadena de dependencias organizada por el **DAG (direct acyclic graph)**. Por ejemplo, Spark puede optimizar la organización (por ejemplo: determinar el número de tasks y workers requeridos) y la ejecución de estas tasks. Visualmente:
<img src="DAG2.png">
(source y + info: https://data-flair.training/blogs/dag-in-apache-spark/)


```python

```
