# Création d'une plateforme IoT Cloud et Big Data pour le diagnostique de la maladie de parkinson via des données de capteurs

Dans ce tutoriel nous allons voir comment réaliser un pipeline permettant de récupérer des données de capteur et les exploiter de manière optimal afin d'obtenir une plateforme scalable permettant de se mettre à jour aisément dans le cadre du diagnostique de parkison.

Le pipeline réalisé dans ce tutoriel sera comme illustré dans la figure ci dessous :

![pipeline](readme-images\pipeline.png)

L'intégralité des outils illustré dans le pipeline seront intégrés en localhost via le langage de programmation Python et avec le système d'exploitation Ubuntu, nous passerons en revue l'integralité des installations nécessaire ainsi que la programation et paramètrage à réaliser pour faire fonctionner la pipeline.



## Intallation des différents outils :

### Python :

Etant donné que nous utilisons Ubuntu comme système d'exploitation Python est dèja installé cependant si vous ne l'avez pas il suffit simplement d'exécuter la commande suivante dans votre terminal :

```bash
sudo apt install python3
```

### MQTT :



### Kafka :



### Cassandra :



### MongoDB :



### ElasticSearch :



### Kibana :



### Hadoop :



### Spark :



### Flask et Scikit Learn :



## Réalisation de la plateforme :

### 1) Récupération des données des capteurs avec MQTT :



### 2) Réception des données et stockage dans un Data Lake avec Hadoop HDFS :



### 3) Envoie des données du Data Lake via kafka :



### 4) Réception des données et prétraitement avec Spark :



### 5) Stockage des données prétraitées dans Cassandra :



### 6) Création du API REST de machine learning avec Flask et Sickit-learn :



### 7) Envoie des données de Cassandra a l'API :



### 8) Stockage des données retournées par l'API dans MongoDB :
