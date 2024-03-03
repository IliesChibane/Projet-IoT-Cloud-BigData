# Création d'une plateforme IoT Cloud et Big Data pour le diagnostique de la maladie de parkinson via des données de capteurs

## Introduction 

Dans ce tutoriel nous allons voir comment réaliser un pipeline permettant de récupérer des données de capteur et les exploiter de manière optimal afin d'obtenir une plateforme scalable permettant de se mettre à jour aisément dans le cadre du diagnostique de parkison.

Le pipeline réalisé dans ce tutoriel sera comme illustré dans la figure ci dessous :

![pipeline](readme-images/pipeline.png?raw=true)

Ce data pipeline collecte des données de capteurs, d'API et d'appareils IoT via MQTT. Les données sont ensuite stockées dans un data lake Hadoop envoyé ensuite via kafka vers Spark qui traite les données et les stocke dans Cassandra. Ces données sont par la suite communiqué à une API REST créer avec flask conteant un modèle de machine learning disponible dans la bibliothéque Sickit-Learn retournant les résultats du modèle qui sont par la suite stocké dans MongoDB et ElasticSearch puis envoyé vers Kibana permetant de visualiser les différentes métriques obtenus.

Ce pipeline est un exemple de la façon dont les données peuvent être collectées, stockées, traitées et analysées pour générer des informations utiles. Il peut être utilisé dans une variété d'applications, telles que la surveillance des performances, la détection de fraude, l'analyse des sentiments et la recommandation de produits.

L'intégralité des outils illustré dans le pipeline seront intégrés en localhost via le langage de programmation Python et avec le système d'exploitation Ubuntu, nous passerons en revue l'integralité des installations nécessaire ainsi que la programation et paramètrage à réaliser pour faire fonctionner la pipeline.

Dans un but d'accéssebilité et de simplicité nous simulerons les capteurs en utilisant le dataset  [Gait in Parkinson's Disease](https://physionet.org/content/gaitpdb/1.0.0/) un dataset contenant différent fichier txt représentant les données retourner par les capteurs sur différents patients.

## Intallation des différents outils :

### Python :

Etant donné que nous utilisons Ubuntu comme système d'exploitation Python est dèja installé cependant si vous ne l'avez pas il suffit simplement d'exécuter la commande suivante dans votre terminal :

```bash
sudo apt install python3
```

### MQTT :

####Étape 1 : Ajout du dépôt Mosquitto
Ajoutez le PPA Mosquitto à vos sources de logiciels pour obtenir la dernière version de Mosquitto.
```bash
sudo apt-add-repository ppa:mosquitto-dev/mosquitto-ppa
```

![MQTT 1](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/457d0a91-6f6f-4967-8f28-d13e092d79f1)

Mettez à jour la liste des paquets avec la commande suivante :
```bash
sudo apt-get update
```
![MQTT 2](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/ea9d4760-1f67-4c57-912c-79497c252a81)

![MQTT 3](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/2e7c3712-4180-47ec-bdcd-3a9a188b002d)

![MQTT 4](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/72b6650f-8f44-49bb-8c6f-97ed4f03c469)

![MQTT 5](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/13615498-a204-4d00-8160-56182aa7f7b6)

### Kafka :
![KAFKA 1](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/ef6716a3-9b5d-4cd9-8f3a-dea68c8802bf)

![KAFKA 2](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/b81eca2b-523a-44f5-8339-e5fd662ab4b6)

### Cassandra :
![Cassandra 1](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/ffa62dbf-f3fe-4a3f-804c-d4d5a0579cb9)

![Cassandra 2](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/627d5b78-8940-44b9-9250-d50ac885b979)

![Cassandra 3](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/fce42517-396c-488a-8742-7a2e1c0ad71b)

![Cassandra 4](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/f7449822-b792-4cb4-92c6-52a7e7d291ab)

![Cassandra 5](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/75166060-3569-48ce-92bd-2dc3aefade07)

![Cassandra 6](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/c09da049-2189-49df-ac09-20e9da200bed)

![Cassandra 7](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/16d9658f-99ab-4f83-89e9-0d3295322947)

![Cassandra 8](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/3dfb8ebe-5eeb-4f79-97ab-4df7118a89c8)


### MongoDB :
![MongoDB 1](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/2fdbeffd-8481-4458-b5cb-f087931f19fc)
![MongoDB 2](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/8da1aa37-c6f3-497a-9932-b24f7d183c25)
![MongoDB 3](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/e69f0003-2fb5-49b1-a281-e38eac7d6e61)
![MongoDB 4](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/f271b708-dfd1-4c77-989a-9f2d72d1494c)
![MongoDB 5](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/2bf396ae-2937-4113-899e-aab793a0cae2)
![MongoDB 6](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/48b6e78d-a43c-4281-ba16-585b577aed3f)

### ElasticSearch :

### Kibana :

### Hadoop :
![Hadoop 1](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/e6a934d4-1104-4892-80f4-f029e436d2c6)
![Hadoop 2](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/177ba3c4-be48-4c95-b33c-7f92fc052f52)
![Hadoop 3](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/012432aa-ebe0-48cc-aad8-243f040818fe)
![Hadoop 4](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/d6f352a4-3be4-4628-bc2d-6f58f02222f2)
![Hadoop 5](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/39436973-1987-4dba-b52b-0017f1edabe8)
![Hadoop 6](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/2be5a03c-0ecd-4f52-bd83-e07619d8b61a)
![Hadoop 7](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/8decb0b0-d00a-4331-a486-f1dcfeec897e)
![Hadoop 8](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/1a198cd3-a65c-4eb2-bb88-c6db87516932)
![Hadoop 9](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/d95bffe8-1467-413b-a0b0-900a3287a8b9)
![Hadoop 10](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/8f8ef206-31f9-47a9-94fc-ca1cca1b23bb)
![Hadoop 11](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/5f83f846-360a-4dde-bf8f-b6e1d09f5cbe)
![Hadoop 12](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/45b8a037-83ea-4b09-8479-75b3048e75af)
![Hadoop 13](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/8c6063b0-82e7-4025-879e-140675ca1697)
![Hadoop 14](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/43ffa409-495f-420a-acbf-7c2966fce12b)
![Hadoop 15](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/a346d562-2893-422f-9903-4f76a228cd0d)
![Hadoop 16](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/92c1a37f-13cb-4f3b-86f7-5099727a8eef)
![Hadoop 17](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/3cdce8fb-d346-4f51-a397-3793c025ef1b)
![Hadoop 18](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/assets/144664907/813e31e8-1984-4835-a62d-51534d4d0ead)

### Spark :

### Flask et Scikit Learn :

## Réalisation de la plateforme :

### 1) Récupération des données des capteurs avec MQTT :

On commence par créer le fichier python [sensors_data_receiver.py](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/blob/main/sensors_data_sender.py)  et on y importe les libraires nécessaire :

```python
import paho.mqtt.client as mqttClient
import time
import os
import pandas as pd
```

Par la suite on crée les fonctions permettant de nous connecter et d'envoyer des messages avec notre broket MQTT comme suit :

```python
def on_connect(client, userdata, flags, rc):
    """
    Fonction de rappel pour gérer la connexion au courtier MQTT.
    """
    if rc == 0:
        print("Connecté au courtier MQTT")
        global Connected
        Connected = True
    else:
        print("Échec de la connexion")

def on_publish(client, userdata, result):
    """
    Fonction de rappel pour gérer la publication réussie des données.
    """
    print("Données publiées \n")
    pass
```

Maintenant qu'on a notre fonction nous permettant de nous connecter à notre broker MQTT et de publier des messages nous pouvons passer a la création de notre client MQTT :

```python
Connected = False
broker_address = "localhost"
port = 1883

client = mqttClient.Client("Python_publisher")  # Créer une nouvelle instance
client.on_connect = on_connect  # Attacher la fonction à l'événement de connexion
client.on_publish = on_publish  # Attacher la fonction à l'événement de publication
client.connect(broker_address, port=port)  # Se connecter au courtier
client.loop_start()  # Démarrer la boucle

while not Connected:  # Attendre la connexion
    time.sleep(0.1)

print("Maintenant connecté au courtier: ", broker_address, "\n")
```

Pour résumer jusqu'ici nous avons créé un client MQTT qui se connecte à un courtier local via le protocole MQTT. Il initialise des variables, définit des fonctions de rappel pour la gestion des événements de connexion et de publication, puis tente de se connecter au courtier. La boucle principale attend que la connexion soit établie, utilisant une pause de 0.1 seconde pour éviter une consommation excessive de ressources. Une fois la connexion réussie, le programme affiche un message confirmant la connexion au courtier.



La prochaine étape consiste à envoyer les données, pour cela nous récupérons les données de notre dataset fichier par fichier et ligne par la ligne que nous envoyons sous format JSON qui est le format supporté par les messages MQTT.

```python
try:
    while True:
        folder_path = 'gait-in-parkinsons-disease-1.0.0'
        txt_files = [file for file in os.listdir(folder_path) if file.endswith('.txt')][2:]

        # Obtenir l'index du fichier 'SHA256SUMS.txt'
        txt_files.index('SHA256SUMS.txt')

        # Retirer l'index de la liste
        txt_files.pop(txt_files.index('SHA256SUMS.txt'))

        for file in txt_files:
            df_p = pd.read_csv(folder_path + "/" + file, sep='\t')

            # Renommer les colonnes pour un accès plus facile
            df_p.columns = ['time', 'L1', 'L2', 'L3', 'L4', 'L5', 'L6', 'L7', 'L8', 'R1', 'R2', 'R3', 'R4', 'R5', 'R6', 'R7', 'R8', 'L', 'R']

            for row in df_p.to_numpy():
                message = {"Patient": file.split('.')[0]}
                for c, r in zip(df_p.columns, row):
                    message[c] = r
                topic = "test/parkinson"
                client.publish(topic, payload=str(message))
                time.sleep(1)
                print("Message publié avec succès vers test/parkinson")
except KeyboardInterrupt:
    print("Sortie de la boucle")
    client.disconnect()
    client.loop_stop()
```

Comme dit précédement nous effectuons une lecture continue de fichiers texte contenant des données liées à la maladie de Parkinson. En utilisant la bibliothèque Pandas, il renomme les colonnes pour faciliter l'accès, extrait le nom du patient du nom du fichier, puis publie ces données sous forme de messages structurés sous format json sur le topic MQTT "test/parkinson". La boucle infinie assure une exécution continue, avec une pause d'une seconde entre chaque publication. Le tout pouvant etre interrompue à n'importe quel moment via le clavier.

### 2) Réception des données et stockage dans un Data Lake avec Hadoop HDFS :

La réception se faisant dans un programme a part nous créeons le fichier [sensors_data_receiver.py](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/blob/main/sensors_data_receiver.py) ou nous y important toutes les librairies dont nous avons besoin :

```python
import paho.mqtt.client as mqtt
import time
import json
from hdfs import InsecureClient
```

Une fois fait comme lors de l'envoie nous créeons la fonction de connexion et la fonction de réception a la place de la fonction d'envoie :

```python
def on_connect(client, userdata, flags, rc):
    """
    Fonction de rappel pour gérer la connexion au courtier MQTT.
    """
    if rc == 0:
         print("Connecté au broker")
         global Connected                
         Connected = True               
    else:
         print("Échec de la connexion")

def on_message(client, userdata, message):
    """
    Fonction de rappel pour gérer les messages MQTT.
    """
    json_object = json.loads(str(message.payload.decode("utf-8")).replace("'", '"'))
    # Stocker les valeurs de données du message dans une chaîne
    data_values = ""
    for key, value in json_object.items():
        if key != "Patient":
            data_values += str(value) + ";"
    data_values = data_values[:-1]
    # Enregistrer la chaîne dans un fichier CSV avec la clé comme nom de colonne du fichier
    local_file = json_object["Patient"]+'.csv'
    with open("csv data/"+local_file, 'a') as file:
        file.write(data_values + "\n")

    hdfs_file_path = f"{data_lake_path}/{local_file}"
    client.upload(hdfs_file_path, "csv data/"+local_file)
```

Ici la fonction `on_message` est une fonction de rappel pour la gestion des messages MQTT. Elle convertit le payload JSON du message, extrait les valeurs de données à l'exception de la clé "Patient", les enregistre localement dans un fichier CSV portant le nom du patient, et effectue un téléchargement vers un data lake en utilisant le système de stockage distribué Hadoop Distributed File System (HDFS)  sur lequel nous reviendrons par la suite.

Maintenant nous pouvons créer notre client MQTT chargé de recevoir les messages mais aussi d'initialisé notre data lake avec hadoop qui se doit d'etre créer avant le lancement de notre client.

```python
print("Création d'une nouvelle instance")
client = mqtt.Client("python_test")
client.on_message = on_message          # Attacher la fonction au rappel
client.on_connect = on_connect
print("Connexion au broker")

# Création du data lake
hdfs_url = "http://localhost:9870"
hdfs_client = InsecureClient(hdfs_url)
data_lake_path = "data_lake/parkinson_data"
hdfs_client.makedirs(data_lake_path)

client.connect(broker_address, port)  # Connexion au broker
client.loop_start()                   # Démarrer la boucle


while not Connected:                  # Attendre la connexion
    time.sleep(0.1)
 
print("Abonnement au sujet", "test/parkinson")
client.subscribe("test/parkinson")
 
try:
    while True: 
        time.sleep(1)

except KeyboardInterrupt:
    print("Sortie")
    client.disconnect()
    client.loop_stop()
```

Ce segment de code réalise tout ce qu'on avait décrit précédemment en établissant une connexion avec un courtier MQTT en utilisant la bibliothèque `paho.mqtt.client`. Une instance du client est créée, des fonctions de rappel sont attachées pour la gestion des événements de connexion et de réception de messages, et une connexion est établie avec le courtier spécifié. Parallèlement, un système de stockage distribué, représenté par Hadoop Distributed File System (HDFS), est préparé avec un répertoire dédié ("parkinson_data") pour le stockage des fichiers. Le programme s'abonne au sujet MQTT "test/parkinson" pour recevoir les messages correspondants. En entrant dans une boucle infinie, le code réagit aux messages reçus en utilisant la fonction de rappel `on_message`, enregistrant les données localement dans des fichiers CSV nommés par le patient, puis transférant ces données vers le Data Lake. L'exécution de la boucle peut être interrompue par un signal de clavier, déclenchant la déconnexion du client MQTT du courtier.

**IMPORTANT :** Les deux programmes [sensors_data_receiver.py](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/blob/main/sensors_data_sender.py) et [sensors_data_receiver.py](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/blob/main/sensors_data_receiver.py) doivent etre exécuté simultanément pour le bon fonctionnement de la plateforme.

### 3) Envoie des données du Data Lake via kafka :

Pour cette étape nous créons un nouveau fichier python se nommant [hdfs_read.py](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/blob/main/hdfs_read.py) et on y importe les librairies nécessaires :

```python
from hdfs import InsecureClient
from confluent_kafka import Producer
import json
import pandas as pd
```

Dans ce programme 2 fonctions sont nécessaire la première assez simple consiste simplement à indiquer si kafka a été en mesure de déliver le message.

```python
def delivery_report(err, msg):
    """
    Fonction de rappel pour gérer les retours de livraison des messages Kafka.
    """
    if err is not None:
        print('Échec de livraison du message : {}'.format(err))
    else:
        print('Message livré pour le prétraitement')
```

la seconde fonction elle est celle de l'envoie du message via kafka :

```python
def produce_sensor_data(producer, topic, file_name, file_content):
    """
    Fonction pour produire des données de capteur vers Kafka.
    """
    group = file_name[2:4]

    message = {
        "file_name": file_name,
        "content": file_content,
        "group": group
    }
   
    producer.produce(topic, key=file_name, value=json.dumps(message), callback=delivery_report)
    producer.poll(0)
    producer.flush()
```

la focntion prend en paramètres un producteur Kafka (`producer`), un sujet Kafka (`topic`), le nom d'un fichier (`file_name`), et le contenu de ce fichier (`file_content`). La fonction extrait la classe du patient (malade ou saint) à partir des deux premiers caractères du nom de fichier. Elle crée ensuite un message structuré avec le nom du fichier, son contenu, et sa classe. Ce message est ensuite produit sur le sujet Kafka en utilisant la méthode `produce` du producteur Kafka, avec le nom du patient comme clé, la représentation JSON du message comme valeur, et une fonction de rappel (`delivery_report`). Enfin, la fonction assure que le message est envoyé immédiatement en utilisant `producer.poll(0)` et termine en vidant le tampon du producteur avec `producer.flush()`.

La prochaine étape consiste simplement à récupérer les données de notre datalake comme suit :

```python
# Remplacez avec les détails de votre cluster HDFS
hdfs_url = "http://localhost:9870"
data_lake_path = "data_lake/parkinson_data"

client = InsecureClient(hdfs_url)

# Liste des fichiers dans le répertoire Data Lake
files_in_data_lake = client.list(data_lake_path)
```

Après ça, on se connecter à notre producer kafka avec le code suivant :

```python
# Configuration du producteur Kafka
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'client.id': 'python-producer'
}

# Création de l'instance du producteur Kafka
producer = Producer(producer_conf)
```

Maintenant qu'on accès aux données de note data lake et que notre producer kafka est pret nous envoyons le contenu de nos fichier CSV ligne par ligne pour ne pas dépasser la limite de taille des messages autorisé sur kafka on parcours donc l'intégralité de nos fichier ligne par ligne comme montrer dans la code suivant :

```python
# Lire le contenu de chaque fichier
for file_name in files_in_data_lake:
    hdfs_file_path = f"{data_lake_path}/{file_name}"
    
    with client.read(hdfs_file_path, encoding='utf-8') as reader:
        file_content = reader.read()
    
    for line in file_content.split("\n"):
        if line == "":
            continue
        produce_sensor_data(producer, "sensor_data", file_name.split(".")[0], line)

print("Toutes les données du data lake on était transmises pour prétraitement")
```

Une fois terminé le programme indique qu'il envoyé l'integralité des données et s'arrete.

### 4) Réception des données et prétraitement avec Spark :

Les données sont transmises à un notre programme coder dans le fichier [data_preprocess.py](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/blob/main/data_preprocess.py) qui a recours aux librairies suivantes :

```python
from confluent_kafka import Consumer, KafkaError
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType
from pyspark.sql.functions import col, row_number
from pyspark.sql import Window
from pyspark.sql.functions import mean
import numpy as np
```

Cette partie nécessite 2 fonctions la première consistant à récupérer les données transmises par kafka comme montrer dans le bloc de code ci dessous :

```python
def retrieve_data(consumer, topic):
    """
    Récupérer les données à partir d'un topic Kafka.
    """
    consumer.subscribe([topic])
    patient_data = dict()
    print("En attente de messages...")
    first_message = False
    count = 0
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            if first_message:
                break
            else:
                continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break

        try:
            first_message = True
            data = json.loads(msg.value())
            row = data["content"].split(";")
            if len(row) == 19:
                row = [float(x) for x in row]
                row.append(0 if data["group"] == "Co" else 1)
                if data["file_name"] not in patient_data:
                    patient_data[data["file_name"]] = []
                patient_data[data["file_name"]].append(row)
            
        except json.JSONDecodeError as e:
            print(f"Erreur de décodage JSON : {e}")
        except KeyError as e:
            print(f"Clé manquante dans le JSON : {e}")

    consumer.close()
    print("Terminé")
    return patient_data
```

Cette fonction est conçue pour récupérer des données depuis un topic Apache Kafka. Elle prend en paramètres un consommateur Kafka (`consumer`) et le sujet Kafka depuis lequel les données doivent être récupérées (`topic`). La fonction souscrit au sujet spécifié, initialise un dictionnaire vide pour stocker les données des patients, puis entre dans une boucle qui écoute continuellement les messages Kafka. Lorsqu'un message est reçu, la fonction le décode depuis le format JSON, extrait et transforme les données de la colonne "content" au format liste de nombres, ajoute une valeur binaire indiquant le groupe du patient, et stocke la ligne résultante dans le dictionnaire sous la clé correspondant au nom du fichier. La boucle continue jusqu'à ce qu'aucun nouveau message ne soit reçu pendant une seconde. Finalement, le consommateur Kafka est fermé, et le dictionnaire contenant les données des patients est renvoyé.

Cela facilitant le travail de la deuxième fonction chargé du prétraitement des données qui prend en entré le résultat de la fonction précédante et qui réalise le traitement des données comme suit :

```python
def preprocess_data(patient_data):
    """
    Prétraiter les données en calculant la moyenne pour chaque groupe de 100 lignes.
    """
    spark = SparkSession.builder.appName('PatientData').getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    schema = StructType([ 
        StructField("Time", FloatType(), True),  
        StructField("L1", FloatType(), True), 
        StructField("L2", FloatType(), True), 
        StructField("L3", FloatType(), True),
        StructField("L4", FloatType(), True),
        StructField("L5", FloatType(), True),
        StructField("L6", FloatType(), True),
        StructField("L7", FloatType(), True),
        StructField("L8", FloatType(), True),
        StructField("R1", FloatType(), True), 
        StructField("R2", FloatType(), True), 
        StructField("R3", FloatType(), True),
        StructField("R4", FloatType(), True),
        StructField("R5", FloatType(), True),
        StructField("R6", FloatType(), True),
        StructField("R7", FloatType(), True),
        StructField("R8", FloatType(), True),
        StructField("L", FloatType(), True),
        StructField("R", FloatType(), True),
        StructField("Class", IntegerType(), True)
    ])

    for patient in patient_data:
        mean_values = []
        patient_data[patient] = spark.createDataFrame(patient_data[patient], schema)
        lenght = patient_data[patient].count()

        for i in range(0, lenght, 100):
            df_with_row_number = patient_data[patient].withColumn("row_number", row_number().over(Window.orderBy("Time")))

            if i+100 > lenght:
                end = lenght
            else:
                end = i+100
            result_df = df_with_row_number.filter((col("row_number") >= i) & (col("row_number") < end))

            result_df = result_df.drop("row_number")

            mean_values.append(np.asarray(result_df.select(mean(result_df.L1), mean(result_df.L2), mean(result_df.L3), \
                            mean(result_df.L4), mean(result_df.L5), mean(result_df.L6), \
                            mean(result_df.L7), mean(result_df.L8), mean(result_df.R1), \
                            mean(result_df.R2), mean(result_df.R3), mean(result_df.R4), \
                            mean(result_df.R5), mean(result_df.R6), mean(result_df.R7), \
                            mean(result_df.R8), mean(result_df.L), mean(result_df.R), \
                            mean(result_df.Class)).collect()).tolist()[0])
        
        patient_data[patient] = mean_values

    return patient_data
```

Conçue pour effectuer le prétraitement des données en utilisant Apache Spark. Elle prend en entrée un dictionnaire de données de patients (`patient_data`) et calcule la moyenne pour chaque groupe de 100 lignes dans chaque jeu de données du patient. La fonction utilise la bibliothèque Spark pour créer un DataFrame avec un schéma spécifié, puis itère sur chaque patient dans le dictionnaire. Pour chaque patient, elle divise le DataFrame en groupes de 100 lignes, calcule la moyenne pour chaque groupe sur les colonnes spécifiées, et stocke les valeurs moyennes dans une liste. Le résultat final est une mise à jour du dictionnaire `patient_data` avec les nouvelles valeurs moyennes pour chaque patient.

Une fois terminer il suffit simplement de lancer le consomateur kafka et de faire appel à nos 2 fonctions :

```python
# Configuration du consommateur Kafka
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'python-consumer',
    'auto.offset.reset': 'earliest'
}

# Création de l'instance du consommateur Kafka
consumer = Consumer(consumer_conf)

patient_data = retrieve_data(consumer, "sensor_data")

preprocessed_data = preprocess_data(patient_data)
```

Cela nous donne un ensemble de données prétraitées qui suffit simplement de stocker pour les utiliser ultérieurement.

### 5) Stockage des données prétraitées dans Cassandra :

Tojours sur le meme fichier python [data_preprocess.py](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/blob/main/data_preprocess.py) nous stockons les données obtenu après le prétraitement dans une base de données cassandra, pour cela nous rajoutons à nos import la ligne de code suivante :

```python
from cassandra.cluster import Cluster
```

Par la suite nous créons la classe suivante :

```python
class Cassandra:
    def __init__(self, create_keyspace_command, data):
        """
        Initialiser une connexion à Cassandra, créer un espace de clés et une table,
        puis insérer des données dans la table.
        """
        self.cluster = Cluster()
        self.session = self.cluster.connect()
        self.session.execute(create_keyspace_command)
        self.session.execute("USE Parkinson")
        self.session.execute("""
            CREATE TABLE IF NOT EXISTS data (
                L1 FLOAT,
                L2 FLOAT,
                L3 FLOAT,
                L4 FLOAT,
                L5 FLOAT,
                L6 FLOAT,
                L7 FLOAT,
                L8 FLOAT,
                R1 FLOAT,
                R2 FLOAT,
                R3 FLOAT,
                R4 FLOAT,
                R5 FLOAT,
                R6 FLOAT,
                R7 FLOAT,
                R8 FLOAT,
                L FLOAT,
                R FLOAT,
                Class INT,
            )
        """)
        self.session.execute("TRUNCATE data")
        self.insert_data(data)

    def insert_data(self, data):
        """
        Insérer des données dans la table Cassandra.
        """
        for d in data:
            self.session.execute(f"""
                INSERT INTO data (
                    L1, L2, L3, L4, L5, L6, L7, L8, R1, R2, R3, R4, R5, R6, R7, R8, L, R, Class
                ) VALUES (
                    {d[0]}, {d[1]}, {d[2]}, {d[3]}, {d[4]}, {d[5]}, {d[6]}, {d[7]}, {d[8]}, {d[9]}, {d[10]}, {d[11]}, {d[12]}, {d[13]}, {d[14]}, {d[15]}, {d[16]}, {d[17]}, {d[18]}
                )
            """)

    def close(self):
        """
        Fermer la connexion Cassandra.
        """
        self.cluster.shutdown()
```

Cette classe Python, nommée `Cassandra`, est conçue pour faciliter l'intégration et la manipulation de données dans Cassandra. Son constructeur prend en paramètres une commande pour créer un keyspace Cassandra (`create_keyspace_command`) et des données à insérer (`data`). La classe initialise une connexion à Cassandra, crée un keyspace et une table appelée "data" avec des colonnes spécifiques représentant des valeurs de capteur et une classe. Ensuite, elle insère les données fournies dans la table à l'aide de la méthode `insert_data`.

La méthode `insert_data` itère sur les données fournies et exécute des requêtes d'insertion Cassandra pour chaque ensemble de données. La méthode `close` permet de fermer proprement la connexion à Cassandra.

Pour finir, nous créons la requete permettant de créer l'espace de clés cassandra et nous sauvegardons les données dans la base de données :

```python
# Définition des paramètres de l'espace de clés Cassandra
keyspace_name = 'ParkinsonData'
replication_strategy = 'SimpleStrategy'
replication_factor = 3

# Création de la requête de création de l'espace de clés
create_keyspace_query = f"""
    CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
    WITH replication = {{'class': '{replication_strategy}', 'replication_factor': {replication_factor}}};
"""

# Initialisation de l'instance Cassandra, insertion des données, sélection des données et fermeture de la connexion
cassandra = Cassandra(create_keyspace_query, preprocessed_data)
cassandra.close()
```

### 6) Création du API REST de machine learning avec Flask et Sickit-learn :

L'objectif principale de notre plateforme étant le diagnostique des patient il est temps de créer l'API REST contenant le modèle réalisant le diagnostic. Pour cela nous créons le fichier [model_api.py](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/blob/main/model_api.py) et nous importons les librairies suivantes :

```python
from flask import Flask, request, jsonify
from flask_cors import CORS, cross_origin
import pandas as pd
import numpy as np
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score, roc_auc_score, roc_curve, auc
from sklearn.metrics import confusion_matrix
from sklearn.model_selection import train_test_split
from mlxtend.evaluate import bias_variance_decomp
from sklearn.neighbors import KNeighborsClassifier
import pickle
```

Nous commençons par initialiser notre application flask comme suit :

```python
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*", "allow_headers": {"Access-Control-Allow-Origin"}}})
app.config['CORS_HEADERS'] = 'Content-Type'
```

Puis nous créons notre endpoint de la manière suivante :

```python
@app.route('/api/model', methods=['POST'])
@cross_origin(origin='*', headers=['content-type'])
def model():
    """
    API endpoint pour entraîner un modèle KNN sur les données fournies.
    """
    if request.method == 'POST':
        data = request.files.get('data')
        columns_name = ["L1", "L2", "L3", "L4", "L5", "L6", "L7", "L8", "R1", "R2", "R3", "R4", "R5", "R6", "R7", "R8", "L", "R", 'Class']
        df = pd.DataFrame(data)

        X = df.drop('Class', axis=1)
        y = df['Class']
        
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
        
        clf = KNeighborsClassifier(n_neighbors=5)
        clf.fit(X_train, y_train)

        # sauvegarder le modèle avec pickle
        filename = 'knn.sav'
        pickle.dump(clf, open(filename, 'wb'))

        # charger le modèle avec pickle
        loaded_model = pickle.load(open(filename, 'rb'))
        
        y_pred = clf.predict(X_test)

        # Évaluer les performances du classifieur
        accuracy = accuracy_score(y_test, y_pred)
        precision = precision_score(y_test, y_pred, average='weighted')
        recall = recall_score(y_test, y_pred, average='weighted')
        f1 = f1_score(y_test, y_pred, average='weighted')

        # Courbe ROC pour une classification multi-classe
        y_prob = clf.predict_proba(X_test).argmax(axis=1)
        macro_roc_auc_ovo = roc_auc_score(y_test.to_numpy(), y_prob, multi_class="ovo", average="macro")

        # Matrice de confusion
        cm = confusion_matrix(y_test, y_pred)

        # Obtenir les valeurs TP, TN, FP, FN
        FP = cm.sum(axis=0) - np.diag(cm)  
        FN = cm.sum(axis=1) - np.diag(cm)
        TP = np.diag(cm)
        TN = cm.sum() - (FP + FN + TP)

        # Obtenir le biais et la variance du classifieur
        loss, bias, var = bias_variance_decomp(clf, X_train, y_train.to_numpy(), X_test, y_test.to_numpy(), loss='0-1_loss', random_seed=23)

        return jsonify({'model': filename,
                        'accuracy': accuracy,
                        'precision': precision,
                        'recall': recall,
                        'f1': f1,
                        'macro_roc_auc_ovo': macro_roc_auc_ovo,
                        'confusion_matrix': cm,
                        'TP': TP,
                        'TN': TN,
                        'FP': FP,
                        'FN': FN,
                        'bias': bias,
                        'variance': var,
                        'loss': loss})
```

Notre endpoint est accessible par la méthode HTTP POST. Lorsqu'une requête POST est reçue, la fonction `model` est déclenchée. Cette fonction charge des données envoyées dans la requête, les transforme en DataFrame Pandas, puis divise les données en ensembles d'entraînement et de test. Un modèle de classification des k plus proches voisins (KNN) est ensuite entraîné sur les données d'entraînement. Le modèle est sauvegardé en utilisant le module pickle, puis rechargé. Les performances du modèle sont évaluées à l'aide de métriques telles que l'exactitude, la précision, le rappel et le score F1. Une courbe ROC est générée pour une classification multi-classe, et une matrice de confusion est calculée. Les valeurs des vrais positifs, vrais négatifs, faux positifs et faux négatifs sont obtenues, tout comme le biais, la variance et la perte du classifieur. Les résultats de l'évaluation du modèle ainsi que diverses métriques sont renvoyés sous forme de réponse JSON.

Pour terminer on lance notre API avec le code suivant :

```python
if __name__ == '__main__':
    app.run(debug=True)
```

### 7) Envoie des données de Cassandra a l'API :

Notre API étant maintenant opérationel il est temps de lui faire appel en lui envoyant les données qu'on avait stocké sur cassandra. Nous réaliserons cela dans un programme créer dans le fichier [request_model.py](https://github.com/IliesChibane/Projet-IoT-Cloud-BigData/blob/main/request_model.py). Comme à notre habitude nous débutons par importer les libraires dont nous avons besoin :

```python
import requests
from cassandra.cluster import Cluster
import numpy as np
```

Ensuite, nous recréons notre classe Cassandra sans cependant recréer les espaces de clés ou la table et sans non plus réaliser l'insertion cela ayant dèja été effectué plus tot, cependant nous ajoutons une nouvelle fonction `get_data` dans notre classe qui via une requete CQL très simple nous permet de récupérer l'intégralité de nos données stockées.

```python
class Cassandra:
    def __init__(self):
        """
        Initialiser une connexion à Cassandra et utiliser l'espace de clés Parkinson.
        """
        self.cluster = Cluster()
        self.session = self.cluster.connect()
        self.session.execute("USE Parkinson")

    def get_data(self):
        """
        Récupérer les données à partir de la table Cassandra.
        """
        return np.asarray(self.session.execute("SELECT * FROM data"))

    def close(self):
        """
        Fermer la connexion à Cassandra.
        """
        self.cluster.shutdown()
```

Maintenant nous récupérons nos données de la base sous format de `numpy array` et nous les envoyons à notre API via une requete `POST` nous et en utilisant son endpoint comme url.

```python
url = 'http://127.0.0.1:5000/api/model'

# Initialiser la connexion à Cassandra, récupérer les données et fermer la connexion
cassandra = Cassandra()
dataset = cassandra.get_data()
cassandra.close()

# Envoyer les données au serveur Flask
response = requests.post(url, files={'data': dataset})
```

### 8) Stockage des données retournées par l'API dans MongoDB :

Les résultats de notre modèle ayant été récupéré il faut maintenant les stocker, pour cela nous utilisons MongoDB et cela toujours dans le meme programme en ajoutant simplement pour commencer cette ligne de code à nos import :

```python
import pymongo
```

Et a la fin de notre code nous ajoutons ceci :

```python
# Initialiser la connexion à MongoDB
myclient = pymongo.MongoClient("mongodb://localhost:27017")
mydb = myclient["mydatabase"]
mycol = mydb["parkinson"]

# Insérer un document de test, puis le supprimer
mycol.insert_one({"test": "test"})
mycol.drop()

# Réinitialiser la collection
mycol = mydb["parkinson"]

# Vérifier si la base de données existe
dblist = myclient.list_database_names()
if "mydatabase" in dblist:
    print("La base de données existe.")
else:
    print("La base de données n'existe pas.")

# Insérer le résultat de la requête Flask dans MongoDB
mycol.insert_one(response.json())

print("Modèle et métriques d'évaluation sauvegardés dans MongoDB.")
```

Une connexion à une base de données MongoDB locale. une base de données nommée `mydatabase` aisni qu'une collection appelée `parkinson` sont créé au sein de cette base de données. Ensuite, la collection est réinitialisée et vérifiée pour confirmer son existence. Si la base de données `mydatabase` existe, elle est imprimée dans la console, sinon, un message indiquant qu'elle n'existe pas est affiché.

Le script insère ensuite le résultat de notre requête Flask dans la collection MongoDB `parkinson`. Enfin, un message indiquant que le modèle et les métriques d'évaluation ont été sauvegardés dans MongoDB est affiché dans la console.

### 9) Extraction des données avec ElasticSearch :

### 10) Visualisation des données avec Kibana :
