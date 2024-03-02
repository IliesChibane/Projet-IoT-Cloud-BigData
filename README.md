# Création d'une plateforme IoT Cloud et Big Data pour le diagnostique de la maladie de parkinson via des données de capteurs

Dans ce tutoriel nous allons voir comment réaliser un pipeline permettant de récupérer des données de capteur et les exploiter de manière optimal afin d'obtenir une plateforme scalable permettant de se mettre à jour aisément dans le cadre du diagnostique de parkison.

Le pipeline réalisé dans ce tutoriel sera comme illustré dans la figure ci dessous :

![pipeline](readme-images/pipeline.png?raw=true)

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

### 3) Envoie des données du Data Lake via kafka :

### 4) Réception des données et prétraitement avec Spark :

### 5) Stockage des données prétraitées dans Cassandra :

### 6) Création du API REST de machine learning avec Flask et Sickit-learn :

### 7) Envoie des données de Cassandra a l'API :

### 8) Stockage des données retournées par l'API dans MongoDB :
