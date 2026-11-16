# Collecte & Ingestion de Données

> **Source :** cours Gamma « Collecte & Ingestion de Données », Master 1 Data & IA.
> <https://gamma.app/docs/Collecte-Ingestion-de-Donnees-pfme571w959rgkb?mode=doc>
> Récupéré le 2026-11-16, texte intégral.
>
> **Notes de récupération :**
> - Certains schémas affichaient leurs étapes à l'envers (de droite à gauche) : je les ai remis dans l'ordre logique, signalé par *(ordre rétabli)*.
> - Trois blocs de code (producer, consumer, SQLAlchemy) n'ont pas d'indentation dans le document d'origine : je l'ai rétablie, signalé par *(indentation rétablie)*.
> - Les 5 images du cours sont des illustrations décoratives, non reprises.
> - Les dates des exemples (2027) sont celles du cours.

---

## Présentation du module

Un module complet pour maîtriser les architectures d'ingestion scalables, de Kafka à Airflow, des connecteurs à la qualité des données. Chaque séance alterne théorie, exemples concrets et ateliers interactifs autour d'un projet fil rouge e-commerce.

**🎯 Objectifs du module**
- Concevoir et déployer des pipelines d'ingestion robustes
- Maîtriser Kafka, Airflow et les connecteurs personnalisés
- Mettre en place monitoring et validation précoce

**📋 Évaluation**
- Projet fil rouge par groupe (pipeline complet)
- QCM de connaissances
- Soutenance finale

### Les 7 séances du module

Chaque paire de séances se déroule le même jour et se termine par un livrable projet. Les séances sont conçues pour être autonomes tout en s'enchaînant progressivement.

| # | Séances | Contenu |
|---|---|---|
| 01 | Séances 1-2 · Architecture & Kafka | Ingestion scalable, brokers, topics, producers, consumers, garanties de livraison |
| 02 | Séances 3-4 · Connecteurs | APIs REST, bases de données relationnelles, stockage objet, extraction incrémentale |
| 03 | Séances 5-6 · Orchestration & Monitoring | DAGs Airflow, retries, idempotence, alertes, logs centralisés |
| 04 | Séance 7 · Qualité des données | Validation précoce, normalisation, déduplication, DLQ, quarantaine |

### Projet fil rouge : construire un pipeline complet

**🛒 Cas d'école : e-commerce.** En cours, tous les exemples s'appuient sur un pipeline e-commerce réel : commandes, clients, produits. Chaque concept est illustré avec ce contexte concret avant d'être généralisé.

**🚀 Votre projet groupe.** Chaque groupe construit son propre pipeline sur la thématique de son choix. Livrables progressifs à chaque séance :

1. Fiche de cadrage (séance 1)
2. Topic Kafka & schéma (séance 2)
3. Connecteurs (séances 3-4)
4. DAG unifié (séance 5)
5. Monitoring (séance 6)
6. Validation & quarantaine (séance 7)

---

## Séance 1 · Architecture d'ingestion scalable

Comprendre les enjeux de volume, vélocité et variété. Découvrir les patterns fondamentaux (batch, streaming, pull, push, événementiel) et les concepts clés de Kafka pour choisir la bonne architecture.

- **📐 Concepts :** les 3 V, batch vs streaming, patterns d'ingestion
- **⚡ Kafka :** broker, topic, partition, offset, producteurs et consommateurs
- **🛠️ TP :** installer Kafka et Airflow avec Docker Compose
- **🎯 Livrable :** fiche de cadrage du projet (thématique, sources, volumes)

### Interactif : sondage d'accroche 🗳️

*Votre application passe de 10 à 10 millions d'événements par jour. Qu'est-ce qui casse en premier ?*

- 🗄️ **La base de données** : les connexions saturent, les requêtes s'accumulent
- 🌐 **Le réseau** : la bande passante est dépassée, les paquets se perdent
- 💻 **Le code** : les traitements synchrones ne tiennent pas la charge
- 💰 **Le budget** : les coûts d'infrastructure explosent sans prévenir
- 📊 **La qualité** : les données deviennent incohérentes à haute vélocité

On reviendra sur les réponses à la fin de la séance — gardez votre intuition en tête !

### Qu'est-ce que l'ingestion de données ?

L'ingestion de données désigne le processus de collecter des données provenant de sources variées et de les acheminer de façon fiable vers un système de stockage ou de traitement. C'est le premier maillon indispensable de toute chaîne de données.

```
Sources                    →  Ingestion                →  Stockage                      →  Usages
APIs, bases, fichiers,        Collecte fiable             Data lake et entrepôt central     BI, ML et alertes
événements                    et transport                                                  opérationnelles
```

L'ingestion est souvent sous-estimée : c'est pourtant là que se jouent la fiabilité, la latence et la scalabilité de tout le système aval. Une ingestion défaillante compromet chaque couche suivante.

### Les 3 V : Volume, Vélocité, Variété

- **Volume.** Combien ? Des gigaoctets aux pétaoctets. Le volume détermine le choix du stockage, le partitionnement et les stratégies de compression. Un pipeline doit absorber les pics sans saturer.
- **Vélocité.** À quelle vitesse ? Données temps réel (millisecondes) ou différées (batch nuitamment). La vélocité dicte le choix entre streaming et batch, et les exigences de latence.
- **Variété.** Quels formats ? JSON, CSV, Parquet, logs, XML, binaire. Chaque source parle son propre langage. L'ingestion doit normaliser sans perdre d'information.

Un 4e V s'ajoute : la **Véracité** (qualité et fiabilité des données). Ce pilier sera approfondi en séance 7 avec la validation précoce.

### Exemples concrets : quatre sources réelles

| Source | Volume | Vélocité | Variété | Contrainte clé |
|---|---|---|---|---|
| Clics e-commerce | Millions / jour | Temps réel | JSON, logs | Pas de perte |
| Capteurs IoT usine | 1 msg / sec / capteur | Continu | Binaire, MQTT | Ordre garanti |
| API météo | Modéré | Polling / 15 min | JSON REST | Rate limiting |
| Transactions bancaires | Milliers / sec | Quasi temps réel | ISO 20022, CSV | Exactitude totale |

Chaque source cumule souvent plusieurs V. Une transaction bancaire a un volume élevé, une vélocité forte ET une variété de formats selon les banques partenaires.

### Interactif : card sorting, quel V domine ? 🃏

Par binômes, attribuez à chaque source une carte « V dominant » parmi Volume, Vélocité ou Variété — et justifiez votre choix en une phrase.

- **🎯 Consigne :** chaque source reçoit une carte V dominant et une justification courte. Pas de bonne réponse unique : argumentez !
- **⚖️ Cas ambigus :** certaines sources cumulent plusieurs V. Le débat sur ces cas est l'essentiel de l'exercice : comment hiérarchiser les contraintes ?
- **💬 Restitution :** chaque binôme présente sa carte la plus difficile. Les désaccords entre groupes sont débattus collectivement.

### Ingestion par lots (batch)

**⚙️ Principe.** Les données sont collectées et traitées par intervalles réguliers : toutes les heures, chaque nuit, chaque semaine. Un job s'exécute, traite le lot, puis s'arrête.

**✅ Avantages**
- Simple à implémenter et à déboguer
- Économique : ressources utilisées en pic court
- Facile à rejouer sur une période passée
- Compatible avec les outils ETL classiques

**⚠️ Limites**
- Latence : les données ont au moins l'âge de l'intervalle
- Pics de charge : toute la charge arrive d'un coup à l'heure H
- Fenêtre manquée : un échec peut décaler toute la chaîne

**📌 Cas d'usage typiques**
- Rapports journaliers, exports comptables
- Migration d'historiques volumineux
- Chargement nuitamment d'un entrepôt

### Ingestion en streaming

**⚡ Principe.** Chaque événement est traité dès son arrivée, sans attendre un lot. Le système est toujours en écoute et réagit en continu.

**✅ Avantages**
- Faible latence : de la milliseconde à la seconde
- Réactivité immédiate aux événements critiques
- Chargement uniforme, sans pics

**⚠️ Limites**
- Complexité accrue : ordre des messages, doublons, retards
- Coût : infrastructure toujours active
- Débogage plus difficile qu'un job batch

**🔄 Le micro-batch comme compromis.** Traiter de très petits lots toutes les quelques secondes. Spark Structured Streaming utilise cette approche : on garde la simplicité du batch avec une latence quasi-temps réel.

### Batch vs streaming : comparaison

| Critère | Batch | Streaming |
|---|---|---|
| Latence | Minutes à heures | Millisecondes à secondes |
| Complexité | Faible | Élevée (ordre, doublons) |
| Coût infra | Ressources ponctuelles | Infrastructure permanente |
| Rejouabilité | Naturelle (période bornée) | Possible via rétention Kafka |
| Cas d'usage | Rapports, migrations, ETL | Fraude, alertes, tableaux temps réel |

Ce n'est pas « l'un ou l'autre » : la plupart des architectures modernes (Lambda, Kappa) combinent les deux approches selon la criticité des données.

### Interactif : batch ou streaming ? Vote en direct 🗳️

Pour chacun des 6 scénarios, votez batch ou streaming — puis un volontaire justifie son choix avant le débat collectif.

- 🚨 **Détection de fraude** : une transaction suspecte doit être bloquée en temps réel
- 📊 **Rapport mensuel** : consolider les ventes du mois pour la direction
- 📺 **Dashboard temps réel** : afficher le nombre de commandes actives à l'instant T
- 🗃️ **Migration historique** : transférer 5 ans de données vers un nouveau système
- 🌡️ **Alertes capteurs** : déclencher une alarme si température > seuil
- 📧 **Newsletter** : envoyer un email hebdomadaire aux abonnés

### Les trois patterns d'ingestion

- **Pull.** Le système d'ingestion interroge la source activement : appel API planifié, requête SQL périodique. On contrôle le rythme, mais on ne voit que ce qu'on va chercher. Simple à implémenter.
- **Push.** La source envoie les données sans qu'on les demande : webhook, upload de fichiers. Faible latence, mais nécessite un point d'entrée robuste, capable d'absorber les pics et de s'authentifier.
- **Événementiel.** Les producteurs publient sur un bus (Kafka, RabbitMQ) et les consommateurs s'abonnent. Découplage total : un même événement sert plusieurs usages sans toucher le producteur.

### Exemple concret : commandes e-commerce, 3 patterns

Le même besoin — récupérer les commandes — peut être résolu de trois façons. Chacune a des implications très différentes sur la latence, le couplage et la résilience.

1. **Pull · Export SQL horaire.** Un job Airflow exécute `SELECT * FROM commandes WHERE updated_at > :watermark` chaque heure. Latence : jusqu'à 60 min. Simple mais couplé à la base de prod.
2. **Push · Webhook paiement.** La plateforme de paiement appelle notre endpoint HTTP à chaque transaction confirmée. Latence : quelques secondes. Risque : que se passe-t-il si notre endpoint est down ?
3. **Événementiel · Topic Kafka.** La plateforme publie sur `commandes`. Analytics, fraude et stock s'abonnent indépendamment. Latence : millisecondes. Résilient et découplé.

### Pourquoi Kafka s'impose

**😱 Sans bus central : le spaghetti.** Avec N sources et M destinations, chaque système se connecte directement à chacun des autres. Le résultat : N × M connexions point à point, fragiles, impossibles à maintenir, et chaque changement nécessite de toucher plusieurs systèmes.

**✨ Avec Kafka : hub central.** Chaque système ne se connecte qu'une seule fois au bus. Kafka découple totalement producteurs et consommateurs. Un nouveau consommateur s'ajoute sans toucher les sources existantes. La rétention permet de rejouer l'historique.

- *Sans bus central :* N sources vers M destinations = spaghetti
- *Avec broker Kafka :* mêmes sources et destinations via un broker unique

### Concepts fondamentaux de Kafka

- **Broker.** Serveur Kafka qui stocke et distribue les messages. Un cluster comporte plusieurs brokers pour la résilience.
- **Topic.** Flux nommé d'événements. Équivalent d'un canal de communication dédié à un type de données.
- **Partition.** Sous-ensemble ordonné d'un topic. Unité de parallélisme : plusieurs consumers peuvent lire en parallèle.
- **Offset.** Position d'un message dans une partition. Chaque consumer maintient son propre offset : il peut relire ou sauter des messages.

### Exemple concret : anatomie d'un topic Kafka

Un topic est un journal **append-only** : on ne peut qu'ajouter des messages à la fin, jamais modifier ce qui est écrit. L'offset croît infiniment. Les messages sont conservés selon une durée de rétention configurable.

- **Producteur :** ajoute des messages en fin
- **Journal :** log append-only avec offsets
- **Offsets :** 0, 1, 2, 3, 4, 5… croissants
- **Consommateurs :** lisent indépendamment aux offsets
- **Rétention :** fenêtre temporelle de conservation

**🔑 Propriété clé.** Plusieurs consumers peuvent lire le même topic à leurs propres offsets. Chacun avance à son rythme sans affecter les autres.

**⏱️ Rétention.** Par défaut 7 jours. Pendant cette période, un consumer peut relire des messages passés — précieux pour rejouer un traitement raté.

### Interactif : Human Kafka 🎭

Un jeu de rôle pour ancrer les concepts de partitions, offsets et consommateurs de façon mémorable.

1. **Les producteurs.** Plusieurs étudiants reçoivent des cartes « événement » avec une clé `id_client`. Ils les envoient aux partitions selon la règle de hachage.
2. **Les partitions.** 3 étudiants tiennent des feuilles. Chaque nouveau message est ajouté à la suite — l'ordre est garanti dans la partition.
3. **Les consommateurs.** D'autres étudiants notent leur offset courant et avancent indépendamment. Observer : l'ordre global n'est pas garanti entre partitions.

**Observation clé :** deux événements du même client arrivent toujours dans la même partition → ordre garanti pour ce client.

### Exemple concret : clés et ordonnancement

Dans un topic `commandes` à 3 partitions, si la clé du message est `id_client`, Kafka applique un hachage déterministe : toutes les commandes d'un même client arrivent dans la même partition, donc dans l'ordre chronologique strict.

Une clé mal choisie (trop peu de valeurs distinctes) crée des partitions déséquilibrées, appelées **« hot partitions »**, qui saturent pendant que d'autres restent vides.

### Kafka vs Airflow : qui fait quoi ?

**⚡ Kafka.** Transport et stockage temporaire d'un flux d'événements en continu. Kafka ne sait pas ce que sont les tâches, les dépendances ou les planifications — il livre des messages, point.
- Découplage producteurs / consommateurs
- Rétention et rejouabilité
- Haute vélocité (millions de msg/s)

**🗓️ Airflow.** Orchestration de tâches planifiées avec gestion des dépendances, reprises en cas d'échec et suivi de l'exécution. Airflow ne stream pas de données.
- DAGs avec dépendances complexes
- Retries automatiques et backfill
- Interface de monitoring des exécutions

Les deux sont complémentaires : Airflow orchestre les connecteurs qui alimentent Kafka. Airflow ne remplace pas un bus d'événements.

### Architecture complète : vue d'ensemble du pipeline

```
Ingestion  →  Orchestration  →  Streaming  →  Validation & Stockage
```
*(ordre rétabli)*

Cette architecture est le fil conducteur du module. Chaque séance approfondit un composant : connecteurs (3-4), Kafka (1-2), Airflow (5), monitoring (6), validation (7). Le projet groupe est une instance de ce schéma sur votre thématique.

### Activité groupe : conception d'architecture 🏗️

Groupes de 3-4 étudiants. Un cas réel est tiré au sort parmi : plateforme de VTC, supermarché connecté, réseau de capteurs météo, application bancaire, réseau social.

1. **Schéma.** Dessiner : sources, patterns (pull/push/événementiel), Kafka, stockage, orchestration Airflow.
2. **Justifications.** Expliquer 3 choix d'architecture : pourquoi ce pattern ? pourquoi ce nombre de partitions ? pourquoi batch ou streaming ?
3. **Restitution.** Présentation rapide par groupe. Un autre groupe pose obligatoirement une question critique.

### TP : installation de Kafka & Airflow avec Docker Compose

**📋 Prérequis**
- Docker Desktop installé et lancé
- Docker Compose v2+
- Au moins 4 Go de RAM disponibles

**🚀 Démarrage**
```bash
git clone https://github.com/cours/data-ingestion-tp
cd data-ingestion-tp
docker compose up -d
docker compose ps
```

**✅ Vérification**
- Créer un topic : `kafka-topics.sh --create`
- Publier un message en ligne de commande
- Ouvrir Airflow sur `localhost:8080`

**⚠️ Erreurs fréquentes**
- Port 9092 ou 8080 déjà occupé → changer dans `.env`
- Mémoire insuffisante → fermer autres apps
- Conteneur qui redémarre → `docker logs nom_conteneur`

### Exemple de code : Docker Compose, structure clé

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0
    # Mode KRaft : pas besoin de ZooKeeper
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
    ports:
      - "9092:9092"
    volumes:
      - kafka-data:/var/lib/kafka/data

  airflow:
    image: apache/airflow:2.9.0
    ports:
      - "8080:8080"
    environment:
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: sqlite:////opt/airflow/airflow.db
    volumes:
      - ./dags:/opt/airflow/dags
```

Kafka tourne en mode **KRaft** (sans ZooKeeper depuis Kafka 3.3+). Airflow utilise SQLite pour simplifier le TP — en production, on privilégie PostgreSQL.

### Livrable séance 1 : fiche de cadrage

**📌 À définir**
- Thématique et problématique métier
- Au moins 2 sources (ex. : API REST + base de données ou stockage objet)
- Volumes estimés (messages/jour, taille moyenne)
- Usage final des données (ML, dashboard, alertes…)

**📄 Format attendu.** Une page au format README ou PDF, déposée avant la séance 2. Elle servira de contrat pour les livrables suivants.

**💡 Exemples de thématiques**
- Suivi de livraisons en temps réel
- Consommation énergétique d'un campus
- Flux Twitter/Bluesky sur un sujet
- Prix de produits sur un site e-commerce

### Récap séance 1 : synthèse & quiz éclair ⚡

- **Les 3 V :** Volume, Vélocité, Variété — plus la Véracité (séance 7)
- **Batch / Streaming :** latence vs complexité, souvent combinés en production
- **Pull / Push / Événementiel :** trois façons de faire circuler les données entre systèmes
- **Kafka :** broker, topic, partition, offset — le bus central du module

Quiz éclair de 5 questions sur Mentimeter, puis on revient sur le sondage d'ouverture : qu'est-ce qui casse en premier quand on passe à 10M d'événements ? La réponse est souvent… plusieurs choses à la fois.

---

## Séance 2 · Kafka en pratique

Écrire de vrais producers et consumers en Python. Comprendre les garanties de livraison, maîtriser les offsets et passer à l'échelle avec des consumer groups.

- **🐍 Producer Python :** sérialisation, clé, acks, flush et callbacks de livraison
- **📥 Consumer Python :** groupes, boucle de poll, commits manuels et automatiques
- **🔒 Garanties :** at-most-once, at-least-once, exactly-once et idempotence
- **⚖️ Scaling :** consumer groups, rebalancing et consumer lag

### Workflow du producer

```
Créer le message  →  Sérialiser  →  Choisir la partition  →  Envoyer en lots
```
*(ordre rétabli)*

- **🚀 Performance.** Les lots (`batch.size`, `linger.ms`) regroupent plusieurs messages avant l'envoi. Plus de débit, mais légère latence supplémentaire.
- **🔒 Garanties.** Le niveau d'acquittement (`acks`) détermine quand le producer considère le message « livré ». Ce choix est le compromis central latence / durabilité.

### Exemple de code : un producer minimal en Python

*(indentation rétablie ; le dernier commentaire est vide dans le cours)*

```python
from confluent_kafka import Producer
import json

p = Producer({"bootstrap.servers": "localhost:9092"})

event = {
    "id_commande": 1001,
    "id_client": 42,
    "montant": 59.90
}

def on_delivery(err, msg):
    if err:
        print(f"Erreur livraison : {err}")
    else:
        print(f"Livré : partition={msg.partition()}, offset={msg.offset()}")

p.produce(
    "commandes",
    key="42",  # id_client comme clé de routage
    value=json.dumps(event),
    callback=on_delivery
)
p.flush()  #
```

### Sérialisation et schéma d'événement

**📦 Formats de sérialisation**

| Format | Avantages | Limites |
|---|---|---|
| JSON | Lisible, universel | Verbeux, pas de typage |
| Avro | Compact, schéma explicite | Besoin d'un registry |
| Protobuf | Très compact, rapide | Compilation requise |

**📋 Enveloppe d'événement recommandée**

```json
{
  "event_id": "uuid-v4",
  "event_type": "commande_créée",
  "event_time": "2027-01-18T10:00:00Z",
  "source": "api-boutique",
  "version": "1.0",
  "payload": { ... }
}
```

L'`event_id` permet la déduplication. La `version` gère l'évolution du schéma.

### Les niveaux d'acquittement (acks)

- **`acks=0` · Fire and forget.** Le producer n'attend aucune confirmation. Débit maximal, latence nulle — mais aucune garantie. Un message envoyé peut être perdu. Acceptable pour des métriques non critiques.
- **`acks=1` · Leader confirme.** Le broker leader accuse réception avant que le message soit répliqué. Compromis raisonnable. Risque résiduel : si le leader tombe avant réplication, le message est perdu.
- **`acks=all` · Tous les réplicas.** Tous les réplicas synchronisés confirment. Durabilité maximale. Latence plus élevée. Obligatoire pour les données financières ou critiques.

### Exemple de code : un consumer minimal en Python

*(indentation rétablie)*

```python
from confluent_kafka import Consumer
import json

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "analytics",          # Identifiant du groupe
    "auto.offset.reset": "earliest",  # Lire depuis le début si nouveau groupe
    "enable.auto.commit": False,      # Commit manuel pour plus de contrôle
})

c.subscribe(["commandes"])

try:
    while True:
        msg = c.poll(1.0)  # Attendre 1 seconde max
        if msg is None:
            continue
        if msg.error():
            print(f"Erreur consumer : {msg.error()}")
            continue
        event = json.loads(msg.value())
        # ... traitement métier ...
        c.commit(message=msg)  # Commit APRÈS traitement réussi
finally:
    c.close()  # Libérer les ressources
```

### Offsets et commits : le piège classique

- **Cas A : commit avant traitement.** Le consumer commit l'offset, puis tombe en panne avant de traiter le message. Au redémarrage, l'offset est déjà avancé : le message ne sera jamais traité. **Perte de données.**
- **Cas B : commit après traitement ✅.** Le consumer traite le message, puis commit. En cas de crash entre les deux, le message est rejoué au redémarrage. **Risque de doublon**, géré par l'idempotence du traitement.

```
Réception          →  Traitement            →  Crash                  →  Redémarrage
Message reçu          Application traite        Processus tombe           Consumer redémarre
par le consumer       le message                en panne                  et reprend
```

**La règle d'or :** toujours commiter après un traitement réussi. Couplez cela à un traitement idempotent pour absorber les doublons sans dommage.

### Interactif : trouvez les bugs dans ce consumer 🐛

Ce consumer contient 3 défauts volontaires. Par binômes, annotez le code, puis correction collective.

```python
from confluent_kafka import Consumer
import json

c = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "analytics",
    "enable.auto.commit": True,   # BUG 1 : commit automatique avant traitement
})
c.subscribe(["commandes"])

while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    # BUG 2 : pas de gestion de msg.error()
    event = json.loads(msg.value())
    traiter(event)
# BUG 3 : pas de c.close() → les partitions restent bloquées dans le groupe
```

Bug 1 : commit auto avant traitement → perte possible. Bug 2 : une erreur de partition fait planter. Bug 3 : le groupe ne se rééquilibre pas proprement à l'arrêt.

### Consumer groups et scaling

Au-delà du nombre de partitions, les consumers supplémentaires restent inactifs. Dimensionner le nombre de partitions dès la création du topic, car l'augmenter en production est complexe.

### Garanties de livraison

- **At-most-once.** Zéro doublon mais possible perte. Commit avant traitement. Acceptable pour des logs non critiques.
- **At-least-once.** Zéro perte mais doublons possibles. Commit après traitement. Standard de facto avec traitement idempotent.
- **Exactly-once.** Ni perte ni doublon. Transactions Kafka. Complexe et coûteux — réservé aux cas critiques.

**✅ Recommandation pratique.** At-least-once + traitement idempotent est le compromis le plus courant. Rejouer un message deux fois doit produire le même résultat.

**🔁 Consumer idempotent.** Utiliser un `UPSERT` sur `event_id` plutôt qu'un `INSERT` simple. Si le message est traité deux fois, le résultat est identique.

### Livrable séance 2 : votre topic d'ingestion

Définissez dès maintenant l'architecture Kafka de votre projet. Ces choix sont difficiles à modifier ensuite.

1. **Nommage du topic.** Convention : `source.entité.version` — ex. `boutique.commandes.v1`. Un nom clair évite les confusions dans un cluster partagé.
2. **Nombre de partitions.** Dimensionner selon le parallélisme souhaité et le volume. Commencer par 3 est raisonnable pour un projet.
3. **Clé et format.** Quelle clé garantit l'ordre nécessaire ? JSON pour démarrer, avec l'enveloppe standard `event_id` + `payload`.

**Livrable :** section « Topic d'ingestion » complétée dans le README du projet, avec justification des choix.

---

## Séance 3 · Connecteurs personnalisés : APIs REST

Concevoir un connecteur réutilisable, robuste et maintenable. Gérer authentification, pagination et limites de débit. Extraire de façon incrémentale sans rater ni dupliquer de données.

- **🔌 Interface :** `BaseConnector` réutilisable pour REST, bases et stockage
- **🔑 Auth :** API Key, OAuth2 client credentials, variables d'environnement
- **📄 Pagination :** offset, curseur, en-tête Link — et leurs pièges
- **⏱️ Incrémental :** état persistant, déduplication, gestion des erreurs HTTP

### Exemple de code : interface BaseConnector

```python
from abc import ABC, abstractmethod
from confluent_kafka import Producer

class BaseConnector(ABC):
    """Interface commune à tous les connecteurs du module."""

    @abstractmethod
    def extract(self, since):
        """Extraire les données depuis la source depuis un point de reprise."""
        ...

    @abstractmethod
    def to_event(self, record) -> dict:
        """Transformer un enregistrement brut en enveloppe d'événement standard."""
        ...

    def run(self, producer: Producer, topic: str):
        """Boucle d'extraction-publication commune à tous les connecteurs."""
        state = self.load_state()
        for record in self.extract(since=state.get("last_id")):
            event = self.to_event(record)
            producer.produce(topic, key=event["event_id"],
                             value=json.dumps(event))
        producer.flush()
        self.save_state(state)
```

Un seul `run()`, des connecteurs différents selon la source. Cette séparation permet de tester `extract()` et `to_event()` indépendamment.

### Interactif : repérez les design smells 🔍

Ce script « tout-en-un » contient plusieurs défauts de conception. Par binômes : listez les problèmes, puis proposez comment les corriger en classes et fonctions séparées.

```python
import requests

API_KEY = "sk-1234abcd"  # ← dans le code !
URL = "https://api.boutique.com/orders"

def run():
    page = 0
    while True:
        r = requests.get(f"{URL}?page={page}&key={API_KEY}")
        data = r.json()
        for order in data["results"]:
            # 50 lignes de traitement mélangées à l'extraction
            db.insert(order)
        if not data["next"]:
            break
        page += 1
```

Défauts attendus : secret dans le code, URL en dur, fonction monolithique, aucune gestion d'erreur HTTP, pas de timeout, état non persisté, couplage fort entre extraction et chargement.

### Authentification : API Key et OAuth2

**🔑 API Key**

```python
import os, requests

headers = {
    "Authorization": f"Bearer {os.environ['API_TOKEN']}"
}
r = requests.get(
    "https://api.example.com/orders",
    headers=headers,
    params={"limit": 100},
    timeout=10         # ← TOUJOURS définir
)
r.raise_for_status()   # Lève une exception sur 4xx/5xx
```

**🔐 OAuth2 client credentials.** Pour les connecteurs sans utilisateur humain :

1. POST sur le serveur d'autorisation avec `client_id` + `client_secret`
2. Récupérer le `access_token` et son `expires_in`
3. Utiliser le token dans les appels suivants
4. Renouveler avant expiration

⚠️ Stocker les secrets en variables d'environnement ou dans un vault — jamais dans le code ou le dépôt Git.

### Stratégies de pagination

| Type | Principe | Avantage | Limite |
|---|---|---|---|
| Offset/Limit | `?offset=0&limit=100` | Simple | Sauts si insertion pendant l'extraction |
| Numéro de page | `?page=2` | Intuitif | Mêmes problèmes que offset |
| Curseur | Token opaque fourni par l'API | Stable face aux insertions | Ne permet pas de sauter |
| En-tête Link | URL suivante dans `Link: <url>; rel="next"` | Standard HTTP | Parsing de l'en-tête |

La pagination par offset sur des données qui bougent saute ou duplique des lignes. Préférez un curseur ou un paramètre `updated_since` pour l'extraction incrémentale.

### Rate limiting et backoff exponentiel

**⚠️ Signaux d'une limite atteinte**
- Code HTTP `429 Too Many Requests`
- En-tête `Retry-After: 30` (attendre N secondes)
- En-têtes `X-RateLimit-Remaining` et `X-RateLimit-Reset`

**🔄 Stratégie recommandée**

```python
import time, random

def call_with_backoff(url, max_retries=5):
    for attempt in range(max_retries):
        r = requests.get(url, timeout=10)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 2 ** attempt))
            time.sleep(wait + random.uniform(0, 1))  # jitter
            continue
        r.raise_for_status()
        return r
```

Exemple concret : une API autorise 5 000 requêtes par heure → maximum 1,38 requête par seconde. Implémenter un `time.sleep(0.73)` entre chaque appel est plus simple et plus poli qu'attendre un 429.

### Extraction incrémentale et gestion d'état

*(ordre rétabli)*

- **Run 1 :** extraire depuis l'epoch, watermark → 2027-01-15
- **Run 2 :** extraire depuis 2027-01-15, watermark → 2027-01-17
- **Run 3 :** extraire depuis 2027-01-17, seuls nouveaux enregistrements

**💾 Où stocker l'état**
- Fichier JSON local (simple, fragile)
- Table de métadonnées en base
- Variable Airflow XCom (séance 5)

**⚠️ Règle fondamentale.** N'avancer l'état (watermark) qu'après la publication réussie dans Kafka. Si le connecteur plante entre l'extraction et la publication, il faut pouvoir réextraire sans perte.

### Exemple de code : retry HTTP avec requests

```python
from requests import Session
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

retry = Retry(
    total=5,
    backoff_factor=1,          # délai : 1s, 2s, 4s, 8s, 16s
    status_forcelist=[429, 500, 502, 503, 504],
    allowed_methods=["GET"]    # ne pas réessayer POST par défaut
)

session = Session()
session.mount("https://", HTTPAdapter(max_retries=retry))

# Maintenant tous les appels via session bénéficient des retries
r = session.get("https://api.example.com/orders", timeout=10)
r.raise_for_status()
```

**Règle pour les codes HTTP :** 4xx (sauf 408 et 429) → erreur client, ne pas réessayer. 5xx → erreur serveur transitoire, réessayer avec backoff. Timeouts → réessayer, puis alerter.

### Livrable séance 3 : votre connecteur principal

1. **Implémenter BaseConnector.** Dans `connectors/votre_source.py`, hériter de `BaseConnector` et implémenter `extract()` et `to_event()`.
2. **Authentification et pagination.** Gestion propre des credentials (variables d'env), boucle de pagination robuste face aux insertions concurrentes.
3. **Documentation courte.** Un README de 10 lignes : source, endpoint, format, comment lancer, variables d'environnement requises.

TP suggéré : Open-Meteo (sans clé) pour démarrer, puis API GitHub pour travailler authentification, pagination et rate limiting simultanément.

---

## Séance 4 · Connecteurs : bases relationnelles & stockage cloud

Extraire par lots depuis une base de données avec SQLAlchemy. Suivre l'avancement avec un watermark. Lire et écrire dans un stockage objet compatible S3 avec MinIO.

- **🗄️ SQLAlchemy :** lecture par lots, watermark, portabilité entre SGBD
- **🌊 Watermark :** extraction incrémentale fiable, pièges et solutions
- **🔄 CDC :** Change Data Capture avec Debezium, captures des suppressions
- **☁️ Stockage objet :** MinIO/S3 avec boto3, formats CSV/JSON Lines/Parquet

### Exemple de code : lecture par lots avec SQLAlchemy

*(indentation rétablie)*

```python
from sqlalchemy import create_engine, text

engine = create_engine(
    "postgresql+psycopg2://user:pwd@localhost/boutique"
)

WATERMARK_FILE = "state/commandes_watermark.txt"
BATCH_SIZE = 1000

def load_watermark():
    try:
        return open(WATERMARK_FILE).read().strip()
    except FileNotFoundError:
        return "1970-01-01T00:00:00Z"

watermark = load_watermark()

with engine.connect() as conn:
    rows = conn.execute(
        text("""
            SELECT * FROM commandes
            WHERE updated_at > :wm
            ORDER BY updated_at
            LIMIT :batch
        """),
        {"wm": watermark, "batch": BATCH_SIZE}
    )
    for row in rows.mappings():
        event = to_event(dict(row))
        producer.produce("commandes", value=json.dumps(event))

producer.flush()
# Sauvegarder le watermark APRÈS flush réussi
save_watermark(last_updated_at)
```

### Pièges du watermark

- **⏱️ Horodatages identiques.** Plusieurs lignes avec le même `updated_at` à la milliseconde. Solution : tri composé `ORDER BY updated_at, id` et clé de reprise `(updated_at, id)` pour ne pas rater la dernière milliseconde.
- **🐢 Transactions longues.** Une transaction ouverte peut commiter une ligne avec un `updated_at` antérieur au watermark déjà avancé. Solution : relire une marge de sécurité (ex. -5 min) et dédoublonner par `event_id`.
- **🗑️ Suppressions invisibles.** Un watermark sur `updated_at` ne capte pas les lignes supprimées. Solution : soft delete (colonne `deleted_at`) ou CDC par journal de transactions.

### Polling vs CDC

- **🔄 Polling.** On interroge la table périodiquement avec un watermark. Simple, ne nécessite pas d'accès au journal de la base. Latence = intervalle de polling. Ne capte pas les suppressions.
- **⚡ CDC via journal (Debezium).** On lit le journal de transactions de la base (WAL pour PostgreSQL, binlog pour MySQL). Capte INSERT, UPDATE et DELETE. Faible latence. Faible charge sur la base. Nécessite des droits d'accès au journal.

### CDC avec Debezium

*(ordre rétabli)*

```
PostgreSQL WAL         →  Debezium (Kafka Connect)  →  Kafka Topic                →  Structure d'événement
Journal des               Capture et transforme        Distribution d'événements      opération, before, after,
transactions                                                                          timestamp
```

### Stockage objet : concepts et formats

**🪣 Modèle objet (S3/MinIO)**
- **Bucket :** conteneur racine
- **Clé :** chemin complet de l'objet (`commandes/2027-01-18.jsonl`)
- **Objet :** le contenu (fichier, blob)

Pas de vrais dossiers, seulement des préfixes. Organisation recommandée : `source/table/date=YYYY-MM-DD/fichier.parquet`

**📂 Formats de fichiers**

| Format | Usage |
|---|---|
| CSV | Export tableur, échange simple |
| JSON Lines | Logs, streaming, facile à compléter |
| Parquet | Analytique, compression, lecture en colonnes |

### Exemple de code : lecture et écriture avec boto3 / MinIO

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://localhost:9000",   # MinIO local
    aws_access_key_id="minioadmin",
    aws_secret_access_key="minioadmin",
)

# Écrire un fichier JSON Lines
with open("commandes.jsonl", "rb") as f:
    s3.put_object(
        Bucket="raw",
        Key="commandes/date=2027-01-18/commandes.jsonl",
        Body=f
    )

# Lire le fichier
obj = s3.get_object(
    Bucket="raw",
    Key="commandes/date=2027-01-18/commandes.jsonl"
)
content = obj["Body"].read().decode("utf-8")
```

Même code pour MinIO et AWS S3 : seul `endpoint_url` change. Cette portabilité permet de développer localement et déployer sur le cloud sans modifier le connecteur.

### Livrable séance 4 : deuxième connecteur

Branchez votre deuxième source sur la même interface `BaseConnector`. Chaque groupe doit avoir au moins un connecteur de chaque type :

- **API REST (séance 3)** : avec authentification, pagination, extraction incrémentale et état persisté
- **Base de données ou stockage objet (séance 4)** : avec watermark robuste ou lecture de fichiers depuis MinIO, publiant sur le même topic Kafka

Les deux connecteurs doivent utiliser la même enveloppe d'événement standard définie en séance 2. Testez en insérant de nouvelles lignes : seules celles-ci doivent être extraites.

---

## Séance 5 · Orchestration avec Airflow

Construire des DAGs complexes avec dépendances et branchements. Configurer retries et timeouts pour absorber les pannes. Rendre le pipeline idempotent et reprenable sans dupliquer de données.

- **🗓️ DAGs :** scheduling, logical dates, dépendances fan-out/fan-in
- **🔀 Branchements :** `@task.branch`, trigger rules et états des tâches
- **🔁 Retries :** backoff exponentiel, timeouts, erreurs transitoires vs déterministes
- **♻️ Idempotence :** backfill, Clear, reprises sans doublons

### Concepts fondamentaux d'Airflow

*(Slide sans texte récupérable : seulement un schéma.)*

### Exemple de code : un DAG d'ingestion minimal

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(
    schedule="@hourly",
    start_date=datetime(2027, 1, 1),
    catchup=False,         # Ne pas rejouer le passé au démarrage
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=2),
    }
)
def ingestion():

    @task
    def extract():
        """Lancer le connecteur et retourner le nombre de messages publiés."""
        connector = MonConnecteur()
        return connector.run(producer, "commandes")

    @task
    def verify_volume(count: int):
        """Alerter si aucune donnée extraite."""
        if count == 0:
            raise ValueError("Aucune donnée extraite — source vide ?")

    verify_volume(extract())

ingestion()
```

**Piège important :** un DAG planifié à `@hourly` avec `start_date=2027-01-01` s'exécute à la **fin** de chaque heure, pas au début. La `logical_date` est le début de l'intervalle traité.

### Dépendances et fan-out / fan-in

**⚙️ Syntaxe des dépendances**

```python
check >> [extract_api,
          extract_db,
          extract_s3]  # fan-out
        >> validate    # fan-in
        >> load
        >> notify
```

**📐 Cas d'usage.** Fan-out : lancer tous les connecteurs en parallèle pour gagner du temps. Fan-in : attendre que tous soient terminés avant de charger. Branching : sauter une étape si aucun fichier nouveau.

```
Vérifier  →  Extraire  →  Valider  →  Charger → Notifier
```

### Trigger rules : gérer les états

| Trigger rule | Condition de déclenchement | Cas d'usage |
|---|---|---|
| `all_success` | Toutes les tâches parentes ont réussi (défaut) | Pipeline standard |
| `one_failed` | Au moins une parente a échoué | Notification d'erreur |
| `all_done` | Toutes terminées (succès ou échec) | Nettoyage final |
| `none_failed_min_one_success` | Aucun échec ET au moins un succès | Après branchement |

La règle `all_success` (défaut) après un branchement fait échouer la tâche de jointure car les branches ignorées sont en état `skipped`. Utiliser `none_failed_min_one_success` après un `@task.branch`.

### Exemple de code : configuration des retries

```python
from datetime import timedelta

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,   # 2min, 4min, 8min...
    "max_retry_delay": timedelta(minutes=20),
    "execution_timeout": timedelta(minutes=30),  # Tuer si bloqué
}

@dag(default_args=default_args, ...)
def ingestion():
    @task(retries=5)  # Override pour une tâche critique
    def call_unstable_api():
        ...
```

**✅ Réessayer si…**
- Erreur réseau transitoire
- Réponse 5xx du serveur
- Timeout de connexion

**❌ Ne pas réessayer si…**
- Mauvaise configuration (401, 403)
- Données invalides (400)
- Bug déterministe dans le code

### Idempotence : relancer sans dupliquer

**🎯 Définition.** Une tâche idempotente produit exactement le même résultat, qu'elle soit exécutée une fois ou dix fois sur la même période. C'est ce qui rend le Clear et le backfill sûrs.

**🛠️ Techniques**
- Écraser la partition du jour : supprimer et réécrire `date=2027-01-18/`
- UPSERT sur clé : `INSERT ... ON CONFLICT (event_id) DO UPDATE`
- Identifiants déterministes : `event_id = sha256(source + id + date)`

**Anti-exemple :** un `INSERT` simple sans gestion de doublon multiplie les enregistrements à chaque reprise. Une tâche non idempotente rend le backfill dangereux.

### Interactif, panne provoquée : Break It on Purpose 💥

Chaque groupe sabote le DAG d'un autre groupe pendant qu'il tourne. Le groupe visé doit diagnostiquer et corriger depuis l'interface Airflow.

- **🔌 Couper l'API :** modifier la variable d'environnement pour pointer vers un faux endpoint
- **💣 Injecter une valeur invalide :** envoyer un événement avec un montant négatif ou un champ manquant
- **⏱️ Provoquer un timeout :** ralentir artificiellement une réponse pour dépasser l'`execution_timeout`

**Diagnostic attendu :** identifier la tâche en échec dans le graphe, lire les logs, comprendre si c'est transitoire ou déterministe, et relancer sans doublons.

### Livrable séance 5 : DAG unifié

1. **Un DAG unique.** Orchestre les deux connecteurs en parallèle (fan-out) puis le chargement en stockage (fan-in). Planning horaire ou quotidien selon la thématique.
2. **Retries et timeouts configurés.** Chaque tâche a ses propres paramètres de retry adaptés au type d'erreur attendu. L'`execution_timeout` protège contre les tâches bloquées.
3. **Idempotence démontrée.** Un Clear + relance du DAG ne crée pas de doublons. Cette propriété est testée et documentée dans le README.

---

## Séance 6 · Monitoring, alerting & logs centralisés

Mesurer la santé réelle d'un pipeline — pas seulement si les DAGs sont verts. Alerter au bon moment, sur les bons signaux. Centraliser des logs exploitables pour diagnostiquer rapidement.

- **📊 Métriques :** latence, volume, fraîcheur, taux d'erreur, consumer lag
- **🔔 Alertes :** callbacks Airflow, email, Slack — utiles vs bruit
- **📝 Logs structurés :** JSON, niveaux, champs constants, corrélation
- **🏥 Incidents :** drill de panne : détection, diagnostic, remédiation

### Interactif, accroche : le dashboard est vide 😱

*Tous les DAGs Airflow affichent un statut vert. Pourtant, le tableau de bord métier est vide depuis hier matin. Pourquoi ?*

- 💭 **Hypothèse 1 :** la source est vide ce jour-là (jour férié ? panne upstream non signalée)
- 💭 **Hypothèse 2 :** les données sont arrivées en retard et la requête filtre sur une mauvaise date
- 💭 **Hypothèse 3 :** le consumer Kafka s'est arrêté sans faire échouer une tâche Airflow
- 💭 **Hypothèse 4 :** un filtre ou une validation trop stricte a rejeté 100 % des messages

**Morale :** un DAG vert ne signifie pas que les données sont arrivées, sont fraîches et sont correctes. Il faut surveiller les métriques métier, pas seulement l'exécution des tâches.

### Les métriques de santé d'un pipeline

| Métrique | Définition | Valeur normale | Signal d'alerte |
|---|---|---|---|
| Latence | Temps entre création et disponibilité | < 15 min | > 30 min |
| Volume | Nombre de messages ingérés / heure | 1 000 ± 20 % | 0 un mardi à 11h |
| Fraîcheur | Âge du message le plus récent | < 15 min | > 2 h |
| Taux d'erreur | % de messages rejetés | < 1 % | > 5 % |
| Consumer lag | Messages produits - messages consommés | Stable ou < 1 000 | Croissant |

### Alertes Airflow : callbacks

**⚙️ Configuration**

```python
def notify_slack(context):
    ti = context["task_instance"]
    msg = (f":x: *{ti.dag_id}.{ti.task_id}* "
           f"a échoué\nRun: `{context['ds']}`\n"
           f"Logs: {ti.log_url}")
    requests.post(
        os.environ["SLACK_WEBHOOK_URL"],
        json={"text": msg}
    )

default_args = {
    "on_failure_callback": notify_slack,
    "on_retry_callback": notify_slack,
    "sla_miss_callback": notify_slack,
}
```

**✅ Une bonne alerte est…**
- **Utile :** quelqu'un doit agir
- **Urgente :** maintenant, pas demain
- **Actionnable :** on sait quoi faire

**❌ Éviter la fatigue d'alerte.** Trop de notifications → elles sont ignorées. Chaque alerte doit pointer vers un **runbook** : que faire concrètement face à ce problème précis ?

### Logs structurés en JSON

```python
import logging
import json

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "ts": self.formatTime(record),
            "level": record.levelname,
            "dag_id": getattr(record, "dag_id", None),
            "task_id": getattr(record, "task_id", None),
            "event": record.getMessage(),
            "extra": getattr(record, "extra", {}),
        })
```

Exemple de log d'erreur exploitable :

```json
{"ts":"2027-01-18T10:15:02Z","level":"ERROR","dag_id":"ingestion",
 "task_id":"extract_api","event":"http_error","status":503,"attempt":2}
```

**✅ Toujours inclure**
- Timestamp ISO 8601 UTC
- Champ `event` clair et actionnable
- Identifiants de corrélation

**❌ Ne jamais logger**
- Mots de passe, tokens, clés API
- Données personnelles (RGPD)
- Messages trop verbeux sans valeur

### Interactif, diagnostic : Debug from the Logs 🔍

Un extrait de 30 lignes de logs JSON d'un vrai incident est distribué à chaque binôme. Mission : retrouver en 10 minutes la cause racine, l'heure de début de l'anomalie et l'impact estimé.

1. **Identifier la première erreur.** Chercher le premier log de niveau ERROR ou WARNING. L'heure de ce log est le début de l'incident.
2. **Corréler par `dag_id` / `task_id`.** Filtrer tous les logs de la même tâche pour reconstituer la chronologie complète.
3. **Estimer l'impact.** Combien de messages n'ont pas été traités ? Depuis quand les données sont-elles stales ?

### Livrable séance 6 : monitoring et alerting

1. **Tableau de métriques.** Latence, volume horaire, fraîcheur et taux d'erreur calculés et affichés. Une vue SQL ou un dashboard simple suffit.
2. **Alertes configurées.** Au moins : alerte sur échec de tâche, alerte sur absence de données depuis N heures. Testées lors du drill de panne.
3. **Logs centralisés.** Tous les connecteurs émettent des logs JSON. Collectés vers un point unique interrogeable. Documenté dans le README.

**Drill final :** le formateur provoque une panne en direct. Chronomètre ouvert : quel groupe détecte l'incident en premier grâce à ses alertes ?

---

## Séance 7 · Transformation précoce & qualité upstream

Valider dès l'ingestion plutôt que de découvrir les problèmes en production. Normaliser et dédoublonner. Isoler les messages invalides pour ne jamais bloquer le pipeline principal.

- **✅ Validation :** structure, types, règles métier, cohérence entre champs
- **🔧 Normalisation :** dates ISO 8601, casse, unités, valeurs nulles
- **🔁 Déduplication :** par `event_id`, clé métier, fenêtre temporelle
- **☠️ DLQ & quarantaine :** topic dédié, zone de stockage, rejeu après correction

### Le coût de la qualité tardive

| Heure | Étape | Ce qui se passe |
|---|---|---|
| 9h00 | Entrée | Montant = -50 € ingéré |
| 11h00 | Dash | Tableau de bord corrompu affiché |
| 15h00 | Détection | Le directeur remarque l'erreur |
| 16h00 | Enquête | Investigation commence, 7 heures perdues |

Plus on détecte tard, plus la correction est coûteuse. Le principe de **« shift left »** : valider au plus près de la source, dès l'ingestion.

Une donnée invalide qui entre à 9h00, fausse un tableau de bord à 11h00 et est découverte par un directeur à 15h00 représente 6 heures de travail perdu — sans compter les décisions prises sur de mauvaises bases.

### Où placer la validation ?

La recommandation du module : un **consumer de validation dédié** lit le topic d'ingestion brut, valide, normalise et publie soit vers le stockage validé, soit vers la Dead-Letter Queue. Ce composant est testable indépendamment.

### Niveaux de validation

1. **Niveau 1 · Structure :** champs requis présents, JSON parseable, types corrects
2. **Niveau 2 · Types et formats :** date ISO 8601, email valide, UUID bien formé, entier vs décimal
3. **Niveau 3 · Règles métier :** montant > 0, quantité entière positive, pays dans la liste autorisée
4. **Niveau 4 · Cohérence :** date de livraison > date de commande, `id_client` existe dans le référentiel

### Exemple de code : JSON Schema, validation message par message

```python
import jsonschema

schema = {
    "type": "object",
    "required": ["event_id", "id_client", "montant", "event_time"],
    "properties": {
        "event_id":   {"type": "string", "minLength": 1},
        "id_client":  {"type": "integer", "minimum": 1},
        "montant":    {"type": "number",  "minimum": 0},
        "event_time": {"type": "string",  "format": "date-time"},
    },
    "additionalProperties": True  # Accepter les champs futurs
}

def validate_message(event: dict) -> list[str]:
    errors = []
    validator = jsonschema.Draft7Validator(schema)
    for error in validator.iter_errors(event):
        errors.append(f"{'.'.join(str(p) for p in error.path)}: {error.message}")
    return errors
```

Validation message par message, indépendante du langage. Idéal dans un consumer Kafka qui traite un événement à la fois. Les erreurs sont collectées toutes ensemble pour enrichir le message DLQ.

### Exemple de code : pandera, validation par lots (DataFrame)

```python
import pandera as pa
import pandas as pd

schema = pa.DataFrameSchema({
    "id_client": pa.Column(int, nullable=False),
    "montant":   pa.Column(float, pa.Check.ge(0),
                           error="montant doit être positif"),
    "email":     pa.Column(str,
                           pa.Check.str_contains("@"),
                           nullable=True),
    "pays":      pa.Column(str,
                           pa.Check.isin(["FR", "BE", "CH"]),
                           nullable=False),
}, coerce=True)

try:
    validated_df = schema.validate(df, lazy=True)
except pa.errors.SchemaErrors as e:
    print(e.failure_cases)  # DataFrame de toutes les erreurs
```

`lazy=True` est essentiel : sans lui, pandera s'arrête à la première erreur. Avec lazy, on récupère toutes les violations d'un lot, ce qui permet d'enrichir les messages DLQ avec tous les motifs de rejet.

### Normalisation : avant et après

| Champ | Brut (source) | Normalisé | Règle |
|---|---|---|---|
| Date | `18/01/2027 10:15` | `2027-01-18T10:15:00Z` | ISO 8601 UTC |
| Ville | `paris` / `PARIS` | `Paris` | Title case |
| Montant | `"59,90 €"` (string) | `59.90` (float) | Type + format |
| Valeur vide | `""` / `"N/A"` / `"null"` | `null` (JSON) | Canonique NULL |

La normalisation doit être **idempotente** : appliquer la règle deux fois doit donner le même résultat. Cela simplifie le rejeu des messages depuis la quarantaine.

### Déduplication

**🔑 Stratégies**
- **Par `event_id` :** identifiant unique dans l'enveloppe. La méthode la plus fiable.
- **Par clé métier :** `(id_commande, event_type)`. Attention aux mises à jour légitimes.
- **Par fenêtre temporelle :** ignorer les doublons arrivant dans les 5 dernières minutes.

**💾 Où stocker l'état de déduplication**
- En mémoire : rapide mais perdu au redémarrage
- Redis / Memcached : partageable, TTL configurable
- UPSERT côté stockage : le plus robuste

**⚠️ Doublon ou mise à jour ?** Deux événements avec le même `id_commande` mais des montants différents = mise à jour légitime. La clé de déduplication doit inclure l'`event_type`.

### Dead-Letter Queue et zone de quarantaine

**📬 Dead-Letter Queue (DLQ).** Un topic Kafka dédié — ex. `commandes.dlq` — reçoit les messages rejetés enrichis avec le motif de rejet, la date et la version du validateur.

```json
{
  "original": {"id_client": "abc",
               "montant": -5},
  "error": "montant < 0; id_client non entier",
  "failed_at": "2027-01-18T10:15:02Z",
  "consumer": "validator-v1"
}
```

**🗃️ Zone de quarantaine.** Stockage objet dédié pour analyse et rejeu durable :

```
quarantine/
  boutique/
    date=2027-01-18/
      batch_001.jsonl  ← messages invalides
```

DLQ = flux temps réel. Quarantaine = stockage durable pour analyse a posteriori et rejeu après correction de la règle ou de la source.

### Rejeu depuis la quarantaine

*(ordre rétabli)*

```
Quarantaine  →  Correction  →  Republication  →  Validation & Stockage
```

**🔄 Processus de rejeu**
1. Analyser les messages en quarantaine
2. Corriger la règle de validation ou demander une correction à la source
3. Republier sur le topic d'origine
4. L'idempotence absorbe les éventuels doublons

**📊 Mesurer le taux de rejet.** Surveiller en continu : messages valides / (valides + invalides). Un taux > 5 % déclenche une alerte — cela signifie souvent un changement de schéma côté source non communiqué.

### Interactif : injectez des données piégées 💣

Chaque groupe fabrique 10 messages Kafka malformés pour le groupe voisin. Le groupe cible doit tous les intercepter avec son consumer de validation.

**🏆 Score :** +1 point par message piégé détecté. -1 par faux positif (message valide rejeté).

**💡 Idées de pièges**
- Montant négatif ou texte
- Date impossible (32 janvier)
- Champ requis manquant
- JSON invalide (syntaxe cassée)
- Doublon avec `event_id` identique
- Nouvelle version de schéma inconnue

**🔍 Débriefing :** quels messages ont échappé ? Pourquoi ? Comment renforcer les règles ?

### Livrable séance 7 : transformation précoce & quarantaine

1. **Consumer de validation intégré au DAG.** Lit le topic d'ingestion brut, applique validation (JSON Schema ou pandera), normalisation et déduplication. Intégré comme tâche dans le DAG Airflow unifié de la séance 5.
2. **DLQ et zone de quarantaine.** Les messages invalides sont envoyés sur `votre_topic.dlq` ET écrits en quarantaine dans MinIO avec le motif d'erreur enrichi.
3. **Taux de rejet monitoré.** Métrique ajoutée au tableau de monitoring de la séance 6. Alerte si > 5 %. Documenté dans le README avec exemple de rejeu.

---

## Erreurs fréquentes à éviter

### Séances 1-2

- **Oubli de `flush()`.** Un producer Python qui se termine sans `flush()` perd silencieusement les messages en buffer. Toujours appeler `flush()` ou utiliser un context manager.
- **Commit avant traitement.** Avec `enable.auto.commit=True`, Kafka commite avant que votre code ait traité le message. En cas de crash, le message est perdu définitivement.
- **Plus de consumers que de partitions.** Les consumers en excès restent inactifs. Dimensionner les partitions lors de la création du topic car c'est difficile à modifier ensuite.
- **Clé mal choisie.** Une clé à faible cardinalité (ex. : pays avec 3 valeurs) crée des hot partitions. L'une est surchargée pendant que les autres dorment.

### Séances 3-4

- **Clé API dans le dépôt Git.** Dès le premier commit, considérer la clé comme compromise. Toujours utiliser des variables d'environnement ou un fichier `.env` listé dans `.gitignore`.
- **Pas de timeout sur les appels HTTP.** Un appel sans timeout peut bloquer indéfiniment, gelant le connecteur et saturant les ressources. Toujours définir `timeout=10` (ou adapté à l'API).
- **Watermark avancé avant publication.** Si le connecteur plante entre l'avancement du watermark et le `flush()` Kafka, les données sont perdues définitivement. Toujours publier avant de sauvegarder l'état.
- **Pagination offset sur données mobiles.** Des insertions pendant l'extraction font sauter ou dupliquer des lignes. Préférer un curseur ou un paramètre `updated_since`.

### Séances 5-6

- **Trigger rule par défaut après branchement.** Avec `all_success`, une branche ignorée (`skipped`) fait échouer la tâche de jointure. Utiliser `none_failed_min_one_success` après un `@task.branch`.
- **Retries sur erreur déterministe.** Réessayer une tâche qui échoue à cause d'un bug ou d'une mauvaise configuration est inutile et masque le vrai problème. Différencier erreurs transitoires et déterministes.
- **Surveiller le succès sans surveiller la fraîcheur.** Un DAG vert ne garantit pas que les données sont récentes et complètes. Surveiller aussi le volume et l'âge des données les plus récentes.
- **Secrets dans les logs.** Un token loggué est un token compromis. Masquer systématiquement tous les secrets avant de logger, même en DEBUG.

### Séance 7

- **Suppression silencieuse des invalides.** Ignorer un message invalide sans trace est la pire option : on perd la donnée ET on ne sait pas qu'on l'a perdue. Toujours envoyer en DLQ avec le motif d'erreur détaillé.
- **DLQ sans motif d'erreur.** Un message DLQ sans contexte est inexploitable. Enrichir systématiquement : champ `error`, `failed_at`, version du validateur, valeur originale complète.
- **Validation trop stricte qui bloque tout.** Une règle qui rejette 80 % des messages n'est pas une protection, c'est une panne. Calibrer les seuils et prévoir une alerte si le taux de rejet dépasse 5 %.
- **Aucun rejeu prévu.** Une quarantaine sans procédure de rejeu est un cimetière de données. Documenter dès le départ comment corriger et rejouer les messages mis en quarantaine.

---

## Récapitulatif du module

### Ce que vous avez construit

En 7 séances, chaque groupe a assemblé un pipeline d'ingestion complet, industriel et documenté. Voici la vue d'ensemble de ce qui a été produit.

### Les concepts fondamentaux du module

- **Architecture :** les 3 V, batch vs streaming, pull/push/événementiel. Kafka comme hub central pour découpler N sources de M destinations.
- **Kafka en Python :** producer (sérialisation, clé, acks, flush), consumer (groupes, offsets, commits manuels), garanties at-least-once + idempotence.
- **Connecteurs :** interface réutilisable `BaseConnector`, auth OAuth2/API Key, pagination par curseur, extraction incrémentale, retries HTTP.
- **Orchestration :** DAGs Airflow avec dépendances, branchements, trigger rules, retries exponentiels, idempotence et backfill.
- **Monitoring :** métriques de santé (latence, volume, fraîcheur, lag), alertes actionnables, logs JSON structurés et centralisés.
- **Qualité upstream :** validation précoce, normalisation, déduplication, DLQ enrichie, quarantaine et rejeu après correction.

### Check-list projet final (à vérifier avant la soutenance)

**💻 Code**
- [ ] `BaseConnector` implémenté et documenté
- [ ] Au moins 2 connecteurs (REST + base ou S3)
- [ ] Secrets dans variables d'environnement uniquement
- [ ] Tests unitaires sur `extract()` et `to_event()`
- [ ] Extraction incrémentale avec état persisté

**🗓️ DAG & pipeline**
- [ ] DAG Airflow unifié avec fan-out / fan-in
- [ ] Retries et timeouts configurés par tâche
- [ ] Idempotence démontrée (Clear sans doublon)
- [ ] Consumer de validation avec DLQ
- [ ] Zone de quarantaine dans MinIO

**📊 Monitoring**
- [ ] Tableau de métriques (latence, volume, fraîcheur)
- [ ] Alertes sur échec et sur absence de données
- [ ] Logs JSON structurés et centralisés

**📄 Documentation**
- [ ] README : problématique, sources, volumes estimés
- [ ] Schéma d'architecture (sources → stockage)
- [ ] Procédure de rejeu depuis la quarantaine

### Format de la soutenance

1. **5 min · Démo live.** Lancer le pipeline complet devant le jury. Un connecteur extrait, Kafka reçoit, le consumer valide et publie en stockage.
2. **5 min · Architecture.** Présenter le schéma du pipeline, justifier 3 choix de conception (pattern, format, nombre de partitions…).
3. **5 min · Résilience.** Montrer un retry en action, un message rejeté en DLQ, une alerte déclenchée. Prouver l'idempotence avec un Clear.
4. **5 min · Questions.** Le jury pose des questions sur les choix techniques et les compromis. Chaque membre du groupe doit pouvoir répondre.

La démo live compte pour **50 % de la note projet**. Préparez un environnement stable et un plan B (captures d'écran) en cas de problème technique le jour J.

### Critères d'évaluation du projet

| Critère | Poids | Ce qu'on évalue |
|---|---|---|
| Fonctionnement du pipeline complet | 30 % | Les données circulent de bout en bout sans erreur |
| Qualité du code et des connecteurs | 20 % | Interface réutilisable, gestion d'erreurs, pas de secrets dans le code |
| Résilience et idempotence | 20 % | Retries configurés, Clear sans doublons, DLQ opérationnelle |
| Monitoring et alertes | 15 % | Métriques de santé, alertes actionnables, logs structurés |
| Documentation et justifications | 15 % | README clair, schéma, choix justifiés, procédure de rejeu |

### Préparation au QCM

**📝 Thèmes garantis**
- Définitions Kafka (broker, topic, partition, offset, consumer group)
- Différences batch / streaming / micro-batch
- Comportement des acks (0, 1, all)
- Commit avant vs après traitement
- Trigger rules Airflow (`all_success` vs `none_failed_min_one_success`)
- Codes HTTP et stratégies de retry
- Rôle de la DLQ vs zone de quarantaine

**💡 Conseils de révision**
- Retravailler les slides « Common Mistakes » de chaque séance
- Refaire les quiz interactifs des séances en solo
- Expliquer à voix haute les schémas d'architecture vus en cours
- Tester votre compréhension : que se passe-t-il si `flush()` est oublié ? si le watermark est avancé trop tôt ?

---

## Référence rapide

### Glossaire du module

| Terme | Définition |
|---|---|
| Broker Kafka | Serveur qui stocke et distribue les messages d'un cluster Kafka |
| Offset | Position d'un message dans une partition ; chaque consumer maintient le sien indépendamment |
| Consumer lag | Écart entre le dernier offset produit et le dernier offset commité par un consumer |
| Watermark | Point de reprise persisté pour l'extraction incrémentale (date, identifiant…) |
| Idempotence | Propriété d'une opération qui donne le même résultat qu'elle soit exécutée une ou plusieurs fois |
| DLQ | Dead-Letter Queue : topic ou file recevant les messages rejetés, enrichis du motif d'erreur |
| CDC | Change Data Capture : capture des modifications d'une base via son journal de transactions |
| Backfill | Rejouer un DAG Airflow sur une période passée pour rattraper des données manquantes |
| Shift left | Principe de détecter et corriger les problèmes le plus tôt possible dans le pipeline |

### Outils du module

- **Apache Kafka :** bus d'événements distribué. Mode KRaft (sans ZooKeeper). Client Python : `confluent-kafka`.
- **Apache Airflow :** orchestrateur de workflows. API Taskflow (`@dag`, `@task`). Interface web sur `localhost:8080`.
- **MinIO :** stockage objet compatible S3 en local. Console sur `localhost:9001`. Client Python : `boto3`.
- **SQLAlchemy :** abstraction base de données. Portabilité PostgreSQL / MySQL / SQLite. Lecture par lots et watermark.
- **jsonschema & pandera :** validation des données. jsonschema pour les messages individuels, pandera pour les DataFrames.
- **Docker Compose :** environnement local complet : Kafka, Airflow, MinIO, PostgreSQL en un seul `docker compose up -d`.

---

## Pour aller plus loin

### Ressources recommandées

**📚 Documentation officielle**
- **Kafka :** kafka.apache.org — architecture, configuration, garanties
- **Airflow :** airflow.apache.org — concepts, opérateurs, best practices
- **Confluent :** developer.confluent.io — tutoriels Python, Schema Registry
- **pandera :** pandera.readthedocs.io — validation de DataFrames

**📖 Lectures complémentaires**
- « Designing Data-Intensive Applications » — Martin Kleppmann. Le livre de référence sur les systèmes distribués et les pipelines de données.
- « Fundamentals of Data Engineering » — Reis & Housley. Vue d'ensemble moderne de l'ingénierie des données.
- Blog Confluent : articles approfondis sur Kafka en production
- Blog Astronomer : best practices Airflow en production

### Architecture Lambda vs Kappa

- **Λ Architecture Lambda.** Deux couches parallèles : une batch layer (Spark, Hadoop) pour les calculs historiques exacts, et une speed layer (Kafka Streams, Flink) pour les résultats approximatifs temps réel. Les deux alimentent une serving layer. Complexe à maintenir : deux codebases pour la même logique.
- **κ Architecture Kappa.** Une seule couche streaming (Kafka + Flink ou Spark Structured Streaming). Le batch est remplacé par un rejeu depuis Kafka (rétention longue). Plus simple, une seule codebase. Nécessite un système de streaming mature. Tendance dominante dans les architectures modernes.

Ce module vous donne les bases pour comprendre et implémenter les deux. Les séances Kafka (1-2) et Airflow (5) couvrent respectivement les couches streaming et batch de Lambda.

### Vers le streaming avancé (perspectives après ce module)

- **Kafka Streams & ksqlDB :** transformations, agrégations et jointures directement sur les topics Kafka, sans consumer externe. La prochaine étape naturelle après ce module.
- **Apache Flink :** moteur de traitement stateful en streaming. Fenêtres temporelles, watermarks de progression du temps, exactly-once distribué. Standard de l'industrie pour le streaming complexe.
- **Schema Registry & Avro :** gestion centralisée des schémas d'événements. Validation à la publication, évolution de schéma compatible, économie de bande passante par rapport à JSON.

### Réflexion finale : les principes qui durent

Les outils changent — Kafka sera peut-être remplacé dans 10 ans. Mais ces principes restent valables quelle que soit la technologie.

- **🔒 Ne jamais supposer la fiabilité.** Tout composant peut tomber. Concevoir pour la panne, pas pour le cas nominal.
- **📐 Découpler les systèmes.** Un producteur ne doit pas savoir qui consomme. Un consumer ne doit pas bloquer la source.
- **🔁 Rendre tout idempotent.** Si une opération peut être rejouée sans dommage, tous les problèmes de fiabilité deviennent résolubles.
- **📊 Mesurer avant d'agir.** Un pipeline sans métriques est une boîte noire. On ne peut pas optimiser ce qu'on ne mesure pas.
- **🚨 Détecter tôt, corriger vite.** Le shift left s'applique à la qualité des données comme à la sécurité : plus tôt on détecte, moins ça coûte.
- **📝 Documenter les décisions.** Un choix d'architecture non documenté devient un mystère six mois plus tard. Écrire le « pourquoi » autant que le « comment ».

### Prochaines étapes pour votre projet

- **Cette semaine :** finaliser les deux connecteurs, tester le pipeline de bout en bout, compléter le README et les justifications d'architecture
- **Avant la soutenance :** vérifier la check-list projet, préparer la démo live, s'assurer que chaque membre peut répondre aux questions techniques
- **Soutenance :** démo 5 min + architecture 5 min + résilience 5 min + questions 5 min. Plan B : captures d'écran si problème technique
- **Après le module :** explorer Kafka Streams, Apache Flink, Schema Registry. Appliquer ces patterns sur un vrai projet de stage ou alternance

### Bon courage pour la suite ! 🚀

Vous avez parcouru l'intégralité de la chaîne d'ingestion : de la source brute au stockage validé, en passant par Kafka, les connecteurs, Airflow, le monitoring et la qualité des données.

> Un pipeline de données, c'est comme un pont : on ne le remarque que quand il s'effondre. Votre travail est de construire des ponts qu'on n'entend jamais parler.

- **📧 Questions après le cours :** utilisez le forum du module ou les heures de permanence. Les questions sur les bugs du TP sont les bienvenues.
- **🤝 Entre vous :** les meilleurs ingénieurs data apprennent des autres groupes. Regardez les architectures de vos camarades lors des soutenances.
- **🌍 En production :** ces patterns sont ceux utilisés chez Netflix, Uber, Airbnb. Vous avez maintenant le vocabulaire et les bases pour les lire et les appliquer.
