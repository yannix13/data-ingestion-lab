# Récap : préparation au module Collecte & Ingestion

*Conversation du 16 novembre 2026, avant la séance 1 (semaine du 23 novembre).*
*Support de cours de référence : `collecte_ingestion.md` (M1 Data & IA, 7 séances).*

---

## 1. Qui je suis et comment j'apprends

- **Profil** : dev fullstack en reconversion vers la data. Kafka, Airflow et Docker sont **nouveaux** pour moi.
- **Expérience data** : un pipeline Python en local, en débutant : API TMDB → nettoyage/transformation → fichiers Parquet → chargement dans Postgres. Je ne le maîtrise pas encore totalement.
- **Façon d'apprendre** :
  - J'ai besoin de **concret** : du code, des exemples réels, « ce qui casse exactement ».
  - **Pas d'analogies.** Je préfère les cas techniques réels.
  - J'apprends en pratique : voir les problèmes arriver (« faire péter » un système) m'aide plus que la théorie.
  - Je pose beaucoup de « pourquoi ? » : expliquer la raison d'être d'un outil avant son fonctionnement.
  - Des explications **un bloc à la fois**, avec une petite question de vérification.

---

## 2. Ce que j'ai compris

### Mon pipeline TMDB dans le vocabulaire du cours
C'est de l'**ingestion batch en mode pull** : je vais chercher les données (pull), par paquets, quand je lance le script (batch). Le cours ajoute deux briques :
- **Kafka au milieu** pour transporter les données entre ceux qui les produisent et ceux qui les utilisent.
- **Airflow autour** pour lancer les tâches selon un planning, les relancer en cas d'échec et garder l'historique (un cron avec dépendances, retries et interface).

### Pourquoi Kafka plutôt qu'une BDD que toutes les apps interrogent
- Une **BDD stocke un état** (la valeur actuelle de chaque ligne). **Kafka stocke des événements** (ce qui s'est passé, dans l'ordre).
- Avec une BDD partagée, chaque app fait du **polling** : un `SELECT ... WHERE updated_at > :watermark` régulier. Chacune doit retenir où elle en est (le **watermark**), et la base reçoit des requêtes en continu.
- **Suppressions invisibles** : une ligne supprimée ne revient plus dans le `SELECT`, l'app qui l'avait comptée ne le saura jamais. Dans Kafka, une suppression est un nouvel événement ajouté au flux.
- La raison n°1 n'est pas la charge, c'est le **découplage** : le producteur ne sait pas qui lit les données, ni quand. Une nouvelle app s'abonne sans rien modifier ailleurs.
- En pratique les deux cohabitent : Kafka transporte, un consommateur range ensuite dans une BDD ou en Parquet.

### Le cas microservices (HTTP direct)
Service Commandes qui appelle Stock, Facturation, Emails, Analytics en `requests.post` successifs :
1. **Panne** : Emails tombe → soit erreur 500 alors que la commande est déjà créée (le client reclique → doublons), soit l'email n'est jamais envoyé sans que personne ne le sache.
2. **Latence** : le service le plus lent fixe le temps de réponse au client.
3. **Couplage** : chaque nouveau service oblige à modifier et redéployer Commandes.
4. **Pics** (Black Friday) : la surcharge d'un service remonte jusqu'au paiement.

Avec Kafka, Commandes publie un seul message (`producer.produce("commandes", ...)`) et chaque service lit le topic à son rythme. Une panne d'Emails = du retard (le **lag**), rattrapé au redémarrage.
**Nuance** : quand il faut une réponse *avant* de répondre au client (« ce produit est-il en stock ? »), l'appel HTTP synchrone reste nécessaire.

J'ai fait le lien : l'essor des microservices a beaucoup contribué au développement des message brokers (architecture orientée événements).

### L'origine big data (LinkedIn, 2011)
- Des milliards d'événements par jour vers plusieurs destinations (Hadoop, recherche, recommandations, monitoring).
- Avant : un pipeline maison par couple source → destination, copies de logs la nuit, files de messages qui saturaient (messages en mémoire, supprimés une fois lus).
- Kafka écrit tout sur disque, en ajout à la fin d'un fichier, et garde plusieurs jours de données → le même flux sert un consommateur temps réel et un batch nocturne, chacun à son rythme. Pendant un pic, l'écart s'accumule sur disque et est rattrapé ensuite.

### Ma solution « colonne `email_envoye` » (pattern outbox)
Ma proposition : une colonne booléenne par traitement + un script qui rattrape les lignes à `false`. Ça existe en production et ça suffit pour une petite app. Limites vues :
1. **Une colonne par service** → migrations sur la table d'une autre équipe, couplage.
2. **Crash entre l'envoi et l'`UPDATE`** → email envoyé deux fois (ou jamais si on inverse l'ordre). C'est exactement le problème du **commit avant/après traitement** de la séance 2, qui existe aussi avec Kafka.
3. **Deux instances du script** → doublons, sauf `SELECT ... FOR UPDATE SKIP LOCKED`.
4. **Fréquence** : rare = lent, fréquent = charge sur la base.

Kafka remplace toutes ces colonnes par **un seul offset par consommateur**, stocké chez Kafka. Les **consumer groups** règlent le problème des instances multiples.

### Évolution de schéma (ajouter une colonne casse-t-il du code ?)
Oui, dans plusieurs cas :
- `INSERT` sans liste de colonnes : plante en MySQL, passe en Postgres (valeur par défaut).
- Colonne `NOT NULL` sans `DEFAULT` : les `INSERT` existants plantent, la migration échoue si la table a des lignes.
- Code qui dépend du nombre de colonnes (`id, client, montant = row`, `SELECT *` renvoyé tel quel au front).
- Migration sur grosse table : verrou, ordre de déploiement (migration avant le code).

Lien avec le cours : champ `version` dans l'enveloppe d'événement (séance 2), `additionalProperties: True` dans le JSON Schema (séance 7).

### Idempotence
Comme un `PUT` (deux appels = même résultat) par opposition à un `POST`. Comme on rejoue en cas de panne, rejouer ne doit pas créer de doublons. Technique principale : UPSERT sur `event_id` (`INSERT ... ON CONFLICT (event_id) DO UPDATE`).

### Les composants de Kafka (repris depuis le début)
- **Broker** : un programme Java qui écoute sur le port **9092**. Dans le TP, un conteneur Docker. Le code s'y connecte via `bootstrap.servers: localhost:9092`, comme on se connecte à Postgres sur 5432. En production, plusieurs brokers = un **cluster**, avec des copies des données.
- **Message** : une **clé** optionnelle (ex. id client), une **valeur** (souvent du JSON), un **timestamp**. Pour Kafka, ce ne sont que des octets : il ne lit ni ne valide le JSON.
- **Topic** : un nom pour une catégorie de messages (`commandes`). Sur disque : un dossier `commandes-0/` avec des fichiers `.log`. On ne peut qu'**ajouter à la fin**, ni modifier ni supprimer un message précis. Les messages sont effacés après la **rétention** (7 jours par défaut). Lire ne supprime pas.
- **Offset** : numéro croissant attribué à chaque message (0, 1, 2…), jamais réutilisé.
- **Offsets des consommateurs** : stockés dans Kafka, dans le topic interne `__consumer_offsets` (ex. groupe `emails` → prochain offset 4 ; groupe `analytics` → 2). Au redémarrage, un consommateur reprend à son offset.

**Question de vérif laissée sans réponse** : dans un topic aux offsets 0 à 3, un nouveau groupe `fidelite` avec `auto.offset.reset: earliest` commence à quel offset et lit combien de messages ? *(Réponse : offset 0, 4 messages.)*

### Exemples de données qui passent par Kafka
Clics et navigation (recommandations), positions GPS (Uber), paiements (détection de fraude), commandes e-commerce, logs et métriques serveurs, capteurs IoT. Point commun : beaucoup d'événements en continu, plusieurs systèmes qui doivent y réagir.

### Pull vs push (Vélib' vs Wikipédia)
- **Pull (Vélib')** : je fais un `GET` toutes les 60 s, je reçois une **photo de l'état complet** de ~1 400 stations. Les changements rapides entre deux relevés peuvent être ratés, c'est à moi de calculer les différences.
- **Push (flux Wikimedia)** : une connexion **Server-Sent Events** qui reste ouverte, le serveur envoie **chaque événement** dès qu'il se produit. Rien n'est raté, reprise possible depuis le dernier identifiant reçu (même principe que l'offset).

---

## 3. Projet fil rouge : choix de Vélib'

**Décision** (à proposer au groupe, c'est un projet collectif) : **Vélib' × météo**. Alternative si le groupe veut un vrai flux temps réel : Wikipédia en direct.

### Ce qu'on sait des données
- Open data Vélib' Métropole sur data.gouv.fr, **sans clé d'API**, JSON, licence ODbL, format standard **GBFS**.
- ~1 400 stations sur 55 communes, **mises à jour chaque minute** : vélos mécaniques, vélos électriques, places libres.
- Deuxième jeu de données : caractéristiques fixes des stations (nom, coordonnées, capacité).
- Météo : **Open-Meteo**, gratuit, sans clé.
- Volume : 1 400 stations × 1 relevé/min ≈ **2 millions de lignes par jour**.

### Pourquoi Vélib' plutôt que Wikipédia (livrable par livrable)
| Livrable | Vélib' | Wikipédia |
|---|---|---|
| S1 Cadrage | Question claire, volumes faciles à estimer | Question métier floue |
| S2 Topic | Clé `station_id` évidente | Clé moins évidente |
| S3 Connecteur REST | API Vélib' + Open-Meteo | Le flux SSE n'est pas du REST classique |
| S4 BDD / stockage objet | Fichier des stations dans Postgres | Rien de naturel |
| S5 Airflow | S'intègre bien | Le flux permanent vit hors du DAG |
| S6 Monitoring | Fraîcheur facile à suivre | Lag très visible (son point fort) |
| S7 Qualité | Vrais défauts (stations HS, compteurs incohérents) | Événements hétérogènes |

### Idées de problématiques
- Alertes stations vides ou pleines en temps réel, et durée de ces états.
- Effet de la pluie et du froid sur l'utilisation (croisement Open-Meteo).
- Migrations quotidiennes : quels quartiers se vident, à quelle heure.
- Électrique vs mécanique : les électriques partent-ils en premier ? Stations en hauteur (Montmartre, Belleville).

### Idées techniques à défendre en soutenance
- Le connecteur compare chaque relevé au précédent et ne publie dans Kafka que les **changements** : transformer un **état** en **événements**.
- **Plan B démo** : enregistrer quelques heures de vraies données dans un fichier et un script qui les republie dans Kafka si l'API tombe le jour J (la démo live vaut 50 % de la note).

### Point à vérifier
Le livrable S3 demande de gérer **authentification et pagination**, or l'API Vélib' et Open-Meteo n'ont ni clé ni pagination. Pistes : la plateforme **PRIM d'Île-de-France Mobilités** (diffuse aussi Vélib', a priori avec une clé gratuite, **à vérifier**), ou ajouter une source qui coche ces cases. **Demander au prof** en présentant la fiche de cadrage.

### Autres sources écartées (et pourquoi)
- **Bluesky** : flux public gratuit en temps réel, mais ~3 M d'utilisateurs actifs/jour (mobile, juillet 2026, estimation Similarweb) contre ~124 M pour X, audience en baisse de ~52 % depuis fin 2024, et du texte libre plus lourd à traiter.
- **X, Reddit** : API payantes ou restreintes.
- **Wikipédia pages vues** : utile mais pas représentatif des recherches (trafic humain en baisse de ~8 % sur un an selon Wikimedia, oct. 2025, à cause des réponses IA).
- **OpenSky** : quotas serrés en accès anonyme, risqué en démo.
- **TMDB** : données qui bougent une fois par jour, pas de sens au streaming.
- **éCO2mix (RTE)** : bonne option de secours, mais mises à jour au quart d'heure.

---

## 4. Boucles ouvertes

| Sujet | État | Où ça mène |
|---|---|---|
| **Partitions & clés** | **Prochaine étape.** Pas encore vu. | Séance 1 (Human Kafka), livrable S2 |
| Producer Python (`acks`, `flush()`, enveloppe d'événement) | À voir | Séance 2 |
| Consumer, consumer groups, rebalancing | À voir | Séance 2 |
| Commit avant / après traitement | Compris sur la colonne en BDD, pas encore en code Kafka (`enable.auto.commit`, `c.commit(message=msg)`) | Séance 2 |
| Garanties at-most / at-least / exactly-once | À voir | Séance 2, thème garanti au QCM |
| Consumer lag | Notion vue, pas manipulée (`kafka-consumer-groups --describe`) | Séances 2 et 6 |
| Évolution de schéma | Mise de côté volontairement. Mots-clés : backward/forward compatibility, expand and contract, breaking changes, Schema Registry, Avro | Séances 2, 7 |
| Labo pratique « faire péter » (HTTP direct vs colonne BDD vs Kafka, avec Docker Compose) | Proposé, mis de côté | Recoupe le TP S1 et « Break It on Purpose » S5 |

---

## 5. Priorités avant la séance 1

Les séances **1 et 2 ont lieu le même jour** : la journée sera entièrement Kafka (architecture le matin, code Python l'après-midi).

1. **Kafka, dans l'ordre** :
   1. Composants : broker, topic, **partition**, offset (manque surtout les partitions)
   2. Producer : clé, `acks` (0 / 1 / all), `flush()`
   3. Consumer : boucle de poll, consumer groups, commit avant/après
   4. Garanties de livraison : at-most-once, at-least-once, exactly-once
2. **Docker** : vérifier qu'il tourne avec au moins 4 Go de RAM libres pour le TP Kafka + Airflow.
3. **Fiche de cadrage** (due avant la séance 2, donc potentiellement en fin de journée) : proposer Vélib' × météo au groupe, avec Wikipédia en alternative.

**À laisser de côté pour l'instant** : Airflow, connecteurs, monitoring, qualité des données (séances 3 à 7).

---

## 6. Ressources

- Carte d'apprentissage interactive (artifact) : « Carte Ingestion de Données », créée pendant cette conversation.
- [Vélib - Vélos et bornes - Disponibilité temps réel (data.gouv.fr)](https://www.data.gouv.fr/datasets/velib-velos-et-bornes-disponibilite-temps-reel)
- [Vélib' - Localisation et caractéristique des stations (data.gouv.fr)](https://www.data.gouv.fr/datasets/velib-localisation-et-caracteristique-des-stations)
- [IDFM Vélib' sur PRIM (Île-de-France Mobilités)](https://prim.iledefrance-mobilites.fr/en/apis/idfm-velib)
- [Bluesky's active user base is shrinking (TechCrunch, août 2026)](https://techcrunch.com/2026/08/11/blueskys-active-user-base-is-shrinking-as-its-focus-expands-beyond-the-app/)
- [Wikipedia reports traffic drop (Computing, 2025)](https://www.computing.co.uk/news/2025/ai/wikipedia-reports-traffic-drop)

---

## 7. Consignes pour l'agent qui reprend

- Reprendre à **« Partitions & clés »**, avec l'exemple Vélib' (clé `station_id`).
- Rester **concret** : code, tableaux d'offsets, commandes réelles. **Pas d'analogies.**
- Un bloc à la fois, terminer par une petite question de vérification.
- Faire le lien avec ce que je connais déjà : HTTP, POST/PUT, migrations de BDD, mon pipeline TMDB.
- Le support de cours `collecte_ingestion.md` est la référence pour le contenu et les livrables.
