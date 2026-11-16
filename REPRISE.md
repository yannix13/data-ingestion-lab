# Reprise — où on en est et marche à suivre

*Mis à jour le 16/11/2026. On quitte le VPS pour continuer dans **WSL sur le PC portable** ; le dépôt passe par GitHub.*

## 1. Le contexte en 30 secondes

- **Qui :** Yannis, dev fullstack en reconversion vers la data. Kafka, Airflow et Docker sont nouveaux pour lui.
- **Priorité absolue :** le module **Collecte & Ingestion** du M1 Data & IA (7 séances, à partir de la semaine du 23/11/2026).
  Les séances 1 et 2 (Kafka) ont lieu le même jour. Tout ce qui n'est pas dans le cours est un **bonus**.
- **Ce dépôt** (`data-ingestion-lab`) est le dépôt personnel d'apprentissage : notes de cours, feuille de route, labos.
  Le projet d'école précédent, `movie-pipeline` (TMDB → JSONL → Parquet → PostgreSQL → Hugging Face), est **terminé**.
- **Machine :** à partir de maintenant, on travaille dans **WSL sur le PC portable** (c'est la machine du TP).
  Le VPS reste disponible plus tard (bonus, ou pour comparer).

## 2. Ce qu'il y a dans ce dépôt

| Chemin | Contenu |
|---|---|
| `Cours/cours_collecte_ingestion.md` | Le support de cours intégral (référence pour les notions et les livrables) |
| `Cours/recap_preparation_kafka.md` | La préparation à Kafka déjà faite avec un autre agent. **S'arrête à « Partitions & clés ».** |
| `Cours/Carte apprentissage.html` | Carte interactive des notions vues et des boucles ouvertes |
| `Infra_learning/infra_roadmap.md` | **La feuille de route** : cours en priorité (étapes 1 à 6, séance par séance), bonus ensuite |
| `Infra_learning/infra_learning.md` | Journal des leçons infra (VPS : nginx-gateway, certbot, logs Docker, swap, RAM) |
| `CLAUDE.md` | Les consignes pour l'agent (façon d'apprendre, règles) |

## 3. Décisions déjà prises

- Le cours passe avant tout ; TMDB conteneurisé, Airflow sur le VPS, Celery, k3s et Spark sont en bonus.
- Nouveau dépôt pour le cours plutôt que `movie-pipeline` (projet fini, à garder propre).
- **16/11 : environnement du TP monté directement dans WSL** (et non d'abord sur le VPS puis dans WSL).
- Projet de groupe proposé : **Vélib' × météo** (à valider avec le groupe ; il aura son propre dépôt).
- Les **données TMDB** servent aux **labos Kafka**, pas au projet de groupe. Exemple prévu : publier les films avec
  `original_language` comme clé → hot partition (les films `en` sont majoritaires), puis avec `id` → répartition équilibrée.
  Il faudra relancer l'ingestion TMDB (clé dans un `.env`) ou commencer avec des messages écrits à la main.
- Idée à garder pour plus tard : TMDB a une clé d'API, de la pagination et `/movie/changes` (incrémental). Ça coche le livrable S3, que Vélib' ne coche pas. À discuter avec le groupe et le prof.

## 4. Le compose Kafka proposé (pas encore écrit dans `kafka/`)

Proposé le 16/11, **en attente de relecture et de feu vert** de Yannis avant d'être écrit dans `kafka/docker-compose.yml`.

Pourquoi l'exemple du cours (`cours_collecte_ingestion.md`, « Docker Compose, structure clé ») ne suffit pas tel quel :

1. **Le conteneur s'arrête au démarrage.** En KRaft, le broker est aussi controller (il gère les métadonnées : topics, partitions, leaders).
   Il lui faut `KAFKA_CONTROLLER_QUORUM_VOTERS`, `KAFKA_CONTROLLER_LISTENER_NAMES` et un `CLUSTER_ID`, sans lequel l'image Confluent refuse de démarrer.
2. **`KAFKA_ADVERTISED_LISTENERS`** est l'adresse que le broker renvoie aux clients. Le client se connecte, demande « où sont les leaders ? »,
   puis se reconnecte à l'adresse annoncée. Si elle est fausse : connexion initiale OK, puis timeout à la publication.
3. **Port :** `"9092:9092"` écoute sur toutes les interfaces. Sur le VPS, Docker contourne ufw. Dans WSL, c'est moins exposé,
   mais on garde le réflexe `127.0.0.1:9092:9092`.
4. **Mémoire :** l'image lance la JVM avec `-Xmx1G` par défaut. Avec `mem_limit: 1g`, le noyau tue le processus (`Exited (137)`).
   Il faut un heap plus petit que la limite.
5. **Un seul broker :** `__consumer_offsets` (offsets des consumer groups) a un facteur de réplication de 3 par défaut.
   Il faut le mettre à 1, sinon les consumer groups ne fonctionnent pas.

```yaml
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.0       # même image que le cours
    container_name: lab-kafka
    mem_limit: 1g                            # borne la RAM prise à WSL
    environment:
      # --- KRaft : ce nœud est broker ET controller, sans ZooKeeper
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qk"   # fixe : le volume est formaté avec cet id
      # --- Listeners : 9092 pour les clients, 9093 pour le controller (interne)
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      # --- Un seul broker : réplication des topics internes à 1
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      # --- Heap JVM sous le mem_limit (défaut 1G → tué par le noyau)
      KAFKA_HEAP_OPTS: "-Xms256m -Xmx512m"
    ports:
      - "127.0.0.1:9092:9092"                # pas d'exposition réseau
    volumes:
      - kafka-data:/var/lib/kafka/data       # les messages survivent à un down/up

volumes:
  kafka-data:
```

**Question de vérification laissée en suspens :** avec `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092`, un script Python lancé
**hors Docker** avec `bootstrap.servers=localhost:9092` arriverait-il à publier ? Que se passerait-il exactement ?

## 5. Marche à suivre sur le PC portable, dans l'ordre

1. **Cloner le dépôt dans WSL**, dans le système de fichiers Linux (`~/data-ingestion-lab`), pas sous `/mnt/c/…`.
   Ouvrir le dossier dans VS Code (extension WSL) et lancer Claude Code. Lui dire : *« Lis REPRISE.md et CLAUDE.md, on reprend. »*
2. **Vérifier l'environnement :** `docker version`, `docker compose version` (v2+), `free -h` (≥ 4 Go libres), port 9092 libre.
   Noter s'il s'agit de Docker Desktop (intégration WSL) ou de Docker Engine installé dans la distribution.
   Dans WSL, la commande sera sans doute `docker compose` (celle du cours), pas `docker-compose` comme sur le VPS.
3. **Répondre à la question de vérification** (section 4), relire le compose, puis donner le feu vert pour écrire `kafka/docker-compose.yml`.
4. **Lancer et vérifier :** `docker compose up -d`, `docker compose ps`, `docker logs lab-kafka`, puis créer un topic,
   publier un message et le relire en ligne de commande. Faire `down` puis `up` et vérifier que le message est toujours là.
5. **Notion de cours suivante : partitions et clés** (exemple Vélib' `station_id`, puis labo hot partition avec TMDB).
6. Ensuite, suivre `Infra_learning/infra_roadmap.md`, étape 2 (Kafka) : producer, consumer, commits, garanties, consumer groups, lag.

## 6. En attente (Yannis)

- Fiche de cadrage (livrable S1) : proposer Vélib' × météo au groupe.
- Dans `movie-pipeline` (VPS) : `docs/HISTORIQUE.md` a une modification non commitée (entrées du 15 et 16/11 sur ces docs).

## 7. Mémo VPS (pour plus tard)

- RAM : 7,6 Go au total, ~5 Go disponibles, + 2 Go de swap. Ports 9092 / 9093 / 8080 libres (vérifié le 16/11).
- Ports 80/443 pris par `nginx-gateway` ; 10 conteneurs perso tournent déjà : ne pas les toucher.
- La commande y est **`docker-compose`** (avec un tiret, v5.0.0) ; le plugin `docker compose` n'est pas installé.
