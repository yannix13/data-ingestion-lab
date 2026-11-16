# Feuille de route d'apprentissage — data engineering & infra

**Règle n°1 : le module « Collecte & Ingestion » (M1 Data & IA) passe avant tout.**
Chaque notion du cours doit être comprise et pratiquée. Tout le reste (TMDB conteneurisé, Celery, k3s, Spark…)
est un **bonus**, à faire seulement quand le cours est à jour.

- Support de cours : `Cours/cours_collecte_ingestion.md` (7 séances, à partir de la semaine du 23/11/2026)
- Préparation déjà faite : `Cours/recap_preparation_kafka.md` et `Cours/Carte apprentissage.html`
- Journal de bord des leçons infra : `Infra_learning/infra_learning.md`
- Projet d’école précédent (fini) : pipeline TMDB, dans `/home/ubuntu/movie-pipeline/` (ses données servent aux labos Kafka)

## Profil et objectifs

- **Parcours :** développeur fullstack, en reconversion vers la data (débutant en data engineering, novembre 2026).
  Kafka, Airflow et Docker sont nouveaux.
- **Acquis à réutiliser :** le code, Git, HTTP et les API (POST vs PUT), les migrations de BDD, le pipeline TMDB.
- **Façon d'apprendre :** du concret (code, cas réels, ce qui casse exactement), **pas d'analogies**,
  faire casser les systèmes volontairement, le « pourquoi » avant le « comment », un bloc à la fois avec une question de vérification.
- **Ce que je veux apprendre :**
  - **data engineering :** ingestion, connecteurs, orchestration, qualité des données ;
  - **systèmes distribués :** Kafka (partitions, consumer groups, garanties de livraison), pannes partielles, idempotence ;
  - **system engineering :** conteneurs, ressources (RAM, swap), réseau ;
  - **system design :** savoir **concevoir** une architecture et justifier ses choix.
- **Méthode pour le system design :** à chaque étape, noter dans le journal *pourquoi cette architecture*,
  *quelles autres options existaient* et *quel compromis on accepte* (coût, RAM, complexité, tolérance aux pannes).

---

## Vue d'ensemble

| # | Quand | Ce qu'on fait | Ce qu'on apprend | Statut |
|---|---|---|---|---|
| **0** | 15/11 | Prérequis VPS : swap 2 Go, MySQL hôte coupé | Swap, OOM killer, systemd | ✅ |
| **1** | avant le 23/11 | Environnement du TP : Docker (WSL) + Kafka en KRaft | Image, conteneur, compose, volumes, ports | ⬜ ← **en cours** (dans WSL) |
| **2** | séances 1-2 | Kafka : architecture puis producer/consumer Python | Partitions, clés, offsets, acks, commits, garanties, consumer groups, lag | ⬜ (prochaine notion : partitions & clés) |
| **3** | séances 3-4 | Connecteurs : REST, base relationnelle, MinIO | `BaseConnector`, auth, pagination, backoff, watermark, CDC, stockage objet | ⬜ |
| **4** | séance 5 | Orchestration Airflow | DAG, fan-out/fan-in, trigger rules, retries, idempotence, backfill | ⬜ |
| **5** | séance 6 | Monitoring | Latence, volume, fraîcheur, lag, alertes, logs JSON | ⬜ |
| **6** | séance 7 | Qualité upstream | Validation, normalisation, déduplication, DLQ, quarantaine, rejeu | ⬜ |
| Bonus | après | TMDB en Docker, Celery, k3s, Spark/DuckDB… | voir la section Bonus | ⬜ |

Principe : chaque étape n'ajoute **qu'une** nouvelle notion. Pour chaque notion du cours :
1. comprendre le **pourquoi** ;
2. lire ou écrire le code ;
3. **la faire casser volontairement** (labo) et observer ;
4. pouvoir répondre aux questions du QCM sur ce thème.

---

## Étape 0 — Prérequis VPS ✅

- [x] Swap de 2 Go (`/swapfile`, persistant via `/etc/fstab`)
- [x] MySQL hôte arrêté + désactivé (vestige inutilisé, ~284 Mo récupérés)
- [x] Repo TMDB cloné dans `/home/ubuntu/movie-pipeline/`
- [ ] Fin novembre : désinstaller MySQL si rien n'a cassé
- [ ] (optionnel) Baisser `vm.swappiness` à 10

---

## Étape 1 — Environnement du TP (avant le 23/11)

**But** : arriver en séance 1 avec Docker qui marche et un premier Kafka déjà lancé une fois.

**Changement de plan (16/11) : on commence directement dans WSL** sur le PC portable, c'est la machine du TP.
Le VPS reste disponible plus tard (bonus, ou pour comparer).
Le dépôt est transféré par GitHub : on le clone dans WSL et on continue là-bas.

Dans WSL (PC portable) — **maintenant** :
- [ ] Cloner le dépôt dans le système de fichiers Linux (`~/…`), pas dans `/mnt/c/…` (lent, et les volumes s'y comportent mal)
- [ ] `docker version`, `docker compose version` (v2+) ; noter si c'est Docker Desktop (intégration WSL) ou Docker Engine installé dans la distribution
- [ ] RAM donnée à WSL (`free -h` : ≥ 4 Go libres pour Kafka + Airflow), sinon la régler dans `.wslconfig` côté Windows
- [ ] Vérifier que le port 9092 est libre (`ss -ltn`) et que `docker pull` passe sur le réseau
- [ ] Écrire `kafka/docker-compose.yml` (proposition prête dans `REPRISE.md`), le relire, puis le lancer
- [ ] Créer un topic, publier un message, le relire en ligne de commande
- [ ] Vérifier que les messages survivent à `docker compose down` puis `up` (volume nommé)

Sur le VPS — plus tard, si utile :
- [x] RAM, ports 9092/9093 libres, compose disponible (vérifié le 16/11 ; la commande y est `docker-compose`)
- [ ] Même compose Kafka, port limité à `127.0.0.1`, `mem_limit` obligatoire
- [ ] Interface Airflow : par tunnel SSH uniquement, jamais le port 8080 ouvert sur Internet

**Concepts** : image vs conteneur, `docker compose up -d` / `ps` / `logs`, volume nommé, ports, mode KRaft.

---

## Étape 2 — Kafka (séances 1-2) ← priorité actuelle

**Notions, dans l'ordre :**
- [x] Pourquoi Kafka : état vs événements, découplage, polling et watermark, suppressions invisibles (vu dans le récap)
- [x] Broker, topic, offset, offsets des consommateurs (vu dans le récap)
- [ ] **Partitions et clés** : hachage de la clé, ordre garanti par partition seulement, hot partitions (exemple Vélib' : `station_id`)
- [ ] Producer : sérialisation, clé, `acks` (0 / 1 / all), `flush()`, callback de livraison, enveloppe d'événement
- [ ] Consumer : boucle de `poll`, `enable.auto.commit`, commit avant / après traitement
- [ ] Garanties : at-most-once, at-least-once, exactly-once ; consumer idempotent (UPSERT sur `event_id`)
- [ ] Consumer groups, rebalancing, consumer lag (`kafka-consumer-groups --describe`)
- [ ] Batch vs streaming vs micro-batch ; pull / push / événementiel ; les 3 V (+ véracité)

**Labos « faire casser » :**
- [ ] Clé à 3 valeurs sur 3 partitions puis sur 6 → observer la hot partition
- [ ] 4 consumers pour 3 partitions → un consumer reste inactif
- [ ] Producer sans `flush()` → messages perdus
- [ ] `enable.auto.commit=True` + `kill -9` en plein traitement → message perdu ; commit après traitement → doublon
- [ ] Arrêter un consumer → voir le lag monter, puis le rattrapage au redémarrage

**Livrables :** fiche de cadrage (S1, Vélib' × météo proposé) ; section « Topic d'ingestion » du README (S2 : nom, partitions, clé, format).

---

## Étape 3 — Connecteurs (séances 3-4)

- [ ] `BaseConnector` : `extract()`, `to_event()`, `run()` ; tests unitaires sur `extract()` et `to_event()`
- [ ] Auth : API Key et OAuth2 client credentials, secrets en variables d'environnement
- [ ] Pagination : offset, page, curseur, en-tête `Link` et leurs pièges
- [ ] Rate limiting : 429, `Retry-After`, backoff exponentiel + jitter ; quels codes HTTP réessayer
- [ ] Extraction incrémentale : état persisté, watermark avancé **après** publication
- [ ] SQLAlchemy par lots ; pièges du watermark (horodatages égaux, transactions longues, suppressions)
- [ ] Polling vs CDC (Debezium, WAL)
- [ ] MinIO / S3 avec boto3 ; organisation `source/table/date=YYYY-MM-DD/`

**Labo :** watermark avancé avant le `flush()` + crash → données perdues.
**Livrables :** connecteur REST (S3) ; deuxième connecteur base ou MinIO sur le même topic (S4).
**Lien avec TMDB :** mon `fetch_async` fait déjà retries, backoff, `Retry-After`, sémaphore et checkpoint.

---

## Étape 4 — Airflow (séance 5)

- [ ] DAG TaskFlow (`@dag`, `@task`), `schedule`, `start_date`, `catchup`, `logical_date`
- [ ] Fan-out / fan-in, `@task.branch`, trigger rules (`all_success` vs `none_failed_min_one_success`)
- [ ] Retries, backoff exponentiel, `execution_timeout` ; erreurs transitoires vs déterministes
- [ ] Idempotence : écraser une partition, UPSERT, ids déterministes ; Clear et backfill sans doublon

**Labo « Break It on Purpose » :** faux endpoint, valeur invalide, timeout.
**Livrable :** DAG unifié (2 connecteurs en parallèle puis chargement), idempotence démontrée.

---

## Étape 5 — Monitoring (séance 6)

- [ ] Métriques : latence, volume, fraîcheur, taux d'erreur, consumer lag
- [ ] Callbacks Airflow (`on_failure_callback`…), alertes utiles et actionnables, runbook
- [ ] Logs JSON structurés, centralisés, sans secrets
- [ ] « Un DAG vert ne veut pas dire que les données sont arrivées »

**Livrable :** tableau de métriques, alertes (échec + absence de données), logs centralisés.

---

## Étape 6 — Qualité upstream (séance 7)

- [ ] Les 4 niveaux de validation ; JSON Schema (message) vs pandera (lot, `lazy=True`)
- [ ] Normalisation idempotente ; déduplication (event_id, clé métier, fenêtre)
- [ ] DLQ (topic `.dlq` enrichi) vs quarantaine (MinIO) ; procédure de rejeu ; alerte si rejet > 5 %

**Labo :** injecter 10 messages piégés et vérifier qu'ils sont tous interceptés.
**Livrable :** consumer de validation dans le DAG, DLQ + quarantaine, taux de rejet monitoré.

---

## Avant la soutenance

- [ ] Check-list projet du cours (code, DAG, monitoring, documentation)
- [ ] Démo live stable (50 % de la note) + plan B (données enregistrées republiées, captures)
- [ ] Révisions QCM : thèmes garantis + slides « erreurs fréquentes »

---

## Bonus (seulement quand le cours est à jour)

### B1 — Conteneuriser le pipeline TMDB

- [ ] `Dockerfile` du pipeline (sans `huggingface_hub`/`datasets`) + `docker-compose.yml` (`pipeline-postgres` + `pipeline` one-shot)
- [ ] Volumes pour `data/` et pour Postgres
- [ ] `load_all_parquet.py` : mot de passe lu depuis `DATABASE_URL` au lieu du clavier
- [ ] Run sur 1-2 années puis complet ; `pytest` dans le conteneur ; analyses SQL

### B2 — Airflow sur le VPS (pipeline TMDB)

- [ ] Compose Airflow 3 réduit avec `mem_limit` ; DAG `ingest` → `transform` → `test` → `load` → `analyses`
- [ ] Route `/airflow/` dans `nginx-gateway`, protégée par oauth2-proxy ; planning géré par Airflow, pas de crontab

### B3 — Airflow CeleryExecutor + Redis

- [ ] Redis + N workers, `--scale airflow-worker=3`, Flower ; tuer un worker en pleine tâche
- À noter : Kafka (étape 2) couvre déjà files de messages, workers et at-least-once.

### B4 — k3s + KubernetesExecutor

- [ ] k3s **sans Traefik** (`--disable traefik`) : nginx-gateway garde les ports 80/443
- [ ] `kubectl` (pod, deployment, service, configmap, secret), Airflow par Helm, 1 tâche = 1 pod

### B5 — Calcul et stockage analytique

- [ ] `transform.py` en PySpark et en DuckDB ; BigQuery ou Snowflake ; dbt ; Kafka Streams / Flink / Schema Registry (suite du cours)

---

## Points d'attention

- RAM du VPS : 7,6 Go au total, ~5 Go disponibles + 2 Go de swap → toujours mettre des `mem_limit`.
- Ports 80/443 du VPS réservés à nginx-gateway (cf. panne de novembre) ; 8080 et 9092 jamais ouverts sur Internet.
- Un seul mécanisme de planification : Airflow (pas de crontab en double, cf. leçon certbot).
- Secrets uniquement dans des `.env` non versionnés.
- Données TMDB : usage non commercial, attribution obligatoire. Vélib' : licence ODbL.
