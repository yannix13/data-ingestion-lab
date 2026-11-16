# Consignes pour l'agent

Dépôt personnel d'apprentissage de Yannis : module **Collecte & Ingestion** (M1 Data & IA) et infra sur son VPS.

## À lire en début de session

- `REPRISE.md` : où on en est et la prochaine action.
- `Infra_learning/infra_roadmap.md` : la feuille de route (cours en priorité, bonus ensuite).
- `Cours/cours_collecte_ingestion.md` : le support de cours, qui fait foi pour les notions et les livrables.
- `Cours/recap_preparation_kafka.md` : ce qui a déjà été vu sur Kafka.

## Priorité

Le cours passe avant tout. Aucune notion du cours ne doit être négligée (les thèmes du QCM en particulier).
Tout le reste (TMDB conteneurisé, Celery, k3s, Spark…) est un bonus.

## Comment travailler avec Yannis

- Dev fullstack en reconversion vers la data : ne pas réexpliquer la programmation, HTTP ou Git ;
  tout ce qui est data, infra ou distribué s'explique depuis le début.
- **Concret** : du code, des commandes réelles, des cas réels, ce qui casse exactement. **Pas d'analogies.**
- Apprendre en pratique : faire casser le système volontairement (labos) et observer.
- Toujours le **pourquoi** avant le comment. Pour le system design : pourquoi ce choix, quelles autres options, quel compromis.
- **Un bloc à la fois**, terminé par une petite question de vérification.
- Faire le lien avec ce qu'il connaît : HTTP, POST/PUT, migrations de BDD, son pipeline TMDB.
- Ne pas écrire ni modifier de code sans feu vert explicite. Écrire, commenter, puis s'arrêter pour qu'il lise.
- Écrire en français.

## Tenir le suivi

- Après chaque avancement : cocher la feuille de route (`Infra_learning/infra_roadmap.md`) et mettre à jour `REPRISE.md`.
- Les leçons infra (Docker, réseau, RAM…) vont dans `Infra_learning/infra_learning.md`, sous un titre daté.

## Règles

- Jamais de secret (clé TMDB, mots de passe) dans un fichier versionné : seulement dans `.env`, ignoré par Git.
- Sur le VPS : ne jamais publier un port sur Internet (toujours `127.0.0.1:…`), ne pas toucher aux conteneurs existants ni aux ports 80/443, toujours mettre un `mem_limit`.
- Ne pas commiter ni pousser sans demande explicite.
