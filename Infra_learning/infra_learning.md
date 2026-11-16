# Journal d'apprentissage infra — VPS yannis-devweb.cloud

## 11 novembre 2026

### Panne nginx-gateway (site down 12 jours)

**Ce qui s'est passé**
Le conteneur `nginx-gateway` était en `Exited (128)` depuis le 30 octobre.
Cause : certbot (renouvellement SSL) tourne en mode **standalone** — il ouvre son propre serveur sur le port 80.
Problème : nginx occupe déjà le port 80. Deux process ne peuvent pas écouter sur le même port → conflit → nginx ne redémarre pas.

**Ce qu'on a appris**
- Exit code 128 sur Docker = erreur réseau (port déjà pris)
- `docker inspect <conteneur>` donne la raison exacte de l'échec
- `restart: unless-stopped` ne suffit pas si le port est toujours occupé au moment du retry
- Certbot a deux modes : **standalone** (prend le port 80, incompatible avec nginx) et **webroot** (dépose un fichier, nginx reste UP)
- Les certificats Let's Encrypt expirent tous les 90 jours, certbot renouvelle quand il reste < 30 jours

---

### Déconnexions SSH fréquentes

**Ce qui s'est passé**
La connexion SSH coupait après ~1 minute d'inactivité.
Cause probable : OVH (ou la box) a un timeout NAT court (~60s). Quand le terminal est inactif, la session TCP est coupée côté réseau sans que SSH le sache.

**Ce qu'on a appris**
- SSH keepalive = des petits "je suis encore là ?" envoyés régulièrement pour garder la connexion vivante
- `ClientAliveInterval` (côté serveur) et `ServerAliveInterval` (côté client Mac) doivent être **inférieurs** au timeout NAT du réseau
- 120s n'était pas assez court → on est passé à 30s
- Le fichier de config SSH client est `~/.ssh/config` sur le Mac
- `sudo systemctl reload ssh` applique les changements sans couper les sessions en cours

**Config appliquée**
- Serveur (`/etc/ssh/sshd_config`) : `ClientAliveInterval 30`, `ClientAliveCountMax 40`
- Client Mac (`~/.ssh/config`) : `ServerAliveInterval 30`, `ServerAliveCountMax 40`
- Résultat : timeout à 20 min d'inactivité, keepalive toutes les 30s

---

### Sécurité SSH

**Ce qu'on a découvert**
- 132 894 tentatives de brute force sur le port 22, bloquées par fail2ban
- `authorized_keys` contient 2 clés SSH (perso + github-actions)
- L'auth par mot de passe est encore active (risque potentiel)

**Ce qu'on a appris**
- fail2ban bannit automatiquement les IP qui échouent trop souvent
- Changer le port SSH (ex. 2222) réduit le bruit mais nécessite de s'en souvenir
- Désactiver `PasswordAuthentication` = seules les clés SSH fonctionnent (plus sûr)
- Avant de désactiver les mots de passe, toujours vérifier que la clé est bien dans `authorized_keys`

---

## 13 novembre 2026

### Étape 1 complète — nginx-gateway relancé + certbot webroot

**Ce qu'on a fait**

1. Relancé `nginx-gateway` avec `docker-compose up -d` (tous les autres conteneurs étaient déjà UP)
2. Passé certbot en mode **webroot** pour les deux certificats

**Leçons importantes**

- `docker-compose restart` ne recharge pas les nouveaux volumes ajoutés dans le docker-compose.yml → il faut `docker-compose up -d` pour recréer le conteneur
- Un `return 301` placé directement dans un bloc `server {}` nginx intercepte TOUTES les requêtes, même les `location` qu'on ajoute en dessous. Il faut le mettre dans un `location / {}` pour que les autres locations soient prioritaires
- Les volumes Docker : le conteneur accède en temps réel aux fichiers du VPS, pas une copie au démarrage
- Il existait des hooks certbot dans `/etc/letsencrypt/renewal-hooks/pre/` et `post/` qui arrêtaient nginx avant chaque renouvellement (mode standalone). Ils ont été supprimés car incompatibles avec webroot

**Modifications apportées**

`/home/ubuntu/nginx-gateway/nginx.conf` — les deux blocs HTTP (port 80) ont été modifiés :
```nginx
server {
    listen 80;
    server_name yannis-devweb.cloud www.yannis-devweb.cloud;

    location /.well-known/acme-challenge/ {
        root /var/www/certbot;
    }

    location / {
        return 301 https://$host$request_uri;
    }
}
```
(idem pour requestformat.fr)

`/home/ubuntu/nginx-gateway/docker-compose.yml` — volume ajouté :
```yaml
- /var/www/certbot:/var/www/certbot:ro
```

`/etc/letsencrypt/renewal/yannis-devweb.cloud.conf` et `requestformat.fr.conf` :
```
authenticator = webroot
webroot_path = /var/www/certbot
```

Hooks supprimés :
- `/etc/letsencrypt/renewal-hooks/pre/stop-nginx-gateway.sh`
- `/etc/letsencrypt/renewal-hooks/post/start-nginx-gateway.sh`

Hook mis à jour `/etc/letsencrypt/renewal-hooks/deploy/fix-nginx-permissions.sh` :
```bash
#!/bin/bash
chmod 755 /etc/letsencrypt/live /etc/letsencrypt/archive
docker exec nginx-gateway nginx -s reload
```

Dossier créé sur le VPS : `/var/www/certbot`

**Validation**
`sudo certbot renew --dry-run` → les deux certificats passent avec succès.

---

---

## 14 novembre 2026

### Certbot tournait deux fois en parallèle

**Ce qu'on a découvert**
Certbot était lancé par deux mécanismes simultanément :
- Un **systemd timer** (installé automatiquement par Ubuntu avec certbot) → fonctionnait correctement
- Une **root crontab** (ajoutée manuellement, probablement en suivant un tuto) → **cassée** depuis des semaines

La crontab generait des erreurs `Unable to find deploy-hook command cd` dans `/var/log/certbot-renew.log` en boucle.
Cause : `cd` est une commande du shell, pas un binaire exécutable. Le deploy-hook `--deploy-hook "cd /home/... && docker-compose restart"` ne fonctionnait pas.

**Ce qu'on a appris**

**Crontab vs systemd timer — deux technologies pour la même chose :**
- **Crontab** : l'ancien système (Unix, années 70). Un fichier texte qui dit "lance cette commande à telle heure". Logs redirigés manuellement.
- **Systemd timer** : le système moderne. Géré par systemd (le chef d'orchestre du serveur). Logs automatiquement dans `journalctl`. Ubuntu installe un timer certbot par défaut.

| | Crontab | Systemd timer |
|---|---|---|
| Config | Fichier texte simple | Service systemd |
| Logs | Redirigés manuellement | Automatiquement dans journalctl |
| Sur ce VPS | Ajouté manuellement, cassé | Installé par Ubuntu, fonctionnel |

**Commandes utiles :**
```bash
sudo crontab -l              # voir la crontab de root
systemctl status certbot.timer   # état du timer systemd
journalctl -u certbot        # logs du timer certbot
```

**Ce qu'on a fait**
1. Supprimé la ligne certbot de la root crontab : `sudo crontab -l | grep -v certbot | sudo crontab -`
2. Vidé le fichier de log pollué : `sudo truncate -s 0 /var/log/certbot-renew.log`
3. Vérifié que le systemd timer est actif (`active (waiting)`, prochain déclenchement automatique)

**Règle retenue** : un seul mécanisme suffit. Ne jamais doubler un timer systemd avec une crontab manuelle.

---

### Étape 2 — Logs Docker centralisés + rotation

**Ce qu'on a découvert**
Les logs Docker grossissent indéfiniment par défaut. Chaque requête HTTP (y compris les bots) écrit une ligne. `hello-gateway` avait accumulé **56 Mo** depuis son lancement.

**Ce qu'on a appris**

- Docker stocke les logs dans `/var/lib/docker/containers/<id>/<id>-json.log`
- Par défaut : **aucune rotation**, le fichier grossit jusqu'à remplir le disque
- Les bots scannent en permanence toutes les adresses IP publiques (robots.txt, .env, WordPress…) → logs constants même si le site ne reçoit aucun vrai visiteur
- La rotation se configure globalement dans `/etc/docker/daemon.json`
- Elle ne s'applique qu'aux **nouveaux** conteneurs — les existants doivent être recréés avec `docker-compose up -d --force-recreate`

**Ce qu'on a fait**

1. Créé `/etc/docker/daemon.json` :
```json
{
  "log-driver": "json-file",
  "log-opts": {
    "max-size": "10m",
    "max-file": "3"
  }
}
```
→ Max 10 Mo par fichier, 3 fichiers conservés = 30 Mo max par conteneur

2. `sudo systemctl restart docker` → tous les conteneurs ont redémarré automatiquement (grâce à `restart: unless-stopped`)

3. Tronqué le log de hello-gateway : `sudo truncate -s 0 <fichier>`

4. Recréé hello-gateway : `docker-compose up -d --force-recreate gateway` pour qu'il hérite de la rotation

**Vérification**
```bash
docker inspect hello-gateway --format='{{index .HostConfig.LogConfig.Config "max-size"}}'
# → 10m ✓
```

**Commandes utiles :**
```bash
# Taille de tous les logs Docker
docker ps --format "{{.Names}}" | while read name; do
  id=$(docker inspect --format='{{.Id}}' "$name")
  size=$(sudo du -sh /var/lib/docker/containers/$id/${id}-json.log 2>/dev/null | cut -f1)
  echo "$size  $name"
done | sort -rh

# Logs d'un conteneur en temps réel
docker logs <conteneur> --tail 20 --follow

# Vérifier la config log d'un conteneur
docker inspect <conteneur> --format='{{.HostConfig.LogConfig}}'
```

---

## 15 novembre 2026

### Nouveau projet : pipeline ETL movie-analytics → Airflow

**Contexte**
Démarrage d'un projet d'apprentissage data engineering + system engineering : déployer le pipeline TMDB
(`github.com/yannix13/movie-analytics-pipeline`) sur le VPS, puis le faire évoluer progressivement
vers un système orchestré et distribué. Feuille de route détaillée : `Infra_learning/infra_roadmap.md`.

Repo cloné dans `/home/ubuntu/movie-pipeline/` (partie Hugging Face publish ignorée).

---

### Concepts data appris

**Orchestrateur (Airflow, Dagster, Prefect)**
Les data engineers ne codent pas d'UI maison pour suivre leurs pipelines : l'orchestrateur la fournit
(graphe des tâches, historique des runs, logs, retries, planning). Les dashboards maison servent à
visualiser les **données**, pas l'exécution.

**Airflow = chef d'orchestre, pas musicien**
Il décide quoi lancer, quand, dans quel ordre, et réessaie en cas d'échec. Le calcul lourd est
généralement délégué ailleurs (Spark, Snowflake, dbt).

| Executor | Principe | Usage |
|---|---|---|
| LocalExecutor | Tâches = process sur la machine du scheduler | Petites équipes, 1 serveur |
| CeleryExecutor | File Redis + N workers qui piochent dedans | Équipes moyennes/grandes |
| KubernetesExecutor | 1 tâche = 1 pod éphémère | Très courant en entreprise |

On distribue Airflow pour le **nombre** de tâches, pas pour leur poids.

**Spark** : moteur de calcul distribué. Découpe les données en partitions, chaque machine du cluster
traite un morceau en parallèle. Code proche de pandas/SQL. Utile quand les données ne tiennent plus
sur une machine.

**Snowflake / BigQuery / Redshift** : data warehouses cloud. On y fait du SQL directement, le service
distribue la requête tout seul. Stockage en colonnes (comme Parquet), optimisé pour l'analyse sur des
milliards de lignes — contrairement à PostgreSQL, optimisé pour les petites lectures/écritures d'une app.

**k3s** : un vrai Kubernetes certifié CNCF, en version légère (1 binaire, ~500 Mo RAM, SQLite au lieu
d'etcd, ingress Traefik inclus). Mêmes `kubectl`, YAML et Helm qu'un cluster d'entreprise.
⚠️ Traefik veut les ports 80/443 → conflit avec nginx-gateway à gérer à l'étape k3s.

---

### RAM : comment Linux la partage entre les conteneurs

**Ce qu'on a appris**
- Un conteneur **ne réserve pas** de RAM (contrairement à une VM) : c'est un process Linux isolé qui
  utilise ce dont il a besoin à l'instant T. Le partage "just in time" est fait par le noyau.
- Toutes les apps Docker du VPS réunies ≈ 240 Mo. Les gros consommateurs sont hors Docker :
  code-server / VS Code (node ~950 Mo), Claude, journald.
- `buff/cache` dans `free -h` = cache disque que Linux libère dès qu'une app en a besoin.
  La bonne colonne à regarder est **available**.
- Quand la RAM est pleine et sans swap → l'**OOM killer** (Out Of Memory) tue un process, potentiellement
  une app importante.
- Garde-fous dans un `docker-compose.yml` :
  ```yaml
  mem_limit: 1g          # plafond dur
  mem_reservation: 256m  # minimum que le noyau évite de reprendre
  ```
- **Scale-to-zero** : éteindre une app inutilisée et la rallumer à la première requête (au prix d'un
  *cold start*). Outils : Sablier (Docker + proxy), systemd socket activation, KEDA/Knative (Kubernetes),
  Lambda/Cloud Run (cloud).

---

### MySQL hôte arrêté (vestige)

**Ce qu'on a découvert**
Un MySQL tournait directement sur Ubuntu (`mysql.service`, hors Docker) et consommait ~284 Mo.
- N'écoutait pas sur 3306, seulement sur `127.0.0.1:33060` → inaccessible aux conteneurs
- Aucune connexion active, aucune app ne le référence
- L'historique ne contient qu'une table de test `posts` → vestige d'un tuto

**Ce qu'on a fait** (réversible, pas de désinstallation)
```bash
sudo systemctl stop mysql
sudo systemctl disable mysql
```
Pour le relancer si besoin : `sudo systemctl enable --now mysql`
**À faire fin novembre** : si rien n'a cassé → désinstaller.

---

### Swap de 2 Go ajouté

**Ce qu'on a appris**
- Le swap n'est **pas** propre à Docker : c'est une fonction du **noyau Linux**, valable pour toute la
  machine (donc aussi pour les conteneurs).
- C'est une partie du disque (ici le disque virtuel OVH `/dev/sda1`) utilisée comme RAM de secours :
  le noyau y déplace les pages mémoire les moins utilisées (ex. une app qui dort la nuit).
- Analogie : RAM = le bureau, swap = le carton sous le bureau.
- C'est un **amortisseur de pics**, pas de la RAM en plus : le disque est ~1000× plus lent. Un serveur
  qui swappe en permanence manque vraiment de RAM.
- `swapon` seul ne survit pas au reboot → il faut une ligne dans `/etc/fstab`.
- `swappiness` (0-100, défaut 60) règle la tendance à swapper. Sur un serveur on la baisse souvent à 10.
  Non modifiée pour l'instant.

**Ce qu'on a fait**
```bash
sudo fallocate -l 2G /swapfile   # réserve le fichier
sudo chmod 600 /swapfile         # root uniquement (contient des bouts de RAM)
sudo mkswap /swapfile            # formate en swap
sudo swapon /swapfile            # active
sudo cp /etc/fstab /etc/fstab.bak-2026-11-15
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
sudo systemctl daemon-reload
sudo findmnt --verify            # vérifie la syntaxe de fstab
```

**Vérification**
```bash
swapon --show   # /swapfile file 2G
free -h         # ligne Swap: 2.0Gi
```

---

## Roadmap

- [x] Étape 1 : Remettre nginx-gateway debout + corriger certbot (webroot)
- [x] Nettoyage : supprimer la crontab certbot cassée (doublait le systemd timer)
- [x] Étape 2 : Logs centralisés — rotation Docker configurée, hello-gateway 56M → 0
- [x] Étape 3 : Alerting email/SMS si le site tombe (UptimeRobot)
- [x] Étape 4 : Healthcheck Docker sur nginx-gateway

---

## Reprendre ici — session suivante

**Où on en est (15 nov 2026)**
Roadmap "remise en état" ci-dessus terminée.
Nouveau chantier : pipeline movie-analytics → voir `Infra_learning/infra_roadmap.md`.
Prérequis faits (swap + MySQL coupé), repo cloné. Prochaine étape : Étape 1 — conteneuriser.

**Plan étape 3 — Alerting UptimeRobot :**
- Créer un compte gratuit sur uptimerobot.com
- Ajouter un monitor HTTP sur `https://yannis-devweb.cloud`
- Configurer les notifications email
- Résultat : alerte en < 5 minutes si le site tombe

**Plan étape 4 — Healthcheck Docker :**
- Ajouter un `healthcheck` dans `/home/ubuntu/nginx-gateway/docker-compose.yml`
- Docker vérifie toutes les X secondes que nginx répond bien
- Si unhealthy → Docker tente de redémarrer automatiquement
