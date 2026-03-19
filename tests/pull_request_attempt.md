# Process Classique

git pull git checkout -b feature (on sort de main pour développer sa feature)
feature terminares
git pull
git merge main (depuis notre branche feature pour se mettre à jour de la main
git checkout main (retourne dans la main pour préparer le merge final
git merge feature
(puis push de la branche)


# Process avec pull request

### 1. On crée et part sur la branche feature
git checkout -b feature

## --- développement ---

### 2. On se met à jour par rapport à main (depuis feature)
git fetch origin
git merge origin/main  # ou git rebase origin/main

### 3. On push la branche feature sur le remote
git push origin feature

### 4. → Sur GitHub/GitLab : on ouvre une Pull Request  feature → main
####    (review, commentaires, approbation par les collègues)

### 5. → Le merge dans main se fait via l'interface (bouton "Merge PR")
####    PAS en local avec git merge feature

### 6. On récupère main à jour en local
git checkout main
git pull origin main

### 7. Nettoyage
git branch -d feature