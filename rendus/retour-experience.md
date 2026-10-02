# Retour d’expérience : StreamVault

**Projet :** StreamVault, pipeline de données de bout en bout (Azure Data Factory, ADLS Gen2, Databricks/PySpark, MongoDB local, Azure Event Hub, Spark Structured Streaming, ontologie RDF/OWL, dataviz)

**Cadre :** Brief 2, Formation Data Engineer (RNCP 40167), Simplon.co
**Auteur :** R. P. ([TheRealRPI](https://github.com/TheRealRPI))
**Date :** 2 octobre 2026

**Objet :** Ce rapport documente les difficultés rencontrées lors de la construction du pipeline et la manière dont elles ont été résolues.

## Synthèse

| # | Difficulté | Résolution |
|---|------------|------------|
| 1 | Compréhension du Self-Hosted Integration Runtime (ADF vers MongoDB local) | Lecture du principe : un exécutable à installer + une chaîne de connexion ; mise en place immédiate une fois le concept assimilé |
| 2 | Activités Copy Data à la configuration peu intuitive | Paramétrage à tâtons ; les upserts vers MongoDB ont ensuite fonctionné sans accroc |
| 3 | Alertes e-mail : configuration des déclencheurs et Logic Apps peu clairs pour une première approche | Mise en place aboutie : Logic Apps retenu dans la version finale pour sa puissance et sa polyvalence |
| 4 | Cluster Databricks mutualisé entre quatre apprenants (budget de l’abonnement) : surcharges CPU et timeouts Python lors des exécutions simultanées | Ajustement des paramètres du cluster pour un fonctionnement optimal en contexte partagé |
| 5 | Années aberrantes ou parasites dans les données sources (movies) | Nettoyage par bornage et extraction des 4 premiers chiffres |
| 6 | `spark.stop()` qui bouclait à l’infini dans plusieurs notebooks | Contournement par réorganisation du code |
| 7 | Documentation Azure dispersée et parfois obsolète | Croisement de plusieurs sources avant d’appliquer une configuration |
| 8 | Débogage d’une architecture répartie cloud/local | Approche incrémentale : valider chaque brique isolément avant de les chaîner |

## 1. Orchestration : Azure Data Factory

### 1.1 Self-Hosted Integration Runtime

Le pipeline devait écrire dans un MongoDB hébergé en local, ce qui impose de faire dialoguer Azure Data Factory (cloud) avec la machine hôte via un Self-Hosted Integration Runtime. La principale difficulté a été de **comprendre le fonctionnement** de ce runtime : sa documentation laisse présager un mécanisme complexe, alors qu’il suffit d’installer un exécutable sur la machine locale et d’y renseigner la chaîne de connexion.

La difficulté relevait donc davantage de l’appréhension de l’inconnu que de la technique : une fois le principe assimilé (une passerelle locale qui relaie les échanges entre le cloud et le réseau local), l’installation et la configuration se sont faites sans obstacle.

### 1.2 Activités Copy Data

La configuration des activités Copy Data s’est révélée **peu logique et peu explicite** (rôle des différents onglets, mapping des champs, comportement d’écriture). Le paramétrage s’est fait à tâtons, par essais et erreurs. Une fois correctement configurées, les opérations d’upsert vers MongoDB via le runtime auto-hébergé se sont déroulées sans difficulté.

### 1.3 Alertes e-mail : mise en place de Logic Apps

La configuration des déclencheurs de notification de succès/échec du pipeline, puis la mise en place de **Logic Apps**, ont été **complexes et confuses pour une première approche** : l’outil s’est avéré peu clair à la prise en main, alors que l’alerting natif d’Azure Data Factory aurait répondu plus simplement au besoin.

C’est toutefois **Logic Apps qui a été retenu dans la version finale** : malgré une courbe d’apprentissage plus élevée, l’outil s’est révélé plus puissant et polyvalent, et sa mise en place aboutie couvre désormais les notifications de succès et d’échec du pipeline.

## 2. Contraintes de coûts et dimensionnement Databricks

L’abonnement Azure fourni dans le cadre de la formation impose des **conditions de consommation strictes**. Le cluster Databricks, déjà dimensionné au plus petit profil disponible, a dû être **mutualisé entre quatre apprenants** pour rester dans le budget.

Cette mutualisation a eu un coût opérationnel : lorsque plusieurs apprenants lançaient leur pipeline en même temps, le cluster subissait des **surcharges CPU**, se traduisant le plus souvent par des **timeouts Python**. Les paramètres du cluster ont été ajustés pour un fonctionnement optimal en contexte partagé, mais la ressource reste sensible aux exécutions massivement simultanées.

Cette contrainte a directement influencé l’architecture du projet : les arbitrages de coûts ne sont pas un sujet annexe ; ils orientent les choix techniques, ici jusqu’à la manière de partager une même ressource de calcul.

## 3. Qualité des données sources

Les données sources (notamment les années des films dans `movies.json`) contenaient des **valeurs aberrantes ou des caractères parasites** rendant les colonnes inexploitables en l’état pour des jointures ou des graphiques.

**Résolution :** nettoyage par bornage des valeurs et extraction des 4 premiers chiffres, appliqué dans les notebooks d’ingestion avant écriture en zone `clean`.

## 4. Environnement d’exécution Databricks

Dans plusieurs notebooks, l’appel `spark.stop()` **ne terminait jamais** : la session tournait en boucle sans libérer les ressources. Le problème, non élucidé à la racine, a été **contourné par réorganisation du code** afin de ne pas dépendre de cet arrêt explicite.

## 5. Difficultés transverses

- **Documentation Azure** : dispersée, parfois obsolète ou trop verbeuse. Il a fallu croiser plusieurs sources (documentation officielle, tutoriels, retours de la communauté) avant d’appliquer une configuration.
- **Débogage d’une architecture répartie** : quand une exécution échoue, identifier la brique fautive (cloud ou local) est difficile. La parade a été de construire le pipeline **brique par brique** : valider chaque composant isolément (upload ADLS, notebook, upsert, flux Event Hub) avant de les chaîner. C’est cette méthode qui a permis au volet streaming de se monter sans incident notable.
- **Volume du projet** : le nombre de briques à assembler dans un temps limité (ADF, ADLS, Databricks, MongoDB, Event Hub, streaming, ontologie, dataviz) a rendu indispensable cette décomposition incrémentale.

## 6. Enseignements retenus

1. **Approche incrémentale** : monter et valider chaque brique séparément avant l’assemblage. Indispensable sur un pipeline distribué où une panne peut venir de n’importe quel maillon.
2. **Comprendre avant de configurer** : la difficulté la plus intimidante (Self-Hosted IR) s’est dissoute à la lecture du principe. Un blocage apparent est souvent un déficit de compréhension, pas un problème technique.
3. **Les coûts orientent l’architecture** : les contraintes de l’abonnement ont imposé un cluster mutualisé entre quatre apprenants, avec son corollaire opérationnel (contentions CPU et timeouts Python lors des exécutions simultanées). Le dimensionnement et le budget font partie de la conception, et non d’un sujet traité en fin de projet.
4. **Adapter l’outil au besoin** : Metabase retenu là où Power BI ne se connectait pas nativement à MongoDB (simplicité gagnante) ; à l’inverse, Logic Apps retenu malgré sa complexité, car sa puissance et sa polyvalence justifiaient l’investissement. Le bon outil est celui dont le rapport capacités/effort correspond au besoin.
