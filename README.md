# Fondements et Applications du Big Data — TP

Auteur : Antoine Chiausa

Un dossier par TP. Chaque TP a un notebook principal, déjà exécuté avec ses sorties, et les fichiers qu'il utilise ou produit.

| TP | Dossier | Sujet | Outils |
|---|---|---|---|
| 1 | `TP1/` | Comptage de mots sur des fichiers de 0,6 Ko à 2,4 Go | Python, Hadoop MapReduce, PySpark |
| 2 | `TP2/` | Analyse d'accidents de la route, d'après un article Medium | PySpark (RDD, DataFrame, SparkSQL) |
| 3 | `TP3_ID7/Small/` | Prédiction des troubles du sommeil, de l'EDA au déploiement | pandas, scikit-learn, FastAPI, Docker |
| 3 | `TP3_ID7/Big/` | Classification de maladies du riz sur 1 Go d'images | PySpark, TensorFlow |

## Environnement

- Python 3.14 dans un environnement virtuel `.venv` (non versionné) :
  `pip install pyspark pyarrow pandas matplotlib seaborn psutil pillow scikit-learn joblib tensorflow ipykernel`
- Java 17 pour Spark et Hadoop. Spark 4 ne démarre pas avec Java 27, les notebooks Spark sélectionnent donc Java 17 avec `/usr/libexec/java_home -v 17`.
- TP1 : Hadoop 3.5.0, téléchargé par le notebook dans `~/hadoop-3.5.0`.
- TP3 Small : Docker Desktop, qui doit être lancé pour la partie déploiement.

Les dossiers `data/` ne sont pas versionnés. Les notebooks génèrent ou téléchargent eux-mêmes les données qui manquent.

## TP1 — Comptage de mots : Python, Hadoop MapReduce et PySpark

Dossier `TP1/`.

On compte les mots d'un fichier avec cinq méthodes, sur trois tailles de fichier (0,6 Ko, 6 Mo et 2,4 Go), et on compare les temps d'exécution. Hadoop tourne en mode pseudo-distribué (HDFS et YARN sur une seule machine) et Spark en `local[*]`.

- `TP1_comptage_mots.ipynb` : installation et configuration de Hadoop, génération des fichiers, mesures et graphique de synthèse
- `scripts/` : les quatre scripts comparés, à savoir Python simple, pseudo-MapReduce, PySpark RDD et PySpark DataFrame (la cinquième méthode est le job `wordcount` fourni avec Hadoop)
- `resultats/` : sortie de chaque méthode sur chaque fichier, et `mesures.json` avec tous les temps
- `logs/` : journaux des jobs Hadoop et Spark
- `YARN_console.png`, `SPARK_console.png` : captures des consoles après exécution

Sur les petits fichiers, Python répond en moins d'une demi-seconde, contre environ 7 s pour PySpark et 15 s pour Hadoop, à cause de leur temps de démarrage. Sur le fichier de 2,4 Go, PySpark DataFrame est le plus rapide (49 s), devant Hadoop MapReduce (84 s), PySpark RDD (106 s) et Python simple (192 s). Le pseudo-MapReduce en Python n'aboutit pas.

## TP2 — Analyse des accidents de la route avec PySpark

Dossier `TP2/`.

On reproduit l'article [Big Data Analysis of Road Crash Data using PySpark](https://medium.com/rakesh-nain/big-data-analysis-of-road-crash-data-using-pyspark-with-pyspark-tutorial-c78ff2c35588) de Rakesh Nain, sur les accidents en Australie-Méridionale de 2012 à 2019. Le code de l'article est repris sans modification.

- `TP2_etude_article_medium.ipynb` : le notebook commenté, avec les requêtes en RDD puis en DataFrame, et deux requêtes écrites avec les trois API (RDD, DataFrame, SparkSQL)
- `Big-data-Analysis-of-Road-Crash-Data-using-PySpark-with-PySpark-Tutorial/` : copie du dépôt GitHub de l'article, avec ses notebooks et les données (`crash_data/`, `units_data/`, `data for given tutorials/`, description des colonnes dans `metadata-for-road-crash.pdf`)
- `severe.png`, `severe_per.png` : gravité des accidents selon l'alcool et les drogues
- `console_SPARK.png` : capture de la console Spark

## TP3 — Chaîne complète de machine learning

Dossier `TP3_ID7/`, avec deux sous-dossiers `Small/` et `Big/`.

Le TP suit toute la chaîne (données, EDA, pré-traitement, transformation, modèle, évaluation, déploiement) sur deux jeux de données. Dans les deux cas, on part d'un notebook Kaggle, gardé à côté du notebook final.

### Small — Troubles du sommeil

Dossier `TP3_ID7/Small/`.

Jeu de données [Sleep Health and Lifestyle](https://www.kaggle.com/datasets/uom190346a/sleep-health-and-lifestyle-dataset) : 374 personnes, 13 colonnes. On prédit le trouble du sommeil (`None`, `Insomnia` ou `Sleep Apnea`).

- `TP3_sleep_health_ML.ipynb` : le notebook principal. L'EDA est celle du notebook Kaggle. Viennent ensuite le pré-traitement, l'encodage, une régression logistique, un arbre de décision, les matrices de confusion et le déploiement.
- `sleep-health-lifestyle-dataset.ipynb` : le notebook Kaggle de départ
- `deploiement/` : fichiers générés par le notebook, à savoir le modèle (`modele.joblib`), l'API FastAPI (`app.py`), le `Dockerfile` et le `requirements.txt`

Après suppression des doublons, il ne reste que 132 lignes. L'arbre de décision obtient 67 % de bonnes prédictions sur le test et la régression logistique 64 %. L'API tourne dans un conteneur Docker et renvoie la même prédiction que le notebook.

### Big — Maladies du riz

Dossier `TP3_ID7/Big/`.

Deux datasets Kaggle d'images de feuilles de riz, [Rice Leaf Diseases](https://www.kaggle.com/datasets/ngivitgccychui/rice-leaf-diseases-dataset-vbookshelf) et [Rice Disease Dataset](https://www.kaggle.com/datasets/anshulm257/rice-disease-dataset), soit 3 949 images et 1 Go. On classe les images en six catégories, cinq maladies et les feuilles saines.

- `TP3_rice_disease_big.ipynb` : le notebook principal. L'EDA, le pré-traitement et la transformation (redimensionnement en 300 × 300) sont faits avec Spark. Le modèle EfficientNetB0, l'évaluation et la sauvegarde sont repris du notebook Kaggle.
- `rice-disease-dataset.ipynb` : le notebook Kaggle de départ
- `rice_model.keras` : le modèle entraîné, généré par le notebook

Le notebook télécharge les deux datasets dans `data/` s'ils manquent, soit environ 1,1 Go. La partie Spark prend environ une minute ; l'entraînement est beaucoup plus long. Spark garde 3 878 images (les doublons et la classe `Leaf smut` sont retirés), qui ne pèsent plus que 130 Mo une fois redimensionnées. Le modèle atteint 89 % d'accuracy sur la validation.
