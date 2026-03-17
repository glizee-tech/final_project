from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import StringType

import unicodedata
import re
import phonenumbers
import pycountry
from rapidfuzz import process

# ==============================================
# CONTROLE DES ID UNIQUE
# ==============================================

def check_duplicate_id(df, col_name):
    """
    Vérifie qu'une colonne ID ne contient pas de doublons.

    Comportements :
        Toutes les lignes contenant un ID dupliqué sont envoyées
        en quarantaine (aucune n'est conservée).

    Paramètres :
        df : DataFrame Spark
        col_name : str
            Nom de la colonne ID à vérifier

    Retour :
        df_valid, df_quarantine
    """
    window_id = Window.partitionBy(col_name)

    df_with_count = df.withColumn(
        "__duplicate_count__",
        F.count(col_name).over(window_id)
    )

    # lignes valides (id unique)
    df_valid = (
        df_with_count
        .filter(F.col("__duplicate_count__") == 1)
        .drop("__duplicate_count__")
    )

    # lignes en quarantaine (tous les doublons)
    df_quarantine = (
        df_with_count
        .filter(F.col("__duplicate_count__") > 1)
        .drop("__duplicate_count__")
        .withColumn(
            "quarantine_reason",
            F.lit(f"duplicate_{col_name}")
        )
        .withColumn(
            "quarantine_timestamp",
            F.current_timestamp()
        )
    )

    return df_valid, df_quarantine


# ==============================================
# NETTOYAGE DES COLONNES NUMÉRIQUES
# ==============================================

def clean_integer_column(df, col_name, to_delete=False, accept_float=False):
    """
    Nettoie une colonne censée contenir des entiers.

    Comportements :
    - Si accept_float=False :
        Accepte uniquement :
        - 123
        - 123.0
        - 123.00
        Les valeurs invalides deviennent null.
    - Si accept_float=True :
        Accepte toute valeur numérique :
        - 123
        - 123.0
        - 123.45
        - 123.99
        Les valeurs sont arrondies à l'entier le plus proche.

    Paramètres :
        df : DataFrame Spark
        col_name : str
            Nom de la colonne à nettoyer
        to_delete : bool
            - False : conserve les valeurs invalides en null dans la colonne nettoyée
            - True : supprime les lignes invalides et les retourne dans une quarantaine
        accept_float : bool
            - False : n'accepte que les entiers ou les floats finissant par .0 / .00 / ...
            - True : accepte n'importe quel float numérique et l'arrondit

    Retour :
        Si to_delete=False :
            df_cleaned

        Si to_delete=True :
            df_valid, df_quarantine
    """

    col_str = F.trim(F.col(col_name).cast("string"))

    if accept_float:
        # Accepte tout nombre (entier ou décimal), puis arrondit à l'entier
        cleaned_col = F.when(
            col_str.rlike(r"^[0-9]+(\.[0-9]+)?$"),
            F.round(col_str.cast("double")).cast("int")
        ).otherwise(None)
    else:
        # Accepte uniquement les entiers ou les valeurs du type 123.0 / 123.00
        clean_str = F.regexp_replace(col_str, r"\.0+$", "")
        cleaned_col = F.when(
            col_str.rlike(r"^[0-9]+(\.0+)?$"),
            clean_str.cast("int")
        ).otherwise(None)

    # Ajout temporaire de la colonne nettoyée
    df_with_clean = df.withColumn("__cleaned_col__", cleaned_col)

    if not to_delete:
        # Remplace la colonne d'origine par la version nettoyée
        df_cleaned = (
            df_with_clean
            .drop(col_name)
            .withColumnRenamed("__cleaned_col__", col_name)
        )
        return df_cleaned

    # Séparation entre lignes valides et quarantaine
    df_valid = (
        df_with_clean
        .filter(F.col("__cleaned_col__").isNotNull())
        .drop(col_name)
        .withColumnRenamed("__cleaned_col__", col_name)
    )

    df_quarantine = (
        df_with_clean
        .filter(F.col("__cleaned_col__").isNull())
        .drop("__cleaned_col__")
        .withColumn(
            "quarantine_reason",
            F.when(F.col(col_name).isNull(), F.lit(f"missing_{col_name}"))
             .otherwise(F.lit(f"invalid_{col_name}"))
        )
        .withColumn("quarantine_timestamp", F.current_timestamp())
    )

    return df_valid, df_quarantine


# ==============================================
# PARSING DES DATES
# ==============================================

def normalize_date(col_name):
    """
    Parse une colonne Spark contenant des dates stockées en chaîne de caractères
    selon plusieurs formats possibles.

    La fonction :
    - supprime les espaces inutiles
    - caste la colonne en string
    - tente plusieurs formats de date et datetime
    - retourne la première date valide trouvée
    - retourne NULL si aucun format ne correspond

    Formats supportés :
    - yyyy-MM-dd
    - yyyy/MM/dd
    - dd-MM-yyyy
    - yyyy-MM-dd'T'HH:mm:ss
    - et variantes avec mois/jours sans zéro initial

    Paramètres
    ----------
    col_name : str
        Nom de la colonne à parser.

    Retour
    ------
    pyspark.sql.column.Column
        Une colonne Spark de type DATE si parsing réussi, sinon NULL.
    """

    c = F.trim(F.col(col_name).cast("string"))

    date_formats = [
        # Formats ISO avec tirets
        "yyyy-MM-dd",
        "yyyy-M-d",
        "yyyy-M-dd",
        "yyyy-MM-d",

        # Formats avec slash
        "yyyy/MM/dd",
        "yyyy/M/d",
        "yyyy/M/dd",
        "yyyy/MM/d",

        # Formats européens
        "dd-MM-yyyy",
        "d-M-yyyy",
        "d-MM-yyyy",
        "dd-M-yyyy",

        # Formats datetime (on ignore l'heure)
        "yyyy-MM-dd'T'HH:mm:ss",
        "yyyy-M-d'T'HH:mm:ss",
        "yyyy-MM-d'T'HH:mm:ss",
        "yyyy-M-dd'T'HH:mm:ss",
    ]

    parsed_dates = [
        F.to_date(F.try_to_timestamp(c, F.lit(fmt)))
        for fmt in date_formats
    ]

    return F.coalesce(*parsed_dates)


# ==============================================
# GESTION DES DOUBLONS COMPLETS
# ==============================================

def quarantine_full_duplicates(df, quarantine_reason="full_duplicated_line"):
    """
    Supprime les lignes totalement dupliquées en conservant la première occurrence.
    Les doublons supprimés sont envoyés en quarantaine.

    Paramètres :
        df : DataFrame Spark
        quarantine_reason : str
            Raison ajoutée aux lignes en quarantaine

    Retour :
        df_without_duplicates : DataFrame sans doublons complets
        df_quarantine : DataFrame contenant les doublons supprimés avec métadonnées
    """

    window_all = Window.partitionBy(df.columns).orderBy(F.lit(1))

    df_with_rownum = df.withColumn("row_num", F.row_number().over(window_all))

    df_without_duplicates = (
        df_with_rownum
        .filter(F.col("row_num") == 1)
        .drop("row_num")
    )

    df_quarantine = (
        df_with_rownum
        .filter(F.col("row_num") > 1)
        .drop("row_num")
        .withColumn("quarantine_reason", F.lit(quarantine_reason))
        .withColumn("quarantine_timestamp", F.current_timestamp())
    )

    return df_without_duplicates, df_quarantine


# ==============================================
# VALIDATION DE L'ORDRE DES DATES
# ==============================================

def validate_date_order(df, date_late_col, date_early_col, primary_key):
    """
    Conserve les lignes où date_late_col >= date_early_col
    (si aucune des deux dates n'est null).

    Les lignes invalides sont envoyées en quarantaine.

    Paramètres :
        df : DataFrame Spark
        date_late_col : str
            Colonne censée être la plus tardive
        date_early_col : str
            Colonne censée être la plus ancienne
        primary_key : str
            Clé primaire de la table

    Retour :
        df_valid : lignes valides
        df_quarantine : lignes invalides avec métadonnées de quarantaine
    """

    # Lignes valides
    df_valid = df.filter(
        (F.col(date_late_col).isNull()) |
        (F.col(date_early_col).isNull()) |
        (F.col(date_late_col) >= F.col(date_early_col))
    )

    # Lignes invalides
    df_quarantine = df.join(
        df_valid.select(primary_key),
        on=primary_key,
        how="left_anti"
    ).withColumn(
        "quarantine_reason",
        F.lit(f"{date_late_col}_before_{date_early_col}")
    ).withColumn(
        "quarantine_timestamp",
        F.current_timestamp()
    )

    return df_valid, df_quarantine


# ==============================================
# VALIDATION DES CLÉS ÉTRANGÈRES
# ==============================================

def validate_foreign_key(df_main, df_reference, fk_col, pk_col):
    """
    Vérifie qu'une clé étrangère d'une table principale existe bien
    dans la table de référence.

    Les lignes avec clé étrangère invalide sont envoyées en quarantaine.

    Paramètres :
        df_main : DataFrame Spark principal
        df_reference : DataFrame Spark de référence
        fk_col : str
            Nom de la clé étrangère dans df_main
        pk_col : str
            Nom de la clé primaire dans df_reference

    Retour :
        df_valid : lignes avec clé étrangère valide
        df_quarantine : lignes invalides avec métadonnées
    """

    # Lignes valides
    df_valid = df_main.join(
        df_reference.select(pk_col).distinct().withColumnRenamed(pk_col, fk_col),
        on=fk_col,
        how="inner"
    )

    # Lignes invalides
    df_quarantine = df_main.join(
        df_reference.select(pk_col).distinct().withColumnRenamed(pk_col, fk_col),
        on=fk_col,
        how="left_anti"
    ).withColumn(
        "quarantine_reason",
        F.lit(f"{fk_col}_not_in_reference")
    ).withColumn(
        "quarantine_timestamp",
        F.current_timestamp()
    )

    return df_valid, df_quarantine


# ==============================================
# RAPPORT DES VALEURS MANQUANTES
# ==============================================

def missing_values_report(df):
    """
    Retourne un DataFrame au format long avec le nombre de valeurs manquantes
    pour chaque colonne.

    Sont considérées comme manquantes :
    - null
    - chaîne vide ""
    - "nan", "none", "null" (insensible à la casse)

    Paramètres :
        df : DataFrame Spark

    Retour :
        df_missing : DataFrame Spark avec :
            - column_name
            - missing_count
    """

    dfs = []

    for c in df.columns:
        df_col = df.select(
            F.lit(c).alias("column_name"),
            F.sum(
                F.when(
                    F.col(c).isNull() |
                    (F.trim(F.col(c).cast("string")) == "") |
                    (F.lower(F.trim(F.col(c).cast("string"))).isin("nan", "none", "null")),
                    1
                ).otherwise(0)
            ).alias("missing_count")
        )
        dfs.append(df_col)

    df_missing = dfs[0]
    for d in dfs[1:]:
        df_missing = df_missing.unionByName(d)

    return df_missing


# ==============================================
# NORMALISATION DES CHAÎNES DE CARACTÈRES
# ==============================================

def normalize_string(value):
    """
    Nettoie et normalise une chaîne de caractères :
    - supprime les espaces en début/fin
    - supprime les accents
    - remplace les caractères spéciaux par des espaces (sauf - et ')
    - compresse les espaces multiples
    - applique le title case

    Retourne None si la valeur est vide ou None.
    """

    if value is None:
        return None

    value = str(value).strip()
    if value == "":
        return None

    # Suppression des accents
    value = unicodedata.normalize("NFKD", value).encode("ASCII", "ignore").decode("utf-8")

    # Remplacement des caractères spéciaux
    value = re.sub(r"[^A-Za-z0-9\s\-']", " ", value)

    # Compression des espaces
    value = re.sub(r"\s+", " ", value).strip()

    if value == "":
        return None

    return value.title()


def normalize_phone_with_country(phone, country_code):
    """
    Normalise un numéro de téléphone au format international E.164
    en utilisant le code pays fourni.

    Retourne None si :
    - le numéro est vide
    - le code pays est absent
    - le numéro est invalide
    """

    if phone is None or country_code is None:
        return None

    phone = str(phone).strip()
    if phone == "":
        return None

    try:
        parsed = phonenumbers.parse(phone, country_code)
        if phonenumbers.is_possible_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return None

    return None


def normalize_country_code(country):
    """
    Convertit un nom de pays en code ISO alpha-2.

    Exemple :
    - France -> FR
    - Germany -> DE

    Retourne None si le pays est inconnu ou non résolvable.
    """

    if country is None:
        return None

    country = str(country).strip()
    if country == "":
        return None

    try:
        return pycountry.countries.lookup(country).alpha_2
    except Exception:
        return None


# UDF Spark pour utilisation dans les transformations DataFrame
normalize_string_udf = F.udf(normalize_string, StringType())
normalize_phone_with_country_udf = F.udf(normalize_phone_with_country, StringType())
normalize_country_code_udf = F.udf(normalize_country_code, StringType())


# ==============================================
# CORRECTION PAR FRÉQUENCE (VILLE / PAYS)
# ==============================================

def build_freq_corrector(spark_df, col_name, min_count=2, threshold=85):
    """
    Construit dynamiquement une UDF Spark de correction orthographique
    basée sur la similarité floue (RapidFuzz).

    Principe :
      1. Récupère les valeurs fréquentes de la colonne comme références.
      2. Pour chaque valeur, cherche la référence la plus proche.
      3. Si le score dépasse le seuil, remplace par la référence.
         Sinon, conserve la valeur d'origine.

    Paramètres :
      spark_df : DataFrame Spark source
      col_name : str
          Nom de la colonne à corriger
      min_count : int
          Nombre minimum d'occurrences pour devenir une valeur de référence
      threshold : int
          Score minimum de similarité pour appliquer la correction

    Retour :
      Une UDF Spark prête à être utilisée.
    """

    # Collecte des valeurs fréquentes côté driver
    reference_values = [
        row[col_name]
        for row in spark_df.groupBy(col_name)
            .count()
            .filter(F.col(col_name).isNotNull() & (F.col("count") > min_count))
            .collect()
    ]

    def corrector(value):
        if value is None or not reference_values:
            return value

        match = process.extractOne(value, reference_values)
        return match[0] if match and match[1] > threshold else value

    return F.udf(corrector, StringType())


def cast_all_columns_to_string(df):
    """
    Cast toutes les colonnes d'un DataFrame Spark en string.
    """
    return df.select([F.col(c).cast("string").alias(c) for c in df.columns])
