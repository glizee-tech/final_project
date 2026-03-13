import unicodedata
import re
import phonenumbers
import pycountry
from rapidfuzz import process

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import StringType


# ==============================================
# NORMALISATION DES CHAINES DE CARACTERES
# ==============================================

def normalize_string(value):
    """
    Nettoie et normalise une chaîne de caractères :
    - Supprime les espaces en début/fin
    - Supprime les accents (NFD → ASCII)
    - Remplace les caractères spéciaux par des espaces (sauf - et ')
    - Compresse les espaces multiples
    - Applique le titlecase (ex: "new york" → "New York")
    Retourne None si la valeur est vide ou None.
    """
    if value is None:
        return None

    value = str(value).strip()
    if value == "":
        return None

    # Suppression des accents via normalisation Unicode NFD
    value = unicodedata.normalize("NFKD", value).encode("ASCII", "ignore").decode("utf-8")

    # Remplacement des caractères spéciaux par un espace (conserve lettres, chiffres, -, ')
    value = re.sub(r"[^A-Za-z0-9\s\-']", " ", value)

    # Compression des espaces multiples
    value = re.sub(r"\s+", " ", value).strip()

    if value == "":
        return None

    return value.title()


def normalize_phone_with_country(phone, country_code):
    """
    Normalise un numéro de téléphone au format international E.164
    (ex: +33612345678) en utilisant le country_code comme région.
    
    Retourne None si :
    - Le numéro est absent ou vide
    - Le country_code est absent (impossible de valider sans contexte pays)
    - Le numéro est invalide pour le pays donné
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
    Convertit un nom de pays en code ISO alpha-2
    (ex: "France" → "FR", "Germany" → "DE").
    Utilise pycountry pour la résolution.
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


# UDFs Spark — wrappers PySpark pour utilisation dans les transformations DataFrame
normalize_string_udf             = F.udf(normalize_string, StringType())
normalize_phone_with_country_udf = F.udf(normalize_phone_with_country, StringType())
normalize_country_code_udf       = F.udf(normalize_country_code, StringType())


# ==============================================
# PARSING DES DATES
# ==============================================

def parse_date_col(col_name):
    """
    Tente de parser une colonne date selon une liste de formats courants,
    dans l'ordre de priorité suivant :
      - yyyy-MM-dd / yyyy-M-d (et variantes)
      - yyyy/MM/dd / yyyy/M/d (et variantes)
      - dd-MM-yyyy / d-M-yyyy (et variantes)
      - yyyy-MM-dd'T'HH:mm:ss (format ISO 8601, et variantes)
    Retourne la première correspondance via F.coalesce, ou null si aucun format ne match.
    """
    c = F.trim(F.col(col_name).cast("string"))

    return F.coalesce(
        # Formats yyyy-MM-dd et variantes jour/mois sans zéro
        F.to_date(F.try_to_timestamp(c, F.lit("yyyy-MM-dd"))),
        F.to_date(F.try_to_timestamp(c, F.lit("yyyy-M-d"))),
        F.to_date(F.try_to_timestamp(c, F.lit("yyyy-M-dd"))),
        F.to_date(F.try_to_timestamp(c, F.lit("yyyy-MM-d"))),

        # Formats avec séparateur slash
        F.to_date(F.try_to_timestamp(c, F.lit("yyyy/MM/dd"))),
        F.to_date(F.try_to_timestamp(c, F.lit("yyyy/M/d"))),
        F.to_date(F.try_to_timestamp(c, F.lit("yyyy/M/dd"))),
        F.to_date(F.try_to_timestamp(c, F.lit("yyyy/MM/d"))),

        # Formats dd-MM-yyyy (style européen)
        F.to_date(F.try_to_timestamp(c, F.lit("dd-MM-yyyy"))),
        F.to_date(F.try_to_timestamp(c, F.lit("d-M-yyyy"))),
        F.to_date(F.try_to_timestamp(c, F.lit("d-MM-yyyy"))),
        F.to_date(F.try_to_timestamp(c, F.lit("dd-M-yyyy"))),

        # Formats ISO 8601 avec heure (on extrait uniquement la date)
        F.to_date(F.try_to_timestamp(c, F.lit("yyyy-MM-dd'T'HH:mm:ss"))),
        F.to_date(F.try_to_timestamp(c, F.lit("yyyy-M-d'T'HH:mm:ss"))),
        F.to_date(F.try_to_timestamp(c, F.lit("yyyy-MM-d'T'HH:mm:ss"))),
        F.to_date(F.try_to_timestamp(c, F.lit("yyyy-M-dd'T'HH:mm:ss"))),
    )


# ==============================================
# CORRECTION PAR FREQUENCE (city / country)
# ==============================================

def build_freq_corrector(spark_df, col_name, min_count=2, threshold=85):
    """
    Construit dynamiquement une UDF Spark de correction orthographique
    basée sur la similarité floue (RapidFuzz).

    Principe :
      1. Collecte les valeurs les plus fréquentes de la colonne (> min_count occurrences)
         comme valeurs de référence.
      2. Pour chaque valeur à corriger, cherche la correspondance la plus proche
         parmi les références via un score de similarité (0-100).
      3. Si le meilleur score dépasse le seuil (threshold), remplace par la référence.
         Sinon, conserve la valeur originale.

    Paramètres :
      spark_df  : DataFrame source pour construire les références
      col_name  : nom de la colonne à corriger (ex: "city", "country")
      min_count : seuil minimum d'occurrences pour être une valeur de référence (défaut: 2)
      threshold : score de similarité minimum pour accepter une correction (défaut: 85)

    Retourne une UDF Spark prête à l'emploi.
    """
    # Collecte des valeurs fréquentes côté driver (dataset petit)
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
