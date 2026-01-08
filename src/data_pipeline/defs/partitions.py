# -----------------------------------------------------------
# Dagster Decade Partitions for Querying Wikidata in Parts
# Dagster Data pipeline for Structured and Unstructured Data
#
# (C) 2025-2026 Juan-Francisco Reyes, Cottbus, Germany
# Released under MIT License
# email pacoreyes@protonmail.com
# -----------------------------------------------------------

from dagster import StaticPartitionsDefinition

DECADES_TO_EXTRACT = {
    "1930s": (1930, 1939),
    "1940s": (1940, 1949),
    "1950s": (1950, 1959),
    "1960s": (1960, 1969),
    "1970s": (1970, 1979),
    "1980s": (1980, 1989),
    "1990s": (1990, 1999),
    "2000s": (2000, 2009),
    "2010s": (2010, 2019),
    "2020s": (2020, 2029),
}

decade_partitions = StaticPartitionsDefinition(list(DECADES_TO_EXTRACT.keys()))
