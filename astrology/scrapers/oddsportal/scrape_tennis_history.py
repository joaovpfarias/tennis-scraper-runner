# Script: historico completo de tenis do oddsagora.com.br
# Rodar: C:\Users\Dell\anaconda3\python.exe astrology\scrapers\oddsportal\scrape_tennis_history.py
# Saida:  astrology/scrapers/oddsportal/data/raw/tennis_history.db
#
# Fase 1 (discovery): busca /tennis/results/ paginado e extrai todos os slugs de torneio ja disputados
# Fase 2 (scraping):  para cada torneio, raspa temporada atual + sufixos YYYY-YYYY / YYYY
# Fase 3 (paralelo):  torneios rodam em paralelo (PARALLEL_LEAGUES ao mesmo tempo)

import asyncio
import gzip
import json
import os
import re
import sys
import time as _time
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from astrology.scrapers.oddsportal.browser import OddsPortalBrowser
from astrology.scrapers.oddsportal import cache as cache_mod
from astrology.scrapers.oddsportal import url_builder
from astrology.scrapers.oddsportal.normalizer import stable_event_id
from astrology.scrapers.oddsportal.output_writer import SQLiteWriter
from astrology.scrapers.oddsportal.parsers import (
    results_listing, match_header, match_tabs,
)
from astrology.scrapers.oddsportal.market_catalog import get_market
import importlib
import sqlite3
import time
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Sharding (GitHub Actions matrix): SHARD_ID/TOTAL_SHARDS dividem a lista de torneios.
SHARD_ID      = int(os.environ.get("SHARD_ID", "0"))
TOTAL_SHARDS  = int(os.environ.get("TOTAL_SHARDS", "1"))
DISCOVER_ONLY    = os.environ.get("DISCOVER_ONLY", "0") == "1"
PRIORITY_INVERT = os.environ.get("PRIORITY_INVERT", "0") == "1"  # shards 7-9: ITF-first
DEBUG_LEAGUES   = os.environ.get("DEBUG_LEAGUES", "0") == "1"   # roda apenas 5 ligas tier 0/1 localmente

# Graceful shutdown: para de iniciar trabalho novo apos TIME_BUDGET_MIN minutos
# (0 = sem limite). Evita ser morto pelo timeout-minutes do Actions com trabalho
# in-flight; o shard encerra limpo e grava STATUS_PATH com exhausted=false.
TIME_BUDGET_MIN = float(os.environ.get("TIME_BUDGET_MIN", "0") or 0)
STATUS_PATH     = os.environ.get("STATUS_PATH", "shard_status.json")
_START_MONOTONIC = time.monotonic()
_BUDGET = {"exceeded": False}


def _budget_exceeded() -> bool:
    if TIME_BUDGET_MIN <= 0:
        return False
    if time.monotonic() - _START_MONOTONIC > TIME_BUDGET_MIN * 60:
        if not _BUDGET["exceeded"]:
            print(f"\n[budget] TIME_BUDGET_MIN={TIME_BUDGET_MIN:.0f} atingido — encerrando sem iniciar trabalho novo.")
        _BUDGET["exceeded"] = True
        return True
    return False
DB_PATH       = os.environ.get("DB_PATH_OVERRIDE") or str(Path(__file__).parent / "data" / "raw" / "tennis_history.db")
BASE_URL      = "https://www.oddsagora.com.br"
SPORT_KEY     = "tennis"
SPORT_SLUG    = "tennis"
MARKETS       = [
    "home_away",       # Home/Away — moneyline principal
    "over_under",      # Acima/Abaixo — total de games/sets
    "asian_handicap",  # Handicap Asiatico
    "placar_exato",    # Placar exato — 2:0 / 2:1 (sets) — especifico de tenis
    "total_sets",      # Total de Sets — quando disponivel (depende do torneio)
    "total_games",     # Total de Games — quando disponivel
]

# Anos para tentar com sufixo de season - ordem REVERSA (mais recente primeiro)
# Combinado com early-stop: torneios discontinuados nao gastam 16 fetches.
SEASON_YEARS  = list(range(2026, 1997, -1))   # 2026, 2025, ..., 1998
SEASON_SUFFIXES = [None] + [str(y) for y in SEASON_YEARS]

EARLY_STOP_THRESHOLD = 3     # vazios consecutivos para disparar o probe
PROBE_CUTOFF         = 2021  # só dispara se ainda estamos acima desse ano
ANCHOR_YEARS         = [2020, 2019, 2018, 2017, 2016, 2015, 2014, 2013, 2012]

_CUR_YEAR = datetime.now(timezone.utc).year


def _coalesce_score(primary, fallback):
    """Mantem o valor do header preferindo o primario, MAS preserva 0.
    BUG anterior: `header.get('score_home') or m.get('score_home')` descartava
    o 0 (set count do perdedor em sets diretos = falsy) -> placar virava NULL e
    o evento ficava 'finished' sem score. Atingia ~20k jogos."""
    return primary if primary not in (None, "") else fallback


def _season_is_final(suffix) -> bool:
    """True so para seasons PASSADAS (ano < ano atual): finalizadas, nao mudam mais.
    A season atual (None = feed ao vivo) e o ano corrente NUNCA viram cache."""
    if suffix is None:
        return False
    try:
        return int(suffix) < _CUR_YEAR
    except (TypeError, ValueError):
        return False

PARALLEL_LEAGUES  = 4   # ligas em paralelo por shard (4 ligas × 7 matches = 28 pages total)
PARALLEL_MATCHES  = 7   # matches em paralelo por liga — semaforo GLOBAL (compartilhado entre ligas)
BROWSER_POOL      = 28  # paginas Chromium no pool
USE_CACHE         = True
PAGE_FULL         = 40  # se a pg1 trouxe >= isso, provavelmente ha mais paginas
MAX_RESULT_PAGES  = 25  # teto de paginas de resultado por season
DISCOVERY_MAX_PAGES = 150       # paginas maximas a varrer na fase de discovery
DISCOVERY_CACHE_FILE = str(Path(__file__).parent / "data" / "raw" / "discovered_tennis_leagues.json")
DISCOVERY_CACHE_TTL  = 30 * 24 * 3600  # 30 dias

# ---------------------------------------------------------------------------
# Venue derivation (country/city from league slug — site nao fornece diretamente)
# ---------------------------------------------------------------------------

LEAGUE_CITY_OVERRIDES: dict[str, str] = {
    # Grand Slams
    "france/atp-aberto-da-franca":              "Paris",
    "france/wta-aberto-da-franca":              "Paris",
    "australia/atp-australian-open":            "Melbourne",
    "australia/wta-australian-open":            "Melbourne",
    "united-kingdom/atp-wimbledon":             "Wimbledon",
    "united-kingdom/wta-wimbledon":             "Wimbledon",
    "usa/atp-us-open":                          "New York",
    "usa/wta-us-open":                          "New York",
    # Masters 1000 / WTA 1000
    "usa/atp-indian-wells":                     "Indian Wells",
    "usa/wta-indian-wells":                     "Indian Wells",
    "usa/atp-miami":                            "Miami",
    "usa/wta-miami":                            "Miami",
    "usa/atp-cincinnati":                       "Cincinnati",
    "usa/wta-cincinnati":                       "Cincinnati",
    "monaco/atp-monte-carlo":                   "Monte Carlo",
    "spain/atp-madri":                          "Madrid",
    "spain/wta-madri":                          "Madrid",
    "italy/atp-roma":                           "Rome",
    "italy/wta-roma":                           "Rome",
    "canada/atp-toronto":                       "Toronto",
    "canada/atp-montreal":                      "Montreal",
    "canada/wta-toronto":                       "Toronto",
    "canada/wta-montreal":                      "Montreal",
    "china/atp-xangai":                         "Shanghai",
    "china/wta-wuhan":                          "Wuhan",
    "china/wta-beijing":                        "Beijing",
    "france/atp-paris":                         "Paris",
    # ATP 500 / WTA 500
    "netherlands/atp-roterda":                  "Rotterdam",
    "united-arab-emirates/atp-dubai":           "Dubai",
    "united-arab-emirates/wta-dubai":           "Dubai",
    "mexico/atp-acapulco":                      "Acapulco",
    "spain/atp-barcelona":                      "Barcelona",
    "germany/atp-hamburgo":                     "Hamburg",
    "germany/atp-halle":                        "Halle",
    "germany/atp-munique":                      "Munich",
    "germany/atp-stuttgart":                    "Stuttgart",
    "usa/atp-washington":                       "Washington",
    "usa/atp-houston":                          "Houston",
    "usa/wta-washington":                       "Washington",
    "usa/atp-new-haven":                        "New Haven",
    "usa/wta-new-haven":                        "New Haven",
    "japan/atp-tokyo":                          "Tokyo",
    "japan/wta-tokyo":                          "Tokyo",
    "china/atp-beijing":                        "Beijing",
    "switzerland/atp-basel":                    "Basel",
    "switzerland/atp-genebra":                  "Geneva",
    "austria/atp-viena":                        "Vienna",
    "qatar/atp-doha":                           "Doha",
    "qatar/wta-doha":                           "Doha",
    "usa/wta-charleston":                       "Charleston",
    "germany/wta-stuttgart":                    "Stuttgart",
    "russia/wta-moscou":                        "Moscow",
    # ATP 250 / WTA 250+
    "russia/atp-moscou":                        "Moscow",
    "russia/atp-sao-petersburgo":               "St. Petersburg",
    "russia/wta-sao-petersburgo":               "St. Petersburg",
    "brazil/atp-rio-de-janeiro":                "Rio de Janeiro",
    "argentina/atp-buenos-aires":               "Buenos Aires",
    "india/atp-pune":                           "Pune",
    "india/atp-chennai":                        "Chennai",
    "south-korea/atp-seul":                     "Seoul",
    "south-korea/wta-seul":                     "Seoul",
    "sweden/atp-estocolmo":                     "Stockholm",
    "belgium/atp-antuerpia":                    "Antwerp",
    "croatia/atp-zagreb":                       "Zagreb",
    "croatia/atp-umag":                         "Umag",
    "serbia/atp-belgrado":                      "Belgrade",
    "greece/atp-atenas":                        "Athens",
    "chile/atp-santiago":                       "Santiago",
    "ecuador/atp-quito":                        "Quito",
    "colombia/atp-bogota":                      "Bogota",
    "portugal/atp-estoril":                     "Estoril",
    "portugal/wta-estoril":                     "Estoril",
    "czech-republic/wta-praga":                 "Prague",
    "czech-republic/atp-praga":                 "Prague",
    "hungary/atp-budapeste":                    "Budapest",
    "hungary/wta-budapeste":                    "Budapest",
    "poland/atp-varsovia":                      "Warsaw",
    "thailand/wta-pattaya":                     "Pattaya",
    "bahrain/atp-bahrein":                      "Manama",
    "spain/atp-valencia":                       "Valencia",
    "israel/atp-tel-aviv":                      "Tel Aviv",
    "kazakhstan/atp-astana":                    "Astana",
    "turkey/atp-antalya":                       "Antalya",
    "turkey/atp-istambul":                      "Istanbul",
    "austria/atp-kitzbuhel":                    "Kitzbuhel",
    "romania/atp-bucareste":                    "Bucharest",
    "romania/wta-bucareste":                    "Bucharest",
    "australia/atp-adelaide":                   "Adelaide",
    "australia/wta-adelaide":                   "Adelaide",
    "australia/atp-sydney":                     "Sydney",
    "australia/wta-sydney":                     "Sydney",
    "australia/atp-brisbane":                   "Brisbane",
    "australia/wta-brisbane":                   "Brisbane",
    "australia/wta-hobart":                     "Hobart",
    "new-zealand/atp-auckland":                 "Auckland",
    "new-zealand/wta-auckland":                 "Auckland",
    "china/atp-shenzhen":                       "Shenzhen",
    "china/wta-shenzhen":                       "Shenzhen",
    "china/atp-chengdu":                        "Chengdu",
    "china/wta-zhuhai":                         "Zhuhai",
    "china/wta-guangzhou":                      "Guangzhou",
    "china/wta-tianjin":                        "Tianjin",
    "mexico/wta-guadalajara":                   "Guadalajara",
    "mexico/atp-los-cabos":                     "Los Cabos",
    "japan/wta-hiroshima":                      "Hiroshima",
    "japan/atp-hiroshima":                      "Hiroshima",
    "japan/wta-osaka":                          "Osaka",
    "india/wta-pune":                           "Pune",
    "south-africa/atp-joanesburgo":             "Johannesburg",
    "france/atp-lyon":                          "Lyon",
    "france/wta-estrasburgo":                   "Strasbourg",
    "france/atp-marselha":                      "Marseille",
    "netherlands/atp-s-hertogenbosch":          "s-Hertogenbosch",
    "netherlands/wta-s-hertogenbosch":          "s-Hertogenbosch",
    "united-kingdom/atp-eastbourne":            "Eastbourne",
    "united-kingdom/wta-eastbourne":            "Eastbourne",
    "united-kingdom/wta-birmingham":            "Birmingham",
    "united-kingdom/atp-nottingham":            "Nottingham",
    "serbia/wta-belgrado":                      "Belgrade",
    "italy/wta-palermo":                        "Palermo",
    "usa/wta-san-diego":                        "San Diego",
    "usa/wta-cleveland":                        "Cleveland",
    "usa/wta-palm-springs":                     "Palm Springs",
    "usa/atp-dallas":                           "Dallas",
    "usa/atp-delray-beach":                     "Delray Beach",
    "usa/atp-san-jose":                         "San Jose",
    "usa/wta-san-jose":                         "San Jose",
    "usa/atp-winston-salem-eua":                "Winston-Salem",
    "usa/atp-atlanta":                          "Atlanta",
    "usa/atp-newport":                          "Newport",
    "usa/atp-los-angeles":                      "Los Angeles",
    "kazakhstan/wta-astana":                    "Astana",
    "ukraine/itf-w25-kyiv-mulheres":            "Kyiv",
}

_COUNTRY_NAMES: dict[str, str] = {
    "usa":                  "USA",
    "uae":                  "UAE",
    "united-arab-emirates": "UAE",
    "united-kingdom":       "UK",
    "south-korea":          "South Korea",
    "south-africa":         "South Africa",
    "czech-republic":       "Czech Republic",
    "new-zealand":          "New Zealand",
    "saudi-arabia":         "Saudi Arabia",
    "ivory-coast":          "Ivory Coast",
    "trinidad-tobago":      "Trinidad and Tobago",
}


def _derive_venue(league_path: str) -> tuple[str, str]:
    """Retorna (country, city) derivados do slug da liga."""
    parts = league_path.split("/", 1)
    country_slug = parts[0]
    tournament_slug = parts[1] if len(parts) > 1 else ""
    country = _COUNTRY_NAMES.get(country_slug, country_slug.replace("-", " ").title())
    if league_path in LEAGUE_CITY_OVERRIDES:
        return country, LEAGUE_CITY_OVERRIDES[league_path]
    slug = tournament_slug
    for pfx in ("atp-", "wta-", "challenger-", "itf-m15-", "itf-w15-",
                 "itf-m25-", "itf-w25-", "itf-m50-", "itf-w50-",
                 "itf-m100-", "itf-w100-"):
        if slug.startswith(pfx):
            slug = slug[len(pfx):]
            break
    for sfx in ("-open", "-masters", "-challenger", "-homens",
                "-mulheres", "-duplas", "-1", "-2", "-3", "-4"):
        if slug.endswith(sfx):
            slug = slug[:-len(sfx)]
            break
    return country, slug.replace("-", " ").title()


# Torneios conhecidos — usados como COMPLEMENTO ao discovery dinamico.
# O discovery raspa /tennis/results/ e descobre slugs reais automaticamente.
# Esta lista serve de fallback para torneios historicos que nao aparecem nas paginas recentes.
# Nomenclatura: {pais}/{atp|wta}-{cidade-ou-nome-pt}
# Torneios sem historico no site sao ignorados silenciosamente (0 matches = pula)
KNOWN_LEAGUES = [
    # --- Grand Slams ---
    "australia/atp-australian-open",
    "australia/wta-australian-open",
    "france/atp-aberto-da-franca",      # slug real: aberto-da-franca (PT), nao french-open
    "france/wta-aberto-da-franca",
    "united-kingdom/atp-wimbledon",
    "united-kingdom/wta-wimbledon",
    "usa/atp-us-open",
    "usa/wta-us-open",

    # --- ATP Masters 1000 ---
    "usa/atp-indian-wells",
    "usa/atp-miami",
    "monaco/atp-monte-carlo",
    "spain/atp-madri",
    "italy/atp-roma",
    "canada/atp-montreal",
    "canada/atp-toronto",
    "usa/atp-cincinnati",
    "china/atp-xangai",
    "france/atp-paris",                 # slug real: paris (PT), nao paris-bercy

    # --- WTA 1000 ---
    "usa/wta-indian-wells",
    "usa/wta-miami",
    "spain/wta-madri",
    "italy/wta-roma",
    "canada/wta-montreal",
    "canada/wta-toronto",
    "usa/wta-cincinnati",
    "china/wta-wuhan",
    "china/wta-beijing",                # slug real: beijing (EN), nao pequim (PT)

    # --- ATP 500 ---
    "netherlands/atp-roterda",          # slug real: roterda (PT), nao rotterdam
    "united-arab-emirates/atp-dubai",   # slug real: united-arab-emirates, nao uae
    "mexico/atp-acapulco",
    "spain/atp-barcelona",
    "germany/atp-hamburgo",
    "germany/atp-halle",                # ATP 500 - estava faltando
    "germany/atp-munique",
    "usa/atp-washington",
    "japan/atp-tokyo",                  # slug real: tokyo (EN), nao toquio (PT)
    "china/atp-beijing",                # slug real: beijing (EN), nao pequim (PT)
    "switzerland/atp-basel",            # slug real: basel (EN), nao basileia (PT)
    "austria/atp-viena",

    # --- WTA 500 ---
    "australia/wta-adelaide",           # slug real: adelaide (EN), nao adelaida (PT)
    "australia/wta-sydney",
    "australia/wta-brisbane",
    "china/wta-shenzhen",
    "qatar/wta-doha",
    "united-arab-emirates/wta-dubai",   # slug real: united-arab-emirates, nao uae
    "usa/wta-charleston",
    "germany/wta-stuttgart",
    "spain/wta-barcelona",
    "usa/wta-washington",
    "czech-republic/wta-praga",
    "china/wta-tianjin",
    "china/wta-guangzhou",
    "japan/wta-tokyo",                  # slug real: tokyo (EN), nao toquio (PT)
    "russia/wta-moscou",
    "austria/wta-linz",

    # --- ATP 250 (principais) ---
    "australia/atp-adelaide",           # slug real: adelaide (EN), nao adelaida (PT)
    "australia/atp-sydney",
    "australia/atp-brisbane",
    "new-zealand/atp-auckland",
    "qatar/atp-doha",
    "south-africa/atp-joanesburgo",
    "brazil/atp-rio-de-janeiro",
    "argentina/atp-buenos-aires",
    "chile/atp-santiago",
    "colombia/atp-bogota",
    "ecuador/atp-quito",
    "mexico/atp-los-cabos",
    "usa/atp-dallas",
    "usa/atp-delray-beach",
    "usa/atp-houston",
    "usa/atp-san-jose",
    "usa/atp-winston-salem-eua",        # slug real: winston-salem-eua (PT), nao winston-salem
    "usa/atp-atlanta",
    "usa/atp-newport",
    "usa/atp-los-angeles",
    "switzerland/atp-genebra",
    "france/atp-lyon",
    "netherlands/atp-s-hertogenbosch",
    "united-kingdom/atp-eastbourne",
    "united-kingdom/atp-nottingham",
    "germany/atp-stuttgart",
    "austria/atp-kitzbuhel",
    "sweden/atp-estocolmo",
    "russia/atp-moscou",
    "russia/atp-sao-petersburgo",       # slug real: sao-petersburgo (PT), nao st-petersburgo
    "romania/atp-bucareste",
    "hungary/atp-budapeste",
    "croatia/atp-umag",
    "croatia/atp-zagreb",
    "serbia/atp-belgrado",
    "turkey/atp-antalya",               # slug real: antalya (EN), nao antalia (PT)
    "turkey/atp-istambul",
    "india/atp-pune",
    "india/atp-chennai",
    "china/atp-shenzhen",
    "china/atp-chengdu",
    "south-korea/atp-seul",
    "israel/atp-tel-aviv",
    "kazakhstan/atp-astana",
    "belgium/atp-antuerpia",

    # --- WTA 250 (principais) ---
    "new-zealand/wta-auckland",
    "australia/wta-hobart",
    "colombia/wta-bogota",
    "france/wta-estrasburgo",
    "netherlands/wta-s-hertogenbosch",
    "united-kingdom/wta-eastbourne",
    "united-kingdom/wta-birmingham",
    "hungary/wta-budapeste",
    "romania/wta-bucareste",
    "serbia/wta-belgrado",
    "turkey/wta-istanbul",
    "italy/wta-palermo",
    "usa/wta-san-diego",
    "usa/wta-cleveland",
    "china/wta-jiujiang",
    "china/wta-zhuhai",
    "south-korea/wta-seul",
    "japan/wta-hiroshima",
    "japan/wta-osaka",
    "india/wta-pune",
    "kazakhstan/wta-astana",
    "mexico/wta-guadalajara",
    "usa/wta-palm-springs",

    # --- Challengers ATP relevantes ---
    "brazil/challenger-sao-paulo",
    "brazil/challenger-rio-de-janeiro",
    "argentina/challenger-buenos-aires",
    "chile/challenger-santiago",
    "colombia/challenger-bogota",
    "peru/challenger-lima",
    "ecuador/challenger-quito",

    # --- Torneios descobertos no discovery ativo ---
    "ivory-coast/abidjan-challenger-homens",
    "ivory-coast/abidjan-challenger-homens-duplas",
    "kazakhstan/shymkent-challenger-homens",
    "kazakhstan/shymkent-challenger-homens-duplas",
    "portugal/oeiras-4-challenger-mulheres",
    "portugal/oeiras-4-challenger-mulheres-duplas",
    "singapore/itf-m15-singapore-2-homens",
    "singapore/itf-w15-singapore-2-mulheres",
    "south-korea/gwangju-challenger-homens",
    "japan/itf-w100-tokyo-mulheres",
    "italy/roma-challenger-homens",
    "usa/savannah-challenger-homens",
    "china/itf-m25-luzhou-homens",
    "china/itf-w50-baotou-mulheres",
]

# ---------------------------------------------------------------------------
# Prioridade por tier: garante que Grand Slams/Masters são processados PRIMEIRO
# dentro de cada shard, mesmo que o shard expire antes de terminar todos os torneios.
# Tier 0 = mais importante; tier 5 = ITF M15 etc.
# ---------------------------------------------------------------------------

_TIER_GRAND_SLAMS = {
    "australia/atp-australian-open", "australia/wta-australian-open",
    "france/atp-aberto-da-franca",   "france/wta-aberto-da-franca",
    "united-kingdom/atp-wimbledon",  "united-kingdom/wta-wimbledon",
    "usa/atp-us-open",               "usa/wta-us-open",
}

_TIER_MASTERS = {
    # ATP Masters 1000
    "usa/atp-indian-wells", "usa/atp-miami", "monaco/atp-monte-carlo",
    "spain/atp-madri",      "italy/atp-roma",
    "canada/atp-montreal",  "canada/atp-toronto",
    "usa/atp-cincinnati",   "china/atp-xangai",    "france/atp-paris",
    # WTA 1000
    "usa/wta-indian-wells", "usa/wta-miami",
    "spain/wta-madri",      "italy/wta-roma",
    "canada/wta-montreal",  "canada/wta-toronto",
    "usa/wta-cincinnati",   "china/wta-wuhan",     "china/wta-beijing",
}

_TIER_500 = {
    "netherlands/atp-roterda", "united-arab-emirates/atp-dubai", "mexico/atp-acapulco",
    "spain/atp-barcelona",     "germany/atp-hamburgo",           "germany/atp-halle",
    "germany/atp-munique",     "usa/atp-washington",             "japan/atp-tokyo",
    "china/atp-beijing",       "switzerland/atp-basel",          "austria/atp-viena",
    "germany/atp-stuttgart",
    # WTA 500
    "australia/wta-adelaide", "usa/wta-charleston", "germany/wta-stuttgart",
    "united-arab-emirates/wta-dubai", "usa/wta-washington",
    "qatar/wta-doha",         "russia/wta-moscou",  "japan/wta-tokyo",
    "china/wta-shenzhen",
}


def _league_tier(path: str) -> int:
    """Retorna tier de prioridade (0=mais importante, 5=ITF M15)."""
    if path in _TIER_GRAND_SLAMS:
        return 0
    if path in _TIER_MASTERS:
        return 1
    if path in _TIER_500:
        return 2
    seg = path.split("/", 1)[-1] if "/" in path else path
    if seg.startswith(("atp-", "wta-")):
        return 3   # ATP/WTA 250+
    if "challenger" in seg:
        return 4
    return 5        # ITF M15/M25 etc.



# ---------------------------------------------------------------------------
# Discovery dinamico de torneios
# ---------------------------------------------------------------------------

def _extract_league_slugs(html: str) -> set[str]:
    """Extrai slugs {pais}/{torneio} de uma pagina de resultados de tenis."""
    found = set()
    # Match URL pattern: /tennis/{country}/{tournament}/{match-id}/
    for m in re.finditer(r'/tennis/([a-z0-9][a-z0-9-]*/[a-z0-9][a-z0-9-]*)/[a-z0-9#-]', html):
        slug = m.group(1)
        # Exclui slugs que parecem ser path de match ou segmentos invalidos
        parts = slug.split("/")
        if len(parts) == 2 and parts[0] and parts[1]:
            found.add(slug)
    return found


def load_discovered_leagues() -> list[str] | None:
    """Carrega slugs do cache JSON. Retorna None se inexistente, expirado ou vazio."""
    if os.environ.get("FORCE_DISCOVERY") == "1":
        # Cache commitado FRESCO vence o FORCE_DISCOVERY: o discovery re-gerado no
        # runner (IP EUA) recebe do sitemap ~3.7k slugs EN geo-alternates que 404am
        # e queimam o budget (onda 63: 714 fetches 404, zero jogos). O JSON commitado
        # (sitemap via IP BR, slugs PT puros) e a fonte autoritativa ate expirar o TTL.
        try:
            p = Path(DISCOVERY_CACHE_FILE)
            data = json.loads(p.read_text(encoding="utf-8"))
            if (_time.time() - data.get("timestamp", 0) <= DISCOVERY_CACHE_TTL
                    and data.get("leagues")):
                print(f"[discovery] FORCE_DISCOVERY=1 mas cache commitado fresco "
                      f"({len(data['leagues'])} slugs PT) — usando o cache")
                return data["leagues"]
        except Exception:
            pass
        print("[discovery] FORCE_DISCOVERY=1 — ignorando cache, re-descobrindo do zero")
        return None
    try:
        p = Path(DISCOVERY_CACHE_FILE)
        if not p.exists():
            return None
        data = json.loads(p.read_text(encoding="utf-8"))
        if _time.time() - data.get("timestamp", 0) > DISCOVERY_CACHE_TTL:
            print("[discovery] Cache expirado, re-descobrindo...")
            return None
        leagues = data.get("leagues", [])
        if not leagues:
            print("[discovery] Cache vazio, re-descobrindo...")
            return None
        print(f"[discovery] Cache carregado: {len(leagues)} torneios ({p})")
        return leagues
    except Exception as e:
        print(f"[discovery] Erro ao carregar cache: {e}")
        return None


def save_discovered_leagues(leagues: list[str]) -> None:
    try:
        p = Path(DISCOVERY_CACHE_FILE)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(
            json.dumps({"timestamp": _time.time(), "leagues": sorted(leagues)},
                       ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"[discovery] {len(leagues)} slugs salvos em {p}")
    except Exception as e:
        print(f"[discovery] Erro ao salvar cache: {e}")


DISCOVERY_WAIT_SELECTOR = '[data-testid="game-row"], a[href*="/tennis/"]'
# Para LISTAGEM de resultados: esperar SO a game-row real (o seletor de discovery
# acima casa com links de menu e retorna antes das linhas renderizarem -> false-empty).
LISTING_WAIT_SELECTOR = '[data-testid="game-row"]'


async def _fetch_fresh(br: OddsPortalBrowser, url: str, wait_selector: str | None = None,
                       settle: float | None = None) -> str:
    """Fetch sem usar cache local (sempre hit na rede)."""
    return await br.fetch(url, wait_selector=wait_selector, settle=settle)


_SITEMAP_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")


def _fetch_xml(url: str) -> str:
    """Busca um XML cru (sitemap) via HTTP. Descomprime gzip se necessario."""
    req = urllib.request.Request(url, headers={"User-Agent": _SITEMAP_UA,
                                               "Accept": "application/xml,text/xml,*/*",
                                               "Accept-Language": "pt-BR,pt;q=0.9"})
    with urllib.request.urlopen(req, timeout=30) as r:
        data = r.read()
    if data[:2] == b"\x1f\x8b":
        data = gzip.decompress(data)
    return data.decode("utf-8", "replace")


def discover_leagues_from_sitemap(sport_slug: str) -> list[str]:
    """Fonte AUTORITATIVA (porte do football-scraper): le os sitemaps do oddsagora
    e retorna os slugs base {pais}/{torneio}, ja nos nomes PT canonicos.

    Substitui a raspagem de paginas de indice, que gerava slugs EN -> 404
    (ex: 'germany/atp-hamburg' quando o site usa 'germany/atp-hamburgo'). Era a
    causa-raiz do plateau: ~9.3k slugs EN mortos consumiam todo o budget em 404
    e as ligas PT reais nem entravam na fila. Validado 2026-07-01: 9.488 slugs
    PT, todos os Grand Slams/Masters/ITF presentes, zero slugs EN.
    """
    sources = [
        f"{BASE_URL}/sitemap/{sport_slug}/results.xml",
        f"{BASE_URL}/sitemap/{sport_slug}/standings.xml",
        f"{BASE_URL}/sitemap/tournament.xml",
    ]
    raw: set[str] = set()
    for src in sources:
        try:
            txt = _fetch_xml(src)
        except Exception as e:
            print(f"[sitemap] erro {src}: {e}")
            continue
        # Indice (aponta p/ sub-sitemaps, ex: results-1.xml) vs urlset direto.
        subs = re.findall(r"<loc>\s*([^<]*sitemap[^<]*\.xml)\s*</loc>", txt)
        bodies = []
        if subs:
            for s in subs:
                try:
                    bodies.append(_fetch_xml(s.strip()))
                except Exception:
                    pass
        else:
            bodies = [txt]
        for body in bodies:
            raw |= set(re.findall(
                rf"/{sport_slug}/([a-z0-9-]+/[a-z0-9-]+)(?:/results/|/standings/|/)", body))

    # Normaliza sufixo de SEASON (ano YYYY ou YYYY-YYYY) -> slug base, porque o
    # scraper ja anexa SEASON_SUFFIXES.
    def _base(slug: str) -> str:
        return re.sub(r"-(19|20)\d{2}(-(19|20)\d{2})?$", "", slug)

    out = sorted({_base(s) for s in raw})
    print(f"[sitemap] {len(out)} torneios (slugs PT) do sitemap de {sport_slug}")
    return out


async def discover_leagues(br: OddsPortalBrowser) -> list[str]:
    """
    Descobre os slugs de torneios. FONTE PRIMARIA: sitemap do oddsagora (lista
    autoritativa, slugs PT corretos, deterministica). Fallback: raspagem das
    paginas de resultados (metodo antigo) se o sitemap falhar/vier curto.
    """
    try:
        sm = discover_leagues_from_sitemap(SPORT_SLUG)
    except Exception as e:
        print(f"[sitemap] falhou completamente: {e}")
        sm = []
    if len(sm) >= 200:
        print(f"[discovery] usando {len(sm)} slugs do sitemap (fonte autoritativa)")
        return sm
    print(f"[discovery] sitemap deu so {len(sm)} slugs — fallback p/ raspagem de indice")

    slugs: set[str] = set(sm)
    discovery_urls = [
        f"{BASE_URL}/tennis/results/",
        f"{BASE_URL}/tennis/",
        f"{BASE_URL}/matches/tennis/",
    ]

    print(f"\n[discovery] Buscando slugs reais em oddsagora.com.br ...")

    for base in discovery_urls:
        print(f"\n[discovery] Fonte: {base}")
        consecutive_no_new = 0
        for page in range(1, DISCOVERY_MAX_PAGES + 1):
            url = base if page == 1 else f"{base}#/page/{page}/"
            try:
                # Espera a game-row REAL renderizar + settle de 2.5s para a GRADE inteira
                # carregar. Sem isso, retorno cedo pega só o shell SSR/hreflang (slugs EN)
                # -> discovery flaky (ora 18k PT, ora 9k EN). Com render: slugs PT estaveis.
                html = await _fetch_fresh(br, url, wait_selector=LISTING_WAIT_SELECTOR, settle=2.5)
            except Exception as e:
                print(f"  [discovery] Erro pagina {page}: {e}")
                break

            before = len(slugs)
            new = _extract_league_slugs(html)
            slugs.update(new)
            after = len(slugs)

            # Verifica se ha algum conteudo util na pagina (game-rows OU links de tenis)
            matches = results_listing.parse(html)
            has_content = bool(matches) or bool(new)
            if not has_content:
                print(f"  [discovery] Pagina {page}: vazia — pulando fonte")
                break

            print(f"  [discovery] Pagina {page}: {len(matches)} matches, +{after-before} slugs novos (total global: {after})")

            if after == before:
                consecutive_no_new += 1
                if consecutive_no_new >= 5:
                    print(f"  [discovery] 5 paginas consecutivas sem slugs novos — proxima fonte")
                    break
            else:
                consecutive_no_new = 0

        if slugs:
            # Se ja achou algo com essa fonte, nao precisa tentar as outras (economiza tempo)
            # Mas continue se quiser maximo de cobertura (deixa comentado como opcional)
            pass

    print(f"\n[discovery] Total: {len(slugs)} torneios descobertos")
    return sorted(slugs)


async def _fetch_cached(br: OddsPortalBrowser, url: str, wait_selector=None, force=False) -> str:
    """Busca HTML do cache local se disponivel; caso contrario faz request.
    force=True ignora o cache (usado para CONFIRMAR um listing vazio antes de cachear)."""
    if USE_CACHE and not force:
        cached = cache_mod.get(url)
        if cached is not None:
            return cached
    html = await br.fetch(url, wait_selector=wait_selector)
    if USE_CACHE:
        cache_mod.put(url, html)
    return html


# ---------------------------------------------------------------------------
# Checkpoint e Backfill
# ---------------------------------------------------------------------------

def _scraped_urls(db_path: str, league_path: str, season: str) -> set:
    """Retorna set de source_url de eventos ja completos: tem score E home_away odds."""
    try:
        con = sqlite3.connect(db_path)
        rows = con.execute("""
            SELECT DISTINCT e.source_url FROM events e
            JOIN leagues l ON l.id = e.league_id
            WHERE l.path = ? AND e.season = ?
              AND e.score_home IS NOT NULL AND e.score_home != ''
              AND (
                e.status != 'finished'
                OR (e.sets_detail IS NOT NULL AND e.sets_detail != '')
              )
              AND EXISTS (
                SELECT 1 FROM odds o JOIN markets m ON m.id = o.market_id
                WHERE o.event_id = e.id AND m.name = 'home_away'
              )
        """, (league_path, season)).fetchall()
        con.close()
        return {r[0] for r in rows}
    except Exception:
        return set()


def _scraped_urls_count(db_path: str, league_path: str, season: str) -> int:
    """Conta eventos existentes para liga+season (Achado 2: guard para false-empty)."""
    try:
        con = sqlite3.connect(db_path)
        row = con.execute(
            "SELECT COUNT(*) FROM events e JOIN leagues l ON e.league_id=l.id WHERE l.path=? AND e.season=?",
            (league_path, season)
        ).fetchone()
        con.close()
        return row[0] if row else 0
    except Exception:
        return 0


def _league_incomplete_events(db_path: str, league_path: str) -> list[dict]:
    """
    Retorna eventos da liga que estao no DB sem score OU sem home_away odds.
    Cobre: fetch da pagina principal falhou, score nao parseado, odds ausentes.
    """
    try:
        con = sqlite3.connect(db_path)
        rows = con.execute("""
            SELECT e.id, e.source_url, e.season,
                   th.name AS home, ta.name AS away, e.dt_utc, e.status
            FROM events e
            JOIN leagues l  ON l.id = e.league_id
            JOIN teams   th ON th.id = e.home_id
            JOIN teams   ta ON ta.id = e.away_id
            WHERE l.path = ? AND e.source_url != ''
              AND (
                e.score_home IS NULL OR e.score_home = ''
                OR NOT EXISTS (
                  SELECT 1 FROM odds o JOIN markets m ON m.id = o.market_id
                  WHERE o.event_id = e.id AND m.name = 'home_away'
                )
                OR (e.status = 'finished'
                    AND (e.sets_detail IS NULL OR e.sets_detail = ''))
              )
        """, (league_path,)).fetchall()
        con.close()
        return [
            {"event_id": r[0], "match_url": r[1], "season": r[2],
             "home": r[3], "away": r[4], "event_datetime_utc": r[5], "status": r[6]}
            for r in rows
        ]
    except Exception:
        return []


# ---------------------------------------------------------------------------
# Processar um match
# ---------------------------------------------------------------------------

def _load_parser(module_name: str):
    return importlib.import_module(
        f"astrology.scrapers.oddsportal.parsers.{module_name}"
    )


_ALL_TAB_LABELS: list[str] | None = None


def _all_tab_labels() -> list[str]:
    """Labels (dedupados) de todos os mercados que exigem clique de tab."""
    global _ALL_TAB_LABELS
    if _ALL_TAB_LABELS is None:
        labels = []
        for mk in MARKETS:
            try:
                meta = get_market(mk)
            except KeyError:
                continue
            if meta.get("tab_slug", "") and meta.get("tab_label"):
                labels.append(meta["tab_label"])
        _ALL_TAB_LABELS = list(dict.fromkeys(labels))
    return _ALL_TAB_LABELS


async def _process_match(
    br: OddsPortalBrowser, m: dict,
    league_path: str, season: str,
    writer: SQLiteWriter, sem: asyncio.Semaphore,
    idx: int, total: int,
) -> int:
    async with sem:
        # Respeita o orcamento POR JOGO: budget estourado no meio de um lote grande
        # -> tasks restantes saem na hora -> shard encerra limpo (success), nao cancelled.
        if _budget_exceeded():
            return 0
        match_url = m["match_url"]
        print(f"  [match {idx}/{total}] {m.get('home','?')} vs {m.get('away','?')}")
        try:
            # UMA navegacao por jogo: header + tabs na mesma visita (antes eram
            # 2 navegacoes da mesma URL — fetch da main page + fetch_all_tabs).
            # Tabs ausentes na pagina simplesmente nao voltam no dict.
            h_main, tab_htmls = await br.fetch_match(match_url, _all_tab_labels())
            header = match_header.parse(h_main)
            home = header.get("home") or m.get("home", "")
            away = header.get("away") or m.get("away", "")
            iso  = header.get("event_datetime_utc") or m.get("event_datetime_utc", "")
            event_id = stable_event_id(SPORT_KEY, league_path, home, away, iso)

            ctx = {
                "sport": SPORT_KEY, "league": league_path, "season": season or "",
                "event_id": event_id, "event_datetime_utc": iso,
                "event_datetime_local": "",
                "home": home, "away": away,
                "score_home": _coalesce_score(header.get("score_home"), m.get("score_home", "")),
                "score_away": _coalesce_score(header.get("score_away"), m.get("score_away", "")),
                "sets_detail": header.get("sets_detail", ""),
                "status": header.get("status", "scheduled"),
                "venue": header.get("venue", ""), "venue_city": header.get("venue_city", ""),
                "venue_country": header.get("venue_country", ""),
                "venue_lat": "", "venue_lon": "",
                "payout_pct": "", "source_url": match_url,
                "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
            }

            _country, _city = _derive_venue(league_path)
            ctx["venue_country"] = ctx["venue_country"] or _country
            ctx["venue_city"]    = ctx["venue_city"]    or _city

            wanted = []
            for mk in MARKETS:
                try:
                    meta = get_market(mk)
                except KeyError:
                    continue
                label    = meta["tab_label"]
                tab_slug = meta.get("tab_slug", "")
                # Requer navegacao (tab_slug) mas a tab nao existia na pagina
                # (fetch_match nao retornou o label) -> mercado indisponivel, pula.
                if tab_slug and label and label not in tab_htmls:
                    continue
                wanted.append((mk, label, meta["parser_module"]))

            htmls = {"": h_main}
            htmls.update(tab_htmls)

            rows_written = 0
            for mk, label, pmod_name in wanted:
                try:
                    pmod = _load_parser(pmod_name)
                    rows = pmod.parse(htmls.get(label, h_main), dict(ctx))
                except Exception as e:
                    print(f"    [ERRO parser {pmod_name}]: {e}")
                    continue
                for r in rows:
                    writer.write(r)
                rows_written += len(rows)

            # Garante que o evento fica no DB com score mesmo sem odds historicas
            if rows_written == 0 and ctx.get("event_id"):
                writer.ensure_event(ctx)

            print(f"  -> {rows_written} linhas (score: {ctx.get('score_home', '')}:{ctx.get('score_away', '')})")
            return rows_written

        except Exception as e:
            print(f"  [ERRO match]: {e}")
            return 0


# ---------------------------------------------------------------------------
# Processar um torneio (todas as seasons)
# ---------------------------------------------------------------------------

async def scrape_league(
    br: OddsPortalBrowser, league_path: str, writer: SQLiteWriter,
    league_sem: asyncio.Semaphore, match_sem: asyncio.Semaphore,
    idx: int = 0, total: int = 0,
):
    # Slug morto: listing da season atual deu 404 numa onda anterior (n=-1 no
    # season_state) e a liga nao tem nenhum evento no DB. 404 nao muda com a
    # season — re-fetchar e desperdicio. Sem este skip, cada onda re-fetchava as
    # seasons NAO-finais (atual + ano corrente) de cada liga morta (~3 fetches
    # x ~20s por liga) — dreno principal do budget de 330min.
    try:
        _row = writer._con.execute(
            "SELECT n_matches FROM season_state WHERE league_path=? AND season=''",
            (league_path,)).fetchone()
        _root_broken = _row is not None and (_row[0] or 0) == -1
    except Exception:
        _root_broken = False
    if _root_broken and _scraped_urls_count(str(writer.path), league_path, "") == 0:
        print(f"[skip-broken] {league_path}: slug 404 em onda anterior — pulando liga")
        return {"league": league_path, "matches_total": 0, "empty_seasons": 0,
                "skipped_cache": 1, "skipped_complete": 0}
    async with league_sem:
        if _budget_exceeded():
            return {"league": league_path, "matches_total": 0, "empty_seasons": 0,
                    "skipped_cache": 0, "skipped_complete": 0, "skipped_budget": True}
        tier = _league_tier(league_path)
        _matches_total = 0
        _empty_seasons = 0
        _skipped_cache = 0
        _skipped_complete = 0
        prefix = f"[{idx}/{total}] " if idx else ""
        print(f"\n{'='*55}")
        print(f"{prefix}Torneio: {league_path}  [tier={tier}]")
        print(f"{'='*55}")

        _consecutive_empty = 0
        _sfx_idx = 0
        while _sfx_idx < len(SEASON_SUFFIXES):
            if _budget_exceeded():
                break
            suffix = SEASON_SUFFIXES[_sfx_idx]
            _sfx_idx += 1
            season_str = suffix or ""

            # Pre-listing skip (correcao B): seasons passadas ja raspadas por completo
            # numa onda anterior sao puladas ANTES de paginar a listagem. Sem isso o
            # shard re-rastejava a listagem (ate 25 pgs) toda onda e nunca avancava
            # pela fila para alcancar os tiers profundos (ITF).
            if _season_is_final(suffix) and writer.is_season_complete(league_path, season_str):
                print(f"  [skip-cache] {league_path} season={season_str} ja completa (onda anterior)")
                _skipped_cache += 1
                continue

            results_url = url_builder.build_results_url(SPORT_SLUG, league_path, suffix)
            print(f"  [listing] season={season_str or 'atual'}")

            # Listagem + paginacao via CLIQUE (fetch_listing_pages):
            #  - espera a game-row REAL (corrige false-empty do seletor de menu)
            #  - clica os botoes "1 2 3 ... Próximo" (a paginacao #/page/N por goto
            #    nao funciona no oddsagora; so trazia a pg1 ~51 de ~380)
            #  - re-tenta a pg1 ate 2x antes de declarar vazio (throttle != vazio)
            try:
                _pages, _http = await br.fetch_listing_pages(results_url, wait_selector=LISTING_WAIT_SELECTOR)
            except Exception as e:
                print(f"  [ERRO listing {results_url}]: {e}")
                continue

            # Distingue vazio-real (HTTP 200) de slug quebrado (404) — base da garantia
            # de completude: 404 NUNCA conta como "vazio confirmado".
            _broken      = (_http is not None and _http >= 400)
            _valid_empty = (_http == 200)

            seen_urls = set()
            all_matches = []
            for _h in _pages:
                for m in results_listing.parse(_h):
                    if m["match_url"] not in seen_urls:
                        seen_urls.add(m["match_url"])
                        all_matches.append(m)
            # A paginacao por clique esgota todas as paginas -> pass completa.
            _pag_ok = True

            if not all_matches:
                tag = "404/quebrado" if _broken else ("200-vazio" if _valid_empty else "incerto")
                print(f"  [vazio:{tag}] sem matches em {results_url} (http={_http})")
                _empty_seasons += 1
                # 404 = slug QUEBRADO (nao muda com a season). Cacheia broken em QUALQUER
                # season — INCLUSIVE a atual/None — senao o listing 404 e re-tentado TODA
                # onda. Era a causa do plato: ~936 slugs ruins do discovery re-raspados a
                # cada onda -> 0 jogos novos. Agora marca broken na 1a vez e pula depois.
                if _broken and _scraped_urls_count(str(writer.path), league_path, season_str) == 0:
                    writer.mark_season_complete(league_path, season_str, -1)
                if _season_is_final(suffix):
                    _consecutive_empty += 1
                    existing_count = _scraped_urls_count(str(writer.path), league_path, season_str)
                    if existing_count > 0:
                        print(f"  [skip-empty-guard] {league_path}/{season_str} tem {existing_count} eventos no DB — nao cacheia como vazio")
                    elif _broken:
                        writer.mark_season_complete(league_path, season_str, -1)   # slug quebrado/404
                    elif _valid_empty:
                        writer.mark_season_complete(league_path, season_str, 0)    # 200 -> vazio confirmado
                    # else: http incerto (rede/throttle) -> NAO cacheia, re-tenta

                    # Early-stop: apos EARLY_STOP_THRESHOLD vazios consecutivos acima de
                    # PROBE_CUTOFF, verifica ancoras historicas para decidir se a liga esta
                    # morta ou tem dados so em anos antigos.
                    try:
                        _sfx_end_year = int(str(suffix).split("-")[-1])
                    except (TypeError, ValueError):
                        _sfx_end_year = 0
                    if _consecutive_empty >= EARLY_STOP_THRESHOLD and _sfx_end_year > PROBE_CUTOFF:
                        # 404 = slug quebrado deterministico: probar ancoras (ate 9
                        # fetches x2 com retry) e desperdicio — vai direto ao bulk-cache -1.
                        anchors_to_probe = [] if _broken else [y for y in ANCHOR_YEARS if y < _sfx_end_year]
                        found_anchor = None
                        for anchor_year in anchors_to_probe:
                            probe_url = url_builder.build_results_url(SPORT_SLUG, league_path, str(anchor_year))
                            try:
                                probe_html = await _fetch_cached(br, probe_url, wait_selector=LISTING_WAIT_SELECTOR)
                                probe_matches = results_listing.parse(probe_html)
                                if not probe_matches:
                                    probe_html = await _fetch_cached(br, probe_url,
                                                                     wait_selector=LISTING_WAIT_SELECTOR, force=True)
                                    probe_matches = results_listing.parse(probe_html)
                            except Exception:
                                probe_matches = []
                            if probe_matches:
                                found_anchor = anchor_year
                                break

                        if found_anchor is not None:
                            print(f"  [early-stop-probe] {league_path}: dados em {found_anchor} — retomando iteracao normal")
                            _consecutive_empty = 0
                        else:
                            # Liga sem dados: bulk-cache as seasons restantes. 404 (slug
                            # quebrado) -> -1 (gap p/ auditoria); 200 (morta) -> 0 (confirmado).
                            _mark = -1 if _broken else 0
                            remaining = SEASON_SUFFIXES[_sfx_idx:]
                            n_cached = 0
                            for rem in remaining:
                                if rem and _season_is_final(rem):
                                    writer.mark_season_complete(league_path, rem, _mark)
                                    n_cached += 1
                            print(f"  [early-stop] {league_path}: {EARLY_STOP_THRESHOLD} vazios + {len(anchors_to_probe)} anchors vazios — {n_cached} seasons cacheadas")
                            break
                continue

            _consecutive_empty = 0
            # Checkpoint por jogo: pula apenas os ja completos (score + home_away odds)
            done_urls = _scraped_urls(str(writer.path), league_path, season_str)
            matches_to_process = [m for m in all_matches if m["match_url"] not in done_urls]
            if not matches_to_process:
                print(f"  [skip] {league_path} season={season_str or 'atual'} — todos {len(all_matches)} ja completos")
                _skipped_complete += 1
                # Season passada totalmente coberta -> grava no cache p/ pular antes da listagem na proxima onda
                if _season_is_final(suffix) and _pag_ok:
                    writer.mark_season_complete(league_path, season_str, len(all_matches))
                continue

            print(f"  {len(matches_to_process)}/{len(all_matches)} jogos a raspar (season '{season_str or 'atual'}')")
            _matches_total += len(matches_to_process)
            tasks = [
                _process_match(br, m, league_path, season_str, writer, match_sem, i, len(matches_to_process))
                for i, m in enumerate(matches_to_process, 1)
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            # Pass completa numa season PASSADA: o conjunto de jogos do torneio finalizado
            # nao muda mais; marca no cache para nao re-rastejar a listagem nas proximas ondas.
            # (jogos sem odds no site permanecem sem odds — re-tentar e desperdicio que trava o avanco)
            if _season_is_final(suffix) and _pag_ok:
                writer.mark_season_complete(league_path, season_str, len(all_matches))

        # Backfill: re-raspa eventos sem score OU sem home_away odds.
        # Exclui seasons passadas ja marcadas completas (cache B): re-tentar jogos
        # cronicamente sem odds (challenger/ITF antigos) impediria o shard de avancar.
        incomplete = [] if _budget_exceeded() else [
            ev for ev in _league_incomplete_events(str(writer.path), league_path)
            if not (_season_is_final(ev.get("season")) and writer.is_season_complete(league_path, ev.get("season", "")))
        ]
        if incomplete:
            print(f"  [backfill] {len(incomplete)} eventos incompletos (sem score ou odds) em {league_path}")
            bf_tasks = [
                _process_match(br, ev, league_path, ev["season"], writer, match_sem, i, len(incomplete))
                for i, ev in enumerate(incomplete, 1)
            ]
            await asyncio.gather(*bf_tasks, return_exceptions=True)
        return {
            "league": league_path,
            "matches_total": _matches_total,
            "empty_seasons": _empty_seasons,
            "skipped_cache": _skipped_cache,
            "skipped_complete": _skipped_complete,
        }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def _amnesty(writer, cutoff_iso: str, include_root: bool) -> int:
    """Remove marcas n<=0 do season_state p/ re-tentativa: anos-buraco + ratchet
    pre-min de ligas cobertas; opcionalmente as marcas de raiz (season='').
    Retorna quantas linhas removeu."""
    _yrs_by_lg: dict[str, set[int]] = {}
    for _path, _season in writer._con.execute(
            "SELECT DISTINCT l.path, e.season FROM events e "
            "JOIN leagues l ON e.league_id=l.id WHERE e.season != ''"):
        _s = str(_season)
        _y = None
        if _s.isdigit():
            _y = int(_s)
        elif "-" in _s and _s.split("-")[-1].isdigit():
            _y = int(_s.split("-")[-1])
        if _y:
            _yrs_by_lg.setdefault(_path, set()).add(_y)
    _n = 0
    for _path, _yrs in _yrs_by_lg.items():
        if not _yrs:
            continue
        _lo, _hi = min(_yrs), max(_yrs)
        _targets = [_y for _y in range(_lo, _hi + 1) if _y not in _yrs]
        if _lo - 1 >= 1998:
            _targets.append(_lo - 1)
        for _y in _targets:
            for _sfx in (str(_y), f"{_y-1}-{_y}"):
                _n += writer._con.execute(
                    "DELETE FROM season_state WHERE league_path=? AND season=? "
                    "AND n_matches<=0 AND completed_at < ?",
                    (_path, _sfx, cutoff_iso)).rowcount
    if include_root:
        # Reabre ligas marcadas mortas na raiz (skip-broken): em janela boa o
        # 404 falso de throttle se desfaz; 404 real re-marca em ~3s (fast-exit).
        _n += writer._con.execute(
            "DELETE FROM season_state WHERE season='' AND n_matches<0 "
            "AND completed_at < ?", (cutoff_iso,)).rowcount
    writer._con.commit()
    return _n


async def main():
    print("=" * 60)
    print("Tennis Full History Scraper")
    print(f"Output: {DB_PATH}")
    print(f"Paralelo: {PARALLEL_LEAGUES} torneios x {PARALLEL_MATCHES} matches")
    print("=" * 60)

    writer = SQLiteWriter(DB_PATH)


    async with OddsPortalBrowser(headful=False, concurrency=BROWSER_POOL) as br:

        # --- Fase 1: discovery de torneios reais ---
        discovered = load_discovered_leagues()
        if discovered is None:
            discovered = await discover_leagues(br)
            save_discovered_leagues(discovered)

        # Modo discover-only (usado pelo GitHub Actions para gerar slugs antes do matrix)
        if DISCOVER_ONLY:
            print(f"\n[discover-only] {len(discovered)} slugs salvos. Saindo.")
            writer.close()
            return

        # ------------------------------------------------------------------
        # CANARIO de janela (2026-07-11): o site serve grade VAZIA (200) para o
        # runner de forma INTERMITENTE (dias com 8-12k eventos arquivados raspados
        # alternam com dias ~0; provado que campeonato-ingles-2020-2021 "vazio
        # confirmado" tem 381 jogos). Testa 2 listagens arquivadas SABIDAMENTE com
        # dados: se renderizam -> janela BOA -> amnistia agressiva (re-tenta tudo
        # que esta bloqueado agora, inclusive ligas mortas na raiz). Se vazias ->
        # janela RUIM -> amnistia conservadora (cadencia 3d) e nada de desperdicar
        # budget re-listando vazios falsos.
        # ------------------------------------------------------------------
        from datetime import timedelta
        _canary_ok = False
        for _cu in ['https://www.oddsagora.com.br/tennis/france/atp-aberto-da-franca-2023/results/', 'https://www.oddsagora.com.br/tennis/netherlands/atp-roterda-2023/results/']:
            try:
                _pgs, _st = await br.fetch_listing_pages(_cu, wait_selector=LISTING_WAIT_SELECTOR)
                if any('data-testid="game-row"' in _h for _h in _pgs):
                    _canary_ok = True
                    break
            except Exception:
                pass
        try:
            if _canary_ok:
                _cut = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                _na = _amnesty(writer, _cut, include_root=True)
                print(f"[canario] janela BOA — amnistia agressiva: {_na} bloqueios removidos")
            else:
                _cut = (datetime.now(timezone.utc) - timedelta(days=3)).strftime("%Y-%m-%dT%H:%M:%SZ")
                _na = _amnesty(writer, _cut, include_root=False)
                print(f"[canario] janela RUIM — amnistia conservadora (3d): {_na} removidos")
        except Exception as _e:
            print(f"[amnistia] erro (nao-fatal): {_e}")

        # Merge: torneios descobertos + KNOWN_LEAGUES + ligas ja no DB (complemento historico)
        # DB-restore: ligas raspadas em runs anteriores que sumiram do discovery (slug mismatch)
        # sao re-incluidas automaticamente. Isso resolve o plateau quando o discovery muda de
        # formato e deixa de gerar os slugs que ja temos no banco.
        try:
            _db_leagues = {r[0] for r in writer._con.execute(
                "SELECT DISTINCT l.path FROM leagues l JOIN events e ON e.league_id=l.id")}
            _n_db_extra = len(_db_leagues - set(discovered) - set(KNOWN_LEAGUES))
            if _n_db_extra:
                print(f"[db-restore] +{_n_db_extra} ligas do DB nao no discovery/KNOWN — re-incluindo")
        except Exception as _e:
            print(f"[db-restore] erro: {_e}")
            _db_leagues = set()
        all_leagues = sorted(set(discovered) | set(KNOWN_LEAGUES) | _db_leagues)
        print(f"\n[info] {len(discovered)} descobertos + {len(KNOWN_LEAGUES)} conhecidos + {len(_db_leagues)} do DB = {len(all_leagues)} torneios unicos")

        # Sharding (GitHub Actions matrix): divide a lista global entre shards
        if TOTAL_SHARDS > 1:
            all_leagues = [s for i, s in enumerate(all_leagues) if i % TOTAL_SHARDS == SHARD_ID]
            print(f"[shard {SHARD_ID}/{TOTAL_SHARDS}] {len(all_leagues)} torneios neste shard (antes de reordenar)")

        # COVERAGE-FIRST: ligas SEM dados no seed vao PRIMEIRO, pra cada onda atacar a
        # CAUDA (tier 4/5, ITF) em vez de re-andar o topo ja coberto. Sem isso o shard
        # gastava o orcamento re-raspando a temporada atual do topo e nunca alcancava as
        # ~7.8k ITF que faltam. Cobertas ficam por ultimo (puladas rapido pelo cache).
        try:
            _covered = {r[0] for r in writer._con.execute(
                "SELECT DISTINCT l.path FROM events e JOIN leagues l ON e.league_id=l.id")}
        except Exception:
            _covered = set()
        all_leagues.sort(key=lambda p: (
            p in _covered,
            -_league_tier(p) if PRIORITY_INVERT else _league_tier(p),
            p,
        ))
        _n_unc = sum(1 for p in all_leagues if p not in _covered)
        print(f"[priority] {_n_unc} sem-dados PRIMEIRO, {len(all_leagues)-_n_unc} cobertas depois")
        if DEBUG_LEAGUES:
            all_leagues = [lg for lg in all_leagues if _league_tier(lg) <= 1][:5]
            print(f"[DEBUG] Modo local — apenas {len(all_leagues)} ligas tier 0/1: {all_leagues}")
        tier_counts = {}
        for lg in all_leagues:
            t = _league_tier(lg)
            tier_counts[t] = tier_counts.get(t, 0) + 1
        print(f"[priority] Ordem: {dict(sorted(tier_counts.items()))} (tier 0=Grand Slam, 5=ITF)")

        # Filtro por manifest de incompletos (retry run)
        manifest_path = os.environ.get("INCOMPLETE_MANIFEST")
        if manifest_path and Path(manifest_path).exists():
            import json as _json
            incomplete_manifest = _json.loads(Path(manifest_path).read_text(encoding="utf-8"))
            slugs = {e["league"] for e in incomplete_manifest}
            all_leagues = [l for l in all_leagues if l in slugs]
            print(f"[manifest] Retry restrito a {len(all_leagues)} ligas incompletas")

        # --- Fase 2: scraping de todos os torneios ---
        league_sem = asyncio.Semaphore(PARALLEL_LEAGUES)
        match_sem  = asyncio.Semaphore(PARALLEL_MATCHES)  # global: compartilhado entre ligas
        tasks = [
            scrape_league(br, lg, writer, league_sem, match_sem, idx, len(all_leagues))
            for idx, lg in enumerate(all_leagues, 1)
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        leagues_with_data     = sum(1 for r in results if isinstance(r, dict) and r.get("matches_total", 0) > 0)
        leagues_empty         = sum(1 for r in results if isinstance(r, dict) and r.get("matches_total", 0) == 0)
        seasons_skipped_cache = sum(r.get("skipped_cache", 0) for r in results if isinstance(r, dict))
        print(f"\n[resumo] Ligas com dados: {leagues_with_data} | Ligas vazias: {leagues_empty} | Seasons puladas por cache: {seasons_skipped_cache}")

    stats = writer.stats()
    writer.close()

    # Status do shard para o job continue: exhausted=true significa que a fila de
    # ligas acabou DENTRO do budget (nada pendente neste shard).
    skipped_budget = sum(1 for r in results if isinstance(r, dict) and r.get("skipped_budget"))
    status = {
        "shard": SHARD_ID,
        "exhausted": not _BUDGET["exceeded"],
        "leagues_total": len(all_leagues),
        "leagues_skipped_budget": skipped_budget,
    }
    try:
        Path(STATUS_PATH).write_text(json.dumps(status), encoding="utf-8")
        print(f"\n[status] {STATUS_PATH}: {status}")
    except Exception as e:
        print(f"\n[status] falha ao gravar {STATUS_PATH}: {e}")

    print("\n\nConcluido!")
    print(f"  DB: {DB_PATH}")
    print(f"  Eventos: {stats.get('events', 0)}")
    print(f"  Odds: {stats.get('odds', 0)}")
    print(f"  Torneios: {stats.get('leagues', 0)}")


if __name__ == "__main__":
    asyncio.run(main())
