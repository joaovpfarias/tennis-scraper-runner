# Script: historico completo de tenis do oddsagora.com.br
# Rodar: C:\Users\Dell\anaconda3\python.exe astrology\scrapers\oddsportal\scrape_tennis_history.py
# Saida:  astrology/scrapers/oddsportal/data/raw/tennis_history.db
#
# Fase 1 (discovery): busca /tennis/results/ paginado e extrai todos os slugs de torneio ja disputados
# Fase 2 (scraping):  para cada torneio, raspa temporada atual + sufixos YYYY-YYYY / YYYY
# Fase 3 (paralelo):  torneios rodam em paralelo (PARALLEL_LEAGUES ao mesmo tempo)

import asyncio
import json
import os
import re
import sys
import time as _time
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
from datetime import datetime, timezone

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

# Sharding (GitHub Actions matrix): SHARD_ID/TOTAL_SHARDS dividem a lista de torneios.
SHARD_ID      = int(os.environ.get("SHARD_ID", "0"))
TOTAL_SHARDS  = int(os.environ.get("TOTAL_SHARDS", "1"))
DISCOVER_ONLY = os.environ.get("DISCOVER_ONLY", "0") == "1"
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
SEASON_YEARS  = list(range(2026, 2004, -1))   # 2026, 2025, ..., 2005 (early-stop automatico)
SEASON_SUFFIXES = [None] + [str(y) for y in SEASON_YEARS]

PARALLEL_LEAGUES  = 1   # 1 torneio por vez (evita travar a maquina)
PARALLEL_MATCHES  = 8   # matches em paralelo (usa todas as paginas do pool)
BROWSER_POOL      = 8   # paginas Chromium no pool
USE_CACHE         = True
DISCOVERY_MAX_PAGES = 150       # paginas maximas a varrer na fase de discovery
DISCOVERY_CACHE_FILE = str(Path(__file__).parent / "data" / "raw" / "discovered_tennis_leagues.json")
DISCOVERY_CACHE_TTL  = 30 * 24 * 3600  # 30 dias

# Torneios conhecidos — usados como COMPLEMENTO ao discovery dinamico.
# O discovery raspa /tennis/results/ e descobre slugs reais automaticamente.
# Esta lista serve de fallback para torneios historicos que nao aparecem nas paginas recentes.
# Nomenclatura: {pais}/{atp|wta}-{cidade-ou-nome-pt}
# Torneios sem historico no site sao ignorados silenciosamente (0 matches = pula)
KNOWN_LEAGUES = [
    # --- Grand Slams ---
    "australia/atp-australian-open",
    "australia/wta-australian-open",
    "france/atp-french-open",
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
    "canada/atp-canada",
    "canada/atp-montreal",
    "canada/atp-toronto",
    "usa/atp-cincinnati",
    "china/atp-xangai",
    "france/atp-paris-bercy",

    # --- WTA Premier / WTA 1000 ---
    "usa/wta-indian-wells",
    "usa/wta-miami",
    "spain/wta-madri",
    "italy/wta-roma",
    "canada/wta-canada",
    "canada/wta-montreal",
    "canada/wta-toronto",
    "usa/wta-cincinnati",
    "china/wta-wuhan",
    "china/wta-pequim",

    # --- ATP 500 ---
    "netherlands/atp-rotterdam",
    "uae/atp-dubai",
    "mexico/atp-acapulco",
    "spain/atp-barcelona",
    "germany/atp-hamburgo",
    "germany/atp-munique",
    "usa/atp-washington",
    "japan/atp-toquio",
    "china/atp-pequim",
    "switzerland/atp-basileia",
    "austria/atp-viena",

    # --- ATP 250 (mais importantes) ---
    "australia/atp-adelaida",
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
    "usa/atp-winston-salem",
    "usa/atp-atlanta",
    "usa/atp-newport",
    "usa/atp-los-angeles",
    "switzerland/atp-genebra",
    "france/atp-lyon",
    "france/atp-estrasburgo",
    "netherlands/atp-s-hertogenbosch",
    "united-kingdom/atp-queens",
    "united-kingdom/atp-eastbourne",
    "germany/atp-stuttgart",
    "austria/atp-kitzbuhel",
    "sweden/atp-estocolmo",
    "russia/atp-moscou",
    "russia/atp-st-petersburgo",
    "romania/atp-bucareste",
    "hungary/atp-budapeste",
    "croatia/atp-umag",
    "croatia/atp-zagreb",
    "serbia/atp-belgrado",
    "turkey/atp-antalia",
    "turkey/atp-istambul",
    "ukraine/atp-kiev",
    "india/atp-pune",
    "india/atp-chennai",
    "china/atp-shenzhen",
    "china/atp-chengdu",
    "japan/atp-hiroshima",
    "south-korea/atp-seul",
    "australia/atp-perth",
    "israel/atp-tel-aviv",
    "kazakhstan/atp-astana",
    "saudi-arabia/atp-riyadh",

    # --- WTA 500 ---
    "australia/wta-adelaida",
    "australia/wta-sydney",
    "australia/wta-brisbane",
    "china/wta-shenzhen",
    "qatar/wta-doha",
    "uae/wta-dubai",
    "usa/wta-charleston",
    "germany/wta-stuttgart",
    "spain/wta-barcelona",
    "usa/wta-san-jose",
    "usa/wta-washington",
    "czech-republic/wta-praga",
    "china/wta-tianjin",
    "taiwan/wta-taipei",
    "china/wta-guangzhou",
    "japan/wta-toquio",
    "russia/wta-moscou",
    "austria/wta-linz",

    # --- WTA 250 (mais importantes) ---
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
    "canada/wta-montreal",
    "china/wta-jiangxi",
    "china/wta-zhuhai",
    "south-korea/wta-seul",
    "japan/wta-hiroshima",
    "japan/wta-osaka",
    "india/wta-pune",
    "ukraine/wta-kiev",
    "kazakhstan/wta-astana",
    "mexico/wta-guadalajara",
    "usa/wta-palm-springs",

    # --- Challengers ATP relevantes (Brasileiros/Sul-americanos) ---
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


async def _fetch_fresh(br: OddsPortalBrowser, url: str, wait_selector: str | None = None) -> str:
    """Fetch sem usar cache local (sempre hit na rede)."""
    return await br.fetch(url, wait_selector=wait_selector)


async def discover_leagues(br: OddsPortalBrowser) -> list[str]:
    """
    Raspa paginas do oddsagora.com.br para descobrir slugs reais de torneios.
    Tenta multiplas URLs-fonte (resultados globais + pagina principal do esporte).
    """
    slugs: set[str] = set()
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
                html = await _fetch_fresh(br, url, wait_selector=DISCOVERY_WAIT_SELECTOR)
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


async def _fetch_cached(br: OddsPortalBrowser, url: str, wait_selector=None) -> str:
    """Busca HTML do cache local se disponivel; caso contrario faz request."""
    if USE_CACHE:
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
              AND EXISTS (
                SELECT 1 FROM odds o JOIN markets m ON m.id = o.market_id
                WHERE o.event_id = e.id AND m.name = 'home_away'
              )
        """, (league_path, season)).fetchall()
        con.close()
        return {r[0] for r in rows}
    except Exception:
        return set()


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


async def _process_match(
    br: OddsPortalBrowser, m: dict,
    league_path: str, season: str,
    writer: SQLiteWriter, sem: asyncio.Semaphore,
    idx: int, total: int,
) -> int:
    async with sem:
        match_url = m["match_url"]
        print(f"  [match {idx}/{total}] {m.get('home','?')} vs {m.get('away','?')}")
        try:
            from astrology.scrapers.oddsportal.browser import ODDS_WAIT_SELECTOR
            h_main = await _fetch_cached(br, match_url, wait_selector=ODDS_WAIT_SELECTOR)
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
                "score_home": header.get("score_home") or m.get("score_home", ""),
                "score_away": header.get("score_away") or m.get("score_away", ""),
                "sets_detail": header.get("sets_detail", ""),
                "status": header.get("status", "scheduled"),
                "venue": header.get("venue", ""), "venue_city": header.get("venue_city", ""),
                "venue_country": header.get("venue_country", ""),
                "venue_lat": "", "venue_lon": "",
                "payout_pct": "", "source_url": match_url,
                "scraped_at_utc": datetime.now(timezone.utc).isoformat(),
            }

            available_tabs = set(match_tabs.parse(h_main))
            wanted = []
            for mk in MARKETS:
                try:
                    meta = get_market(mk)
                except KeyError:
                    continue
                label    = meta["tab_label"]
                tab_slug = meta.get("tab_slug", "")
                # Pula so se requer navegacao (tab_slug nao vazio) E tab nao esta na pagina
                if tab_slug and available_tabs and label not in available_tabs:
                    continue
                wanted.append((mk, label, meta["parser_module"]))

            needed_labels = list({lbl for _, lbl, _ in wanted if lbl})
            tab_htmls = await br.fetch_all_tabs(match_url, needed_labels) if needed_labels else {}
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

            print(f"  -> {rows_written} linhas")
            return rows_written

        except Exception as e:
            print(f"  [ERRO match]: {e}")
            return 0


# ---------------------------------------------------------------------------
# Processar um torneio (todas as seasons)
# ---------------------------------------------------------------------------

async def scrape_league(
    br: OddsPortalBrowser, league_path: str, writer: SQLiteWriter,
    league_sem: asyncio.Semaphore,
):
    async with league_sem:
        match_sem = asyncio.Semaphore(PARALLEL_MATCHES)
        print(f"\n{'='*55}")
        print(f"Torneio: {league_path}")
        print(f"{'='*55}")

        for suffix in SEASON_SUFFIXES:
            season_str = suffix or ""

            results_url = url_builder.build_results_url(SPORT_SLUG, league_path, suffix)
            print(f"  [listing] season={season_str or 'atual'}")

            try:
                html = await _fetch_cached(br, results_url, wait_selector=DISCOVERY_WAIT_SELECTOR)
            except Exception as e:
                print(f"  [ERRO listing {results_url}]: {e}")
                continue

            all_matches = results_listing.parse(html)

            # Paginacao
            total_pages = results_listing.detect_pagination(html)
            for pg in range(2, total_pages + 1):
                page_url = f"{results_url}#/page/{pg}/"
                try:
                    h = await _fetch_cached(br, page_url, wait_selector=DISCOVERY_WAIT_SELECTOR)
                    all_matches.extend(results_listing.parse(h))
                except Exception:
                    break

            if not all_matches:
                print(f"  [vazio] sem matches em {results_url}")
                continue

            # Checkpoint por jogo: pula apenas os ja completos (score + home_away odds)
            done_urls = _scraped_urls(str(writer.path), league_path, season_str)
            matches_to_process = [m for m in all_matches if m["match_url"] not in done_urls]
            if not matches_to_process:
                print(f"  [skip] {league_path} season={season_str or 'atual'} — todos {len(all_matches)} ja completos")
                continue

            print(f"  {len(matches_to_process)}/{len(all_matches)} jogos a raspar (season '{season_str or 'atual'}')")
            tasks = [
                _process_match(br, m, league_path, season_str, writer, match_sem, i, len(matches_to_process))
                for i, m in enumerate(matches_to_process, 1)
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

        # Backfill: re-raspa eventos sem score OU sem home_away odds
        incomplete = _league_incomplete_events(str(writer.path), league_path)
        if incomplete:
            print(f"  [backfill] {len(incomplete)} eventos incompletos (sem score ou odds) em {league_path}")
            bf_tasks = [
                _process_match(br, ev, league_path, ev["season"], writer, match_sem, i, len(incomplete))
                for i, ev in enumerate(incomplete, 1)
            ]
            await asyncio.gather(*bf_tasks, return_exceptions=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

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

        # Merge: torneios descobertos + KNOWN_LEAGUES (complemento historico)
        all_leagues = sorted(set(discovered) | set(KNOWN_LEAGUES))
        print(f"\n[info] {len(discovered)} descobertos + {len(KNOWN_LEAGUES)} conhecidos = {len(all_leagues)} torneios unicos")

        # Sharding (GitHub Actions matrix)
        if TOTAL_SHARDS > 1:
            all_leagues = [s for i, s in enumerate(all_leagues) if i % TOTAL_SHARDS == SHARD_ID]
            print(f"[shard {SHARD_ID}/{TOTAL_SHARDS}] {len(all_leagues)} torneios neste shard")

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
        tasks = [scrape_league(br, lg, writer, league_sem) for lg in all_leagues]
        await asyncio.gather(*tasks, return_exceptions=True)

    stats = writer.stats()
    writer.close()
    print("\n\nConcluido!")
    print(f"  DB: {DB_PATH}")
    print(f"  Eventos: {stats.get('events', 0)}")
    print(f"  Odds: {stats.get('odds', 0)}")
    print(f"  Torneios: {stats.get('leagues', 0)}")


if __name__ == "__main__":
    asyncio.run(main())
