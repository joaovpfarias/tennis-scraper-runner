"""
Fetch Real Odds from oddsagora.com.br
======================================
Para cada partida em cumulative.csv no período [--from, --to], raspa a
pagina correspondente em oddsagora.com.br e extrai a média (de todas as
bookmakers exibidas) das odds para cada mercado de interesse.

Estratégia: itera por data e bate em /matches/football/YYYYMMDD/ — esse
endpoint lista TODAS as partidas do dia em todas as ligas. Bem mais
eficiente que descobrir liga-a-liga.

Saída: astrology/data/odds/real_odds.csv com colunas:
  match_date, home_team, away_team, league, country, market, real_odd,
  n_bookmakers, source_url, scraped_at

Cobre mercados: H_FT, D_FT, A_FT, 1X_FT, X2_FT, Over{15,25,35}_FT,
Under{15,25}_FT, BTTS_Yes, BTTS_No.
(H_HT e Over05_HT ficam de fora — oddsagora não tem tab dedicada ao 1º tempo.)

Uso:
    python astrology/scripts/fetch_real_odds.py --from 2026-04-16 --to 2026-04-18
    python astrology/scripts/fetch_real_odds.py --from 2026-04-16 --to 2026-04-18 --league "europa"
"""
import asyncio
import os
import sys
import argparse
import difflib
import statistics
from pathlib import Path
from datetime import datetime, timezone

import pandas as pd

# Sharding via env vars (GitHub Actions matrix)
SHARD_ID             = int(os.environ.get("SHARD_ID", "0"))
TOTAL_SHARDS         = int(os.environ.get("TOTAL_SHARDS", "1"))
DISCOVER_ONLY        = os.environ.get("DISCOVER_ONLY", "0") == "1"
OUTPUT_PATH_OVERRIDE = os.environ.get("OUTPUT_PATH_OVERRIDE")
DATE_FROM_ENV        = os.environ.get("DATE_FROM")
DATE_TO_ENV          = os.environ.get("DATE_TO")

SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT  = SCRIPT_DIR.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from astrology.scrapers.oddsportal.browser    import OddsPortalBrowser
from astrology.scrapers.oddsportal            import cache as cache_mod
from astrology.scrapers.oddsportal.normalizer import normalize_team_name
from astrology.scrapers.oddsportal.parsers    import (
    results_listing,
    market_1x2,
    market_over_under,
    market_btts,
    market_double_chance,
)

# ─────────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────────
DATA_DIR       = SCRIPT_DIR.parent / 'data'
REPORTS_DIR    = DATA_DIR / 'reports'
ODDS_DIR       = DATA_DIR / 'odds'
CUMULATIVE_CSV = REPORTS_DIR / 'cumulative.csv'
REAL_ODDS_CSV  = ODDS_DIR / 'real_odds.csv'
UNMATCHED_CSV  = ODDS_DIR / 'unmatched.csv'

# ─────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────
BASE_URL              = "https://www.oddsagora.com.br"
# Estritamente game-row — o seletor com a[href*="/football/"] casa com o menu
# do site mesmo quando a listagem de partidas ainda nao renderizou.
LISTING_WAIT_SELECTOR = '[data-testid="game-row"]'
LISTING_MAX_PAGES     = 1    # so pagina 1 — datas recentes ficam ali

MATCH_RATIO_THRESHOLD = 0.85

# Tabs default: so 1X2 (principal, sem clique) + Over/Under.
# BTTS, Double Chance e H_HT exigem cliques/fetches extras (lentos).
# Use --full para incluir todos.
TAB_LABELS_FAST = ["Acima/Abaixo"]
TAB_LABELS_FULL = ["Acima/Abaixo", "Ambas equipes marcam", "Double Chance"]

PARALLEL_LEAGUES_DEFAULT = 16  # ligas em paralelo
PARALLEL_MATCHES         = 6   # partidas em paralelo por liga
USE_CACHE                = True

# Variaveis globais setadas em main por args
INCLUDE_HT_BLOCK = False
TAB_LABELS       = TAB_LABELS_FAST


# ─────────────────────────────────────────────
# CACHE DE HTML
# ─────────────────────────────────────────────
async def _fetch_cached(br: OddsPortalBrowser, url: str, wait_selector=None) -> str:
    if USE_CACHE:
        cached = cache_mod.get(url)
        if cached is not None:
            return cached
    html = await br.fetch(url, wait_selector=wait_selector)
    if USE_CACHE:
        cache_mod.put(url, html)
    return html


# ─────────────────────────────────────────────
# ALVO: LER cumulative.csv
# ─────────────────────────────────────────────
def load_targets(date_from, date_to, league_filter=None) -> pd.DataFrame:
    if not CUMULATIVE_CSV.exists():
        print(f"ERRO: {CUMULATIVE_CSV} não encontrado.")
        sys.exit(1)
    cum = pd.read_csv(CUMULATIVE_CSV)
    cum = cum[(cum['data'] >= date_from) & (cum['data'] <= date_to)]
    if league_filter:
        cum = cum[cum['league'].str.contains(league_filter, case=False, na=False)]
    tgt = cum[['data', 'home_team', 'away_team', 'league']].drop_duplicates().reset_index(drop=True)
    return tgt


# ─────────────────────────────────────────────
# MATCHING FUZZY
# ─────────────────────────────────────────────
def _fuzzy(a: str, b: str) -> float:
    return difflib.SequenceMatcher(
        None,
        normalize_team_name(a).lower(),
        normalize_team_name(b).lower(),
    ).ratio()


def find_listing(target_home, target_away, target_date, listings):
    """Retorna o listing mais próximo ou None."""
    best, best_score = None, 0.0
    for l in listings:
        l_date = (l.get('event_datetime_utc') or '')[:10]
        if l_date != target_date:
            continue
        sh = _fuzzy(target_home, l['home'])
        sa = _fuzzy(target_away, l['away'])
        if sh >= MATCH_RATIO_THRESHOLD and sa >= MATCH_RATIO_THRESHOLD:
            s = sh + sa
            if s > best_score:
                best, best_score = l, s
    return best


# ─────────────────────────────────────────────
# EXTRACAO DE ODDS
# ─────────────────────────────────────────────
def _mean_odds(rows, key_filter):
    """Filtra rows por key_filter(row) e retorna (média, n). Ignora odds vazias."""
    vals = []
    for r in rows:
        if not key_filter(r):
            continue
        o = r.get('odds_closing')
        if isinstance(o, (int, float)) and o > 1.0:
            vals.append(float(o))
    if not vals:
        return None, 0
    return round(statistics.fmean(vals), 4), len(vals)


def extract_market_means(htmls: dict) -> dict:
    """
    Recebe {tab_label: html} (incluindo "" para a principal 1X2 e
    "_ht" para 1X2 do 1º tempo) e retorna dict {market_key: (mean_odd, n_bookmakers)}.
    """
    out = {}

    # ── 1X2 principal (H_FT, D_FT, A_FT) ─────────────────────
    h_main = htmls.get("", htmls.get("1X2", ""))
    if h_main:
        rows = market_1x2.parse(h_main, {})
        out['H_FT'] = _mean_odds(rows, lambda r: r['outcome'] == 'home')
        out['D_FT'] = _mean_odds(rows, lambda r: r['outcome'] == 'draw')
        out['A_FT'] = _mean_odds(rows, lambda r: r['outcome'] == 'away')

    # ── 1X2 do 1º tempo (H_HT) ───────────────────────────────
    h_ht = htmls.get("_ht", "")
    if h_ht:
        rows = market_1x2.parse(h_ht, {})
        out['H_HT'] = _mean_odds(rows, lambda r: r['outcome'] == 'home')

    # ── Double Chance (1X_FT, X2_FT) ─────────────────────────
    h_dc = htmls.get("Double Chance", "")
    if h_dc:
        rows = market_double_chance.parse(h_dc, {})
        out['1X_FT'] = _mean_odds(rows, lambda r: r['outcome'] == '1x')
        out['X2_FT'] = _mean_odds(rows, lambda r: r['outcome'] == 'x2')

    # ── Over/Under ───────────────────────────────────────────
    h_ou = htmls.get("Acima/Abaixo", "")
    if h_ou:
        rows = market_over_under.parse(h_ou, {})
        for n in ('1.5', '2.5', '3.5'):
            key_o = f"Over{n.replace('.','')}_FT"
            out[key_o] = _mean_odds(
                rows,
                lambda r, _n=n: r['submarket'] == _n and r['outcome'] == 'over'
            )
        for n in ('1.5', '2.5'):
            key_u = f"Under{n.replace('.','')}_FT"
            out[key_u] = _mean_odds(
                rows,
                lambda r, _n=n: r['submarket'] == _n and r['outcome'] == 'under'
            )

    # ── BTTS ─────────────────────────────────────────────────
    h_btts = htmls.get("Ambas equipes marcam", "")
    if h_btts:
        rows = market_btts.parse(h_btts, {})
        out['BTTS_Yes'] = _mean_odds(rows, lambda r: r['outcome'] == 'yes')
        out['BTTS_No']  = _mean_odds(rows, lambda r: r['outcome'] == 'no')

    return out


# ─────────────────────────────────────────────
# PROCESSAR 1 PARTIDA
# ─────────────────────────────────────────────
async def process_match(br, listing, target_row, sem, write_queue):
    async with sem:
        match_url = listing['match_url']
        try:
            h_main = await _fetch_cached(br, match_url)
            tab_htmls = (
                await br.fetch_all_tabs(match_url, TAB_LABELS) if TAB_LABELS else {}
            )
            h_ht = ""
            if INCLUDE_HT_BLOCK:
                ht_url = f"{match_url.rstrip('/')}#a:1X2;2"
                try:
                    h_ht = await _fetch_cached(br, ht_url)
                except Exception:
                    h_ht = ""
        except Exception as e:
            print(f"    [ERRO fetch {match_url}]: {e}")
            return

        htmls = {"": h_main, "_ht": h_ht}
        htmls.update(tab_htmls)

        market_means = extract_market_means(htmls)
        now_iso = datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
        found = 0
        for market, (mean, n) in market_means.items():
            if mean is None:
                continue
            write_queue.append({
                'match_date':    target_row['data'],
                'home_team':     target_row['home_team'],
                'away_team':     target_row['away_team'],
                'league':        target_row['league'],
                'country':       '',
                'market':        market,
                'real_odd':      mean,
                'n_bookmakers':  n,
                'source_url':    match_url,
                'scraped_at':    now_iso,
            })
            found += 1
        print(f"    ✓ {target_row['home_team']} x {target_row['away_team']}: {found} mercados")


# ─────────────────────────────────────────────
# DESCOBERTA DE LIGAS (fallback per-liga; URL /matches/football/DATE/
# nao filtra dadas passadas no oddsagora)
# ─────────────────────────────────────────────
import re as _re
import json as _json
import time as _time

DISCOVERY_CACHE = SCRIPT_DIR.parent / 'scrapers' / 'oddsportal' / 'data' / 'raw' / 'discovered_football_leagues.json'
DISCOVERY_TTL   = 30 * 24 * 3600
DISCOVERY_MAX_PAGES = 80


def _extract_football_slugs(html: str) -> set:
    found = set()
    for m in _re.finditer(
        r'/football/([a-z0-9][a-z0-9-]*/[a-z0-9][a-z0-9-]*)/[a-z0-9#-]', html
    ):
        slug = m.group(1)
        parts = slug.split("/")
        if len(parts) == 2 and parts[0] and parts[1] and parts[0] != 'h2h':
            found.add(slug)
    return found


def load_cached_leagues():
    if not DISCOVERY_CACHE.exists():
        return None
    try:
        data = _json.loads(DISCOVERY_CACHE.read_text(encoding='utf-8'))
        if _time.time() - data.get('timestamp', 0) > DISCOVERY_TTL:
            return None
        leagues = data.get('leagues', [])
        if leagues:
            print(f"[discovery] cache: {len(leagues)} ligas")
            return leagues
    except Exception:
        pass
    return None


def save_cached_leagues(leagues):
    DISCOVERY_CACHE.parent.mkdir(parents=True, exist_ok=True)
    DISCOVERY_CACHE.write_text(
        _json.dumps({'timestamp': _time.time(), 'leagues': sorted(leagues)},
                    ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    print(f"[discovery] {len(leagues)} ligas salvas em cache")


async def discover_leagues(br) -> list:
    """Crawla /football/results/ paginado e extrai todos os slugs encontrados."""
    slugs = set()
    sources = [
        f"{BASE_URL}/football/results/",
        f"{BASE_URL}/football/",
        f"{BASE_URL}/matches/football/",
    ]
    for base in sources:
        print(f"[discovery] fonte: {base}")
        empty_streak = 0
        for page in range(1, DISCOVERY_MAX_PAGES + 1):
            url = base if page == 1 else f"{base}#/page/{page}/"
            try:
                html = await br.fetch(url, wait_selector=LISTING_WAIT_SELECTOR)
            except Exception as e:
                print(f"  [erro] {e}")
                break
            before = len(slugs)
            slugs.update(_extract_football_slugs(html))
            after = len(slugs)
            matches = results_listing.parse(html)
            if not matches and after == before:
                empty_streak += 1
                if empty_streak >= 3:
                    break
            else:
                empty_streak = 0
            print(f"  pg{page}: +{after-before} slugs (total {after}), {len(matches)} matches")
    return sorted(slugs)


async def process_league(br, slug, targets_df, date_from, date_to,
                          write_queue, league_sem, match_sem):
    """Para uma liga, fetch /results/, filtra matches no periodo, raspa odds."""
    async with league_sem:
        results_url = f"{BASE_URL}/football/{slug}/results/"
        try:
            html = await _fetch_cached(br, results_url, wait_selector=LISTING_WAIT_SELECTOR)
        except Exception:
            return

        listings = results_listing.parse(html)
        # Quick check: se pagina 1 nao tem nenhuma data no periodo, pula
        dates_in_listing = {(l.get('event_datetime_utc') or '')[:10] for l in listings}
        if not any(date_from <= d <= date_to for d in dates_in_listing):
            return

        total_pages = results_listing.detect_pagination(html)
        for pg in range(2, min(total_pages + 1, LISTING_MAX_PAGES + 1)):
            try:
                h = await _fetch_cached(
                    br,
                    f"{results_url}#/page/{pg}/",
                    wait_selector=LISTING_WAIT_SELECTOR,
                )
                listings.extend(results_listing.parse(h))
            except Exception:
                break

        listings_in_range = [
            l for l in listings
            if date_from <= (l.get('event_datetime_utc') or '')[:10] <= date_to
        ]
        if not listings_in_range:
            return

        # Match com nossos alvos
        to_fetch = []
        matched = set()
        for _, row in targets_df.iterrows():
            lst = find_listing(row['home_team'], row['away_team'], row['data'], listings_in_range)
            if lst is not None:
                k = (row['data'], row['home_team'], row['away_team'])
                if k not in matched:
                    matched.add(k)
                    to_fetch.append((lst, row))

        if not to_fetch:
            return

        print(f"  [{slug}] {len(listings_in_range)} no periodo, {len(to_fetch)} alvos -> raspando")
        tasks = [process_match(br, lst, row, match_sem, write_queue) for lst, row in to_fetch]
        await asyncio.gather(*tasks, return_exceptions=True)


# ─────────────────────────────────────────────
# WRITE OUTPUT (idempotente)
# ─────────────────────────────────────────────
def write_real_odds(rows: list):
    output_path = Path(OUTPUT_PATH_OVERRIDE) if OUTPUT_PATH_OVERRIDE else REAL_ODDS_CSV
    output_path.parent.mkdir(parents=True, exist_ok=True)
    new_df = pd.DataFrame(rows)
    if new_df.empty:
        return
    if output_path.exists():
        existing = pd.read_csv(output_path)
        combined = pd.concat([existing, new_df], ignore_index=True)
    else:
        combined = new_df
    # Dedup: mantém última entrada para cada (date, home, away, market)
    combined = combined.drop_duplicates(
        subset=['match_date', 'home_team', 'away_team', 'market'],
        keep='last',
    )
    combined.to_csv(output_path, index=False)
    print(f"\n[write] {len(new_df)} novas linhas; arquivo total: {len(combined)} linhas -> {output_path}")


def write_unmatched(unmatched: list):
    if not unmatched:
        return
    df = pd.DataFrame(unmatched)
    df.to_csv(UNMATCHED_CSV, index=False)
    print(f"[write] {len(df)} alvos não-encontrados -> {UNMATCHED_CSV}")


# ─────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────
async def async_main():
    global INCLUDE_HT_BLOCK, TAB_LABELS

    parser = argparse.ArgumentParser()
    parser.add_argument('--from', dest='date_from', default=DATE_FROM_ENV, help='YYYY-MM-DD')
    parser.add_argument('--to',   dest='date_to',   default=DATE_TO_ENV,   help='YYYY-MM-DD')
    parser.add_argument('--league', default=None, help='filtra alvos por substring do nome da liga')
    parser.add_argument('--rediscover', action='store_true', help='ignora cache de ligas')
    parser.add_argument('--full', action='store_true',
                        help='inclui BTTS, Double Chance e H_HT (mais lento)')
    parser.add_argument('--concurrency', type=int, default=PARALLEL_LEAGUES_DEFAULT,
                        help=f'ligas em paralelo (padrao {PARALLEL_LEAGUES_DEFAULT})')
    args = parser.parse_args()
    if not args.date_from or not args.date_to:
        parser.error('--from e --to obrigatorios (ou via DATE_FROM/DATE_TO env vars)')

    INCLUDE_HT_BLOCK  = bool(args.full)
    TAB_LABELS        = TAB_LABELS_FULL if args.full else TAB_LABELS_FAST
    PARALLEL_LEAGUES  = args.concurrency

    print("=" * 60)
    print(f"FETCH REAL ODDS - {args.date_from} .. {args.date_to}"
          f"  ({'FULL' if args.full else 'FAST: 1X2 + Over/Under'})")
    print("=" * 60)

    # 1. Carregar alvos
    targets = load_targets(args.date_from, args.date_to, args.league)
    print(f"Alvos unicos: {len(targets)} partidas")
    if targets.empty:
        return

    # 2. Discover + processar todas as ligas em paralelo
    write_queue    = []
    unmatched_list = []
    # concurrency = max das duas semaforos para nao bloquear pool
    pool_size = max(PARALLEL_LEAGUES, PARALLEL_MATCHES) + 2
    async with OddsPortalBrowser(headful=False, concurrency=pool_size) as br:
        leagues = None if args.rediscover else load_cached_leagues()
        if leagues is None:
            leagues = await discover_leagues(br)
            save_cached_leagues(leagues)

        # Modo discover-only (usado pelo GitHub Actions antes do matrix)
        if DISCOVER_ONLY:
            print(f"\n[discover-only] {len(leagues)} ligas salvas em cache. Saindo.")
            return

        # Sharding (GitHub Actions matrix)
        if TOTAL_SHARDS > 1:
            leagues = [s for i, s in enumerate(leagues) if i % TOTAL_SHARDS == SHARD_ID]
            print(f"[shard {SHARD_ID}/{TOTAL_SHARDS}] {len(leagues)} ligas neste shard")

        print(f"Ligas a processar: {len(leagues)} (paralelo: {PARALLEL_LEAGUES})")

        league_sem = asyncio.Semaphore(PARALLEL_LEAGUES)
        match_sem  = asyncio.Semaphore(PARALLEL_MATCHES)
        tasks = [
            process_league(br, slug, targets, args.date_from, args.date_to,
                           write_queue, league_sem, match_sem)
            for slug in leagues
        ]
        # Roda tudo em paralelo (semaforos limitam concorrencia real)
        await asyncio.gather(*tasks, return_exceptions=True)

    # Alvos sem correspondência
    matched_keys = {(r['match_date'], r['home_team'], r['away_team']) for r in write_queue}
    for _, t in targets.iterrows():
        k = (t['data'], t['home_team'], t['away_team'])
        if k not in matched_keys:
            unmatched_list.append({
                'match_date': t['data'],
                'home_team':  t['home_team'],
                'away_team':  t['away_team'],
                'league':     t['league'],
            })

    # 4. Escrever
    write_real_odds(write_queue)
    write_unmatched(unmatched_list)

    # 5. Sumário
    matched  = len({(r['match_date'], r['home_team'], r['away_team']) for r in write_queue})
    print(f"\n{'='*60}")
    print(f"Alvos totais:        {len(targets)}")
    print(f"Com odds raspadas:   {matched}")
    print(f"Sem correspondência: {len(unmatched_list)}")
    print(f"Cobertura:           {matched/len(targets)*100:.1f}%" if len(targets) else "")


def main():
    asyncio.run(async_main())


if __name__ == '__main__':
    main()
