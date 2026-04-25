"""
Extrai informacoes do header da match page do oddsagora.com.br.

Seletores (2026-04):
  [data-testid="game-host"]         - time da casa (home)
  [data-testid="game-guest"]        - time visitante (away)
  [data-testid="game-time-item"]    - "Hoje, 19 Abr 2026, 00:30"
  [data-testid="game-participants"] - "Lakers 107 - 98 Rockets" (placar)
"""
import re
from bs4 import BeautifulSoup
from ..normalizer import normalize_team_name, parse_pt_datetime


def parse(html: str) -> dict:
    soup = BeautifulSoup(html, "lxml")

    # Times
    host_el  = soup.select_one('[data-testid="game-host"]')
    guest_el = soup.select_one('[data-testid="game-guest"]')
    home = normalize_team_name(host_el.get_text()  if host_el  else "")
    away = normalize_team_name(guest_el.get_text() if guest_el else "")

    # Fallback: breadcrumb "Los Angeles Lakers - Houston Rockets"
    if not (home and away):
        bc = soup.select_one('[data-testid="breadcrumb-current-page"]')
        if bc:
            txt = bc.get_text(" ", strip=True)
            for sep in [" - ", " \u2013 ", " vs ", " v "]:
                if sep in txt:
                    parts = txt.split(sep, 1)
                    home = normalize_team_name(parts[0])
                    away = normalize_team_name(parts[1])
                    break

    # Horario
    gt = soup.select_one('[data-testid="game-time-item"]')
    iso = ""
    if gt:
        iso = parse_pt_datetime(gt.get_text(" ", strip=True)) or ""

    # Placar: "Los Angeles Lakers 107 \u2013 98 Houston Rockets"
    gp = soup.select_one('[data-testid="game-participants"]')
    score_home, score_away, status = "", "", "scheduled"
    if gp:
        txt = gp.get_text(" ", strip=True)
        m = re.search(r'(\d+)\s*[\u2013\u2014\-]\s*(\d+)', txt)
        if m:
            try:
                score_home = int(m.group(1))
                score_away = int(m.group(2))
                status = "finished"
            except ValueError:
                pass

    return {
        "event_datetime_utc": iso,
        "home": home,
        "away": away,
        "venue": "",
        "venue_city": "",
        "venue_country": "",
        "score_home": score_home,
        "score_away": score_away,
        "status": status,
    }
