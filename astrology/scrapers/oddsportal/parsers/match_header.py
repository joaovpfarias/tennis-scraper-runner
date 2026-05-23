"""
Extrai informacoes do header da match page do oddsagora.com.br.

Seletores (2026-05):
  [data-testid="game-host"]      - jogador/time da casa
  [data-testid="game-guest"]     - jogador/time visitante
  [data-testid="game-time-item"] - "Sabado, 01 Out 2022, 10:10"
  [data-testid="live-info"]      - "Resultado final <strong>2:1</strong> (sets/placar)"
                                   Para esportes com placar simples (futebol/basquete):
                                   texto livre com padrao "N - N" ou "N:N"
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

    # Fallback: breadcrumb
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

    score_home, score_away, status = "", "", "scheduled"

    # Placar via live-info: "Resultado final <strong>2:1</strong> (sets detail)"
    # Funciona para tenis (sets vencidos), futebol, basquete, etc.
    li = soup.select_one('[data-testid="live-info"]')
    if li:
        strong = li.find("strong")
        if strong:
            score_txt = strong.get_text(strip=True)
            # Formato sets/placar: "2:1", "107:98", "2-1", "107-98"
            m = re.match(r'^(\d+)\s*[:\-]\s*(\d+)$', score_txt)
            if m:
                try:
                    score_home = int(m.group(1))
                    score_away = int(m.group(2))
                    status = "finished"
                except ValueError:
                    pass
        # Fallback: texto livre do live-info sem <strong>
        if status == "scheduled":
            li_text = li.get_text(" ", strip=True)
            m = re.search(r'(\d+)\s*[:\-]\s*(\d+)', li_text)
            if m and any(kw in li_text for kw in ["Resultado", "Result", "Final", "final"]):
                try:
                    score_home = int(m.group(1))
                    score_away = int(m.group(2))
                    status = "finished"
                except ValueError:
                    pass

    # Fallback: game-participants texto livre (futebol/basquete: "Lakers 107 - 98 Rockets")
    if status == "scheduled":
        gp = soup.select_one('[data-testid="game-participants"]')
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
