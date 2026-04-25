"""
Merge de CSVs de shards de fetch_real_odds.py num CSV final.

Uso:
    python merge_real_odds.py <shards_dir> <output_csv>
"""
import sys
from pathlib import Path

import pandas as pd


def main():
    if len(sys.argv) < 3:
        print("Uso: python merge_real_odds.py <shards_dir> <output_csv>")
        sys.exit(1)

    shards_dir = Path(sys.argv[1])
    output_csv = Path(sys.argv[2])

    csvs = sorted(shards_dir.rglob("*.csv"))
    if not csvs:
        print(f"[ERRO] Nenhum .csv encontrado em {shards_dir}")
        sys.exit(2)

    print(f"[merge] {len(csvs)} CSVs:")
    dfs = []
    for c in csvs:
        try:
            df = pd.read_csv(c)
            print(f"  -> {c.name}: {len(df)} linhas")
            dfs.append(df)
        except Exception as e:
            print(f"  [WARN] erro lendo {c}: {e}")

    if not dfs:
        print("[ERRO] Todos os CSVs vazios ou invalidos")
        sys.exit(3)

    combined = pd.concat(dfs, ignore_index=True)
    # Dedup: mantem ultima entrada para cada (date, home, away, market)
    combined = combined.drop_duplicates(
        subset=["match_date", "home_team", "away_team", "market"],
        keep="last",
    )

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    combined.to_csv(output_csv, index=False)

    print(f"\n[merge] Concluido!")
    print(f"  Output:    {output_csv}")
    print(f"  Linhas:    {len(combined)}")
    print(f"  Partidas:  {combined[['match_date','home_team','away_team']].drop_duplicates().shape[0]}")
    print(f"  Mercados:  {combined['market'].nunique()}")


if __name__ == "__main__":
    main()
