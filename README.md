# Tennis Scraper Runner

Roda raspagem de odds de tenis em paralelo via GitHub Actions matrix.

## Como usar

1. Va na aba **Actions**
2. Selecione **Tennis Scrape (parallel)** na lista de workflows
3. Clique em **Run workflow** (lado direito)
4. Aguarde 4-6h (20 shards rodando em paralelo)
5. Baixe o artifact **tennis-history-final** (DB SQLite com tudo mergeado)

## Estrutura

- `astrology/scrapers/oddsportal/` - codigo do scraper
- `.github/workflows/tennis-scrape.yml` - workflow de 3 fases (discover -> 20 shards -> merge)

## Pos-coleta

Apos coletar os dados, este repo pode ser arquivado/privado.
O DB final foi baixado e nao precisa mais ficar publico.
