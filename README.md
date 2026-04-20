# legalize-co

Colombian legislation in Markdown, versioned as git. Each law is a file, each reform is a commit.

**~N laws** · `pais: co` · Source: [SUIN-Juriscol](https://www.suin-juriscol.gov.co/) (Ministry of Justice)

## Coverage

Document types: `ley`, `decreto`, `resolucion`, `decreto_ley`, `acto_legislativo`  
Period: 1887 – present  
Status: `vigente`, `derogada`, `parcialmente_derogada`

## Structure
laws/co/<IDENTIFICADOR>.md

text

Identifier format: `<RANGO>-<NUMBER>-<YEAR>` — e.g. `LEY-57-1887`, `DECRETO-2811-1974`

## Running the pipeline

```bash
pip install -r requirements.txt

# Scrape a single known law by URL
python pipeline/fetch.py --seed-url "https://www.suin-juriscol.gov.co/viewDocument.asp?id=1789030"

# Enumerate a range of SUIN IDs (full corpus)
python pipeline/fetch.py --enumerate --id-start 1700000 --id-end 1800000 --checkpoint checkpoint.json

# Commit all scraped laws with historical dates
python pipeline/commit.py
```

## Known limitations

- SUIN's internal SOAP search endpoint (`/CiclopeWs/Ciclope.svc/Find`) returns invalid XML — full-corpus discovery uses ID range enumeration instead.
- SUIN TLS certificate chain is broken locally; use `--no-verify-tls` flag.

## Part of Legalize

This repo is part of [legalize](https://github.com/legalize-dev/legalize) — legislation as code.
