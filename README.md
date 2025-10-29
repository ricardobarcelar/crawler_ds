# Pipeline de coleta e corpus (violência doméstica — atuação policial)

Este pacote baixa as URLs do `crawl_manifest.csv`, extrai o texto (HTML/PDF), faz limpeza mínima, deduplica e gera shards JSONL no formato `{"text": ...}` para pré-treinamento não supervisionado.

## Arquivos gerados aqui
- `runner.py` — script principal
- `requirements.txt` — dependências
- `crawl_manifest.csv` no mesmo diretório ou forneça o caminho via `--manifest`

## Criação do ambiente (Apenas se não for usar o venv)
```bash
conda create -n crawl python=3.12.7
conda activate crawl

pip install ipykernel
python -m ipykernel install --user --name=crawl --display-name="crawl"
```

## Instalação
```bash
#python -m venv .venv
#source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install -U pip
pip install -r requirements.txt
```

## Uso
```bash
python runner.py --manifest crawl_manifest.csv --outdir corpus_out   --rate 1.5 --max-workers 4 --min-chars 300 --shard-size-mb 100
ou
python runner_v2.py --manifest crawl_manifest.csv --outdir corpus_out \
  --rate 1.0 --max-workers 2 --timeout 15 --log-level DEBUG \
  --user-agent "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36"

```

- **rate**: req/s global (em órgãos públicos 1–2 req/s).
- **max-workers**: threads para download/extração.
- **min-chars**: ignora documentos muito curtos após limpeza (boilerplate).
- **shard-size-mb**: tamanho alvo de cada arquivo JSONL.

## Saída
```
corpus_out/
├─ data/
│  ├─ raw/     # arquivos brutos (html/pdf)
│  ├─ text/    # texto extraído e limpo (um .txt por doc)
│  └─ shards/  # JSONL para treino (corpus_0000.jsonl, ...)
└─ logs/
   ├─ run.log
   └─ report.json  # contagem de sucessos, descartes, shards gerados
```

## Observações
- Respeito a **robots.txt** (estratégia permissiva se indisponível).
- PDFs usam **PyMuPDF** (fallback **pdfminer.six**). 
- HTML usa **trafilatura** (fallback BeautifulSoup).
- Limpeza inclui des-hifenização básica, normalização de espaços/linhas e anonimização leve de **e-mail/CPF/CNPJ**.
- Para auditoria, todo **texto** tem um arquivo `.txt` em `data/text/`, e os **brutos** ficam em `data/raw/`.
