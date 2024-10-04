# adsb-extract
Create data extracts from ADSB historical archive

```bash
mkdir data
cd data
wget https://github.com/adsblol/globe_history_2024/releases/download/v2024.10.03-planes-readsb-prod-0/v2024.10.03-planes-readsb-prod-0.tar.aa
wget https://github.com/adsblol/globe_history_2024/releases/download/v2024.10.03-planes-readsb-prod-0/v2024.10.03-planes-readsb-prod-0.tar.ab
```

Extract these archives:
```bash
cat v2024.10.03-planes-readsb-prod-0.tar.aa v2024.10.03-planes-readsb-prod-0.tar.ab | tar zxf -
```

Free up space from data archives

```bash
rm v2024.10.03-planes-readsb-prod-0.tar.aa v2024.10.03-planes-readsb-prod-0.tar.ab
cd ..
```

Select only the traces that start or end in JFK/LGA/EWR.

```bash
uv run python filter_bbox.py
```

Then collect the data into a Parquet file:

```bash
uv run python read_trace.py
```

The generated Parquet file will be in `data/`.
