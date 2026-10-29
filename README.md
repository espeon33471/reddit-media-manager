# Reddit Media Manager

Zelfstandige download-manager die per subreddit **alle media-items** (afbeeldingen, GIFs, video's)
uit posts downloadt — géén tekstposts, géén comments, géén HTML-pagina's.
NSFW/age-gated en private subreddits worden ondersteund via je eigen Reddit-sessie-cookie.

**Stack:** Python 3.12 · FastAPI · Jinja2 · SQLite (aiosqlite) · httpx · ffmpeg — alles in één container.

## Functies

- **Subreddit-detectie**: automatisch je gesubscribde subreddits ophalen (ook NSFW/age-gated en private), of bulk-importeren
- **Media-download**: foto's, video's en GIFs per post (per type in/uitschakelbaar)
- **Uniform formaat & kwaliteit**: foto's → JPEG (qualiteit 95), video's → MP4 (CRF 18), GIFs blijven geanimeerde GIFs (of converteren naar video)
- **Vlakke opslag**: `<dataroot>/u_<subreddit>/<bestand>`, optioneel maand-submappen of per-post mappen met `meta.json`
- **Deduplicatie op 3 niveaus**: post-id, URL én sha256-inhoud (ook tussen subreddits)
- **Web UI** met tabs: Dashboard, Subreddits, Queue, History, Galerij (met preview) en Settings
- **Auto-sync** (optioneel: elke X minuten)
- **Vriendelijke rate-limiting**: 6 s tussen API-aanroepen, concurrentie 4, backoff bij 429/503

---

## Snel starten (Docker)

```bash
docker compose up -d --build
```

En open <http://127.0.0.1:8000>.

### docker-compose.yml

```yaml
services:
  reddit-media-manager:
    build: .
    container_name: reddit-media-manager
    ports:
      - "8000:8000"          # links: lokale poort — wijzig bij conflict
    environment:
      - RMM_CONFIG=/config   # settings.json, state.db, app.log
      - RMM_DATAROOT=/data   # gedownloade media
    volumes:
      - rmm-config:/config
      - rmm-data:/data
    restart: unless-stopped

volumes:
  rmm-config:
  rmm-data:
```

### Alternatieven

**Bestaande image** (GitHub Container Registry):

```bash
docker pull ghcr.io/espeon33471/reddit-media-manager:latest
docker run -d --name reddit-media-manager -p 8000:8000 \
    -v rmm-config:/config -v rmm-data:/data --restart unless-stopped \
    ghcr.io/espeon33471/reddit-media-manager:latest
```

**Zelf bouwen**, zonder compose:

```bash
docker build -t reddit-media-manager .
docker run -d --name reddit-media-manager -p 8000:8000 \
    -v rmm-config:/config -v rmm-data:/data --restart unless-stopped reddit-media-manager
```

**Zonder Docker:**

```bash
python3.12 -m venv .venv && .venv\Scripts\activate   # Windows (of: source .venv/bin/activate)
pip install -r requirements.txt
uvicorn app:app --host 127.0.0.1 --port 8000
```

**Details:**

- **Config (`/config`)**: `settings.json`, `state.db` en `app.log` — de toestand
  van de app zelf. Volume `rmm-config` bewaart ze.
- **Media (`/data`)**: daarin staan de gedownloade bestanden
  (`/data/u_<sub>/…`). Volume `rmm-data` bewaart ze.
- **Media op eigen pad**: mount in plaats van een named volume een host-map,
  bv. `-v D:\Media\reddit:/data` (Windows/Docker Desktop) of
  `-v /pad/naar/media:/data` (Linux). Op Linux moet die map schrijfbaar zijn
  voor de container-user, bv. `chown 1000:1000 /pad/naar/media`.
- **Poort**: bij een conflict de linkszijde van `"8000:8000"` in
  `docker-compose.yml` wijzigen.
- **Logs**: `docker logs -f reddit-media-manager` (ook in `/config/app.log`).
- **Healthcheck** zit in de Dockerfile: `docker ps` toont `healthy` zodra de
  API antwoordt. De container draait niet als root (user `rmm`).

---

## Inloggen (sessie-cookie)

Deze app logt **niet** in met gebruikersnaam/wachtwoord en gebruikt **geen OAuth**.
Je logt in met de `reddit_session`-cookie uit je eigen browser — daarmee kan de
app ook NSFW/age-gated en private subreddits lezen.

1. Log in op reddit.com in je browser.
2. Open DevTools (F12) → **Application** → **Cookies** → `https://www.reddit.com`.
3. Kopieer de waarde van `reddit_session` (optioneel ook `loid`).
4. Plak die in **Settings → Reddit (sessie-cookie)** en sla op. De app
   verifieert direct via `GET /api/v1/me` en slaat de cookie **lokaal** op in
   `settings.json` (in het config-volume `/config`). De waarde wordt nooit verstuurd naar
   een andere server dan reddit.com.

> Plakken in de vorm `reddit_session=eyJ…` wordt ook herkend.
> Sessie verloopt of wachtwoord gewijzigd? → nieuwe cookie plakken.

## Rate limiting & retry

- Max. **~10 Reddit-API-aanroepen/minuut** (standaard 6 s tussen aanroepen,
  instelbaar in Settings). Media-bestanden (CDN) tellen hier niet mee.
- `429`/`503`: `Retry-After` wordt gerespecteerd, anders exponentiële backoff,
  max. **5 pogingen**.
- De app gebruikt de door jou geconfigureerde User-Agent (standaard een
  browser-UA) en stuurt downloads met `Referer: https://www.reddit.com/`.

## Gebruik (web UI)

Tabs: **Dashboard · Subreddits · Queue · History · Galerij · Settings**.

1. **Subreddits**:
   - **Bulk-import**: plak namen (één per regel, of komma-geleid) of upload een
     `.txt`/`.csv`-bestand. `r/`-prefixen en `reddit.com/r/…`-URL's worden herkend.
   - **Detecteer gesubscribed**: haalt je aangesloten subreddits op (ook NSFW/private).
   - Per sub instellen: **enable/disable**, **max posts** (`0` = alles),
     **voorbij datum** (laat oudere posts over), **sorteer** (`new`/`top`/`hot`).
   - Acties per sub:
     - **Sync** — alleen nieuwe posts scannen en in de wachtrij zetten;
     - **Download nu** — sync + workers direct starten;
     - **Resync** — opnieuw scannen vanaf het begin (duplicaten blijven uit).
2. **Queue**: live lijst (auto-vernieuwing), per item status, pogingen en
   foutmelding; **Opnieuw** voor failed/skipped items.
3. **History**: gedownloade posts per sub/datum met aantal bestanden;
   filteren op sub; kolommen sorteerbaar; entries verwijderen (bestanden blijven staan).
4. **Galerij**: grid-preview van alle gedownloade media met sub/type-filter
   en click-to-voorbekijk (afbeeldingen, GIFs en video's).
5. **Settings**: alle instellingen (zie tabel hieronder) + sessie-cookie.
6. **Dashboard**: status, counters per queue-status, download-snelheid en
   recente fouten; globale **Start / Pauzeer / Hervat / Stop**.

### Gedrag pauze/resume/stop

- **Pauze** onderbreekt ook * lopende * downloads (check per chunk) en zet het
  item terug op `pending`. **Hervat** gaat exact verder waar het stopte.
- **Stop** stopt de engine; de wachtrij blijft in de DB.
- De queue staat in SQLite → na een crash of herstart hervat de app automatisch
  (`running`-items worden `pending`, de engine start bij het opstarten).

### Headless (zonder UI)

```bash
python example_sync.py aww                 # alleen nieuwe posts
python example_sync.py MechanicalGulls --max-posts 5
python example_sync.py pics --full         # resync vanaf het begin
```

---

## Opslagstructuur

Standaard (**vlak**, `naming_mode: flat`):

```
<dataroot>/                    (standaard /data; instelbaar)
  u_<subreddit>/
    01.jpg
    02.jpg
    01.mp4
    01.gif
```

Opties:

- `date_subfolder: month` → tussendoor een maandmap: `u_<sub>/2026-03/01.jpg`
- `naming_mode: posts` → per post een map met de posttitel, plus `meta.json`:

```
u_<subreddit>/
  2026-03-12-Some-post-title/
    01.jpg
    02.mp4
    meta.json          # {title, author, created_utc, permalink, sub, urls[]}
```

Bestandsnamen worden gesaneerd (geen spaties/special chars).

## Media-conversie

- **Foto's** → JPEG met instelbare kwaliteit (standaard **95**). Bronformaat
  (png/webp/jpg) is dus niet bepalend voor de opslag.
- **Video's** → MP4 (H.264, `-preset slow`, instelbare **CRF 18** ≈ visueel
  verliesloos), met audio (AAC) en `+faststart`.
- **GIFs** → standaard **blijven geanimeerde GIFs** (`gif_mode: keep`);
  `gif_mode: convert` zet ze om naar het video-formaat.
- Conversie gebeurt pas **na** een geslaagde download (sha256-gecontroleerd);
  bij een download-fout blijft het oorspronkelijke bestand staan.

## Media-extractie (prioriteit per post)

1. `data.media.reddit_video.fallback_url` → video;
2. `data.preview.images[].source.url` → afbeeldingen/GIFs
   (`:placeholder` wordt `:medium`);
3. `post_url` eindigt op `.jpg .jpeg .png .webp .gif .mp4 .mov .m4v` → directe link.

Geen van de drie → post wordt geteld als **skipped** ("geen media"), niet als fout.

## Deduplicatie (SQLite)

Drie niveaus, dus ook **tussen verschillende subreddits** geen dubbele bestanden:

- **Post-level**: staat `post_id` al in `downloads` → hele post wordt overgeslagen;
- **URL-level**: dezelfde URL al `done` → item wordt `skipped` (geen download);
- **Inhoud-level**: na download wordt de sha256 geverifieerd; bestaat een
  identiek bestand al (andere URL/subreddit)? → nieuw bestand wordt verwijderd, item `skipped`.

Database `state.db` in de configmap (in Docker: `/config/state.db`). Tabellen:

| Tabel | Inhoud |
|---|---|
| `downloads` | id, sub, post_id, url, sha256, local_path, downloaded_at, status, attempts, error, post_date — **de queue én de history** |
| `sub_state` | sub, last_after_cursor, last_post_id, last_synced_at |
| `subs` | sub, enabled, max_posts, date_from, sort |
| `settings` | key/value-instellingen |

## Instellingen (Settings-tab)

| Instelling | Standaard | Toelichting |
|---|---|---|
| `dataroot` | `/data` | Map waar `u_<sub>/…` komt te staan (default `/data`) |
| `concurrency` | `4` | Aantal gelijktijdige downloads |
| `request_interval` | `6.0` s | Wachtijd tussen Reddit-API-aanroepen |
| `user_agent` | browser-UA | User-Agent voor alle requests |
| `reddit_session` / `reddit_loid` | — | Sessie-cookie (inloggen) |
| `file_types` | photo+video+gif | Welke types downloaden |
| `photo_format` | `jpeg` | Doel-formaat foto's |
| `video_format` | `mp4` | Doel-formaat video's |
| `gif_mode` | `keep` | GIFs behouden of converteren (`convert`) |
| `naming_mode` | `flat` | `flat` of `posts` (per-post mappen + meta.json) |
| `date_subfolder` | `none` | `none` of `month` |
| `photo_quality` | `95` | JPEG-kwaliteit (1–100) |
| `video_quality` | `18` | CRF (lager = beter) |
| `auto_sync_minutes` | `0` (uit) | Automatisch syncen elke X minuten |

## Logging

Console + roterend `app.log` (2 MB × 3) in de configmap. Elke queue-item-fout
wordt gelogd én in de UI getoond; workers sterven nooit per item.

## Troubleshooting

| Symptoom | Oorzaak/actie |
|---|---|
| `403 Verboden — NSFW/age-gated of private` | Sessie-cookie ontbreekt of is verlopen: Settings → nieuwe `reddit_session` plakken. |
| `404 Niet gevonden` | Subredditnaam verkeerd of sub bestaat niet. |
| Veel `skipped · geen media` in queue | Subreddit met veel tekstposts — dat is beoogd. |
| Downloads staan stil | Rate-limit/backoff: zie `app.log`; eventueel netwerk. |
| `401` bij reads | Sessie verlopen: nieuwe `reddit_session`-cookie plakken. |
| Dubbele bestanden? | Moeten niet voorkomen; controleer of je niet twee draaiende instanties met dezelfde dataroot hebt. |
| Docker: data verdwijnt na `docker rm` | Volumes niet gemount — gebruik `docker-compose.yml` of `-v rmm-config:/config -v rmm-data:/data`. |

## Projectstructuur

```
reddit-media-manager/
├── app.py                 # FastAPI-app: pagina's + API + engine-lifecycle
├── example_sync.py        # werkend headless-voorbeeld
├── requirements.txt
├── Dockerfile             # container-image (python:3.12-slim + ffmpeg, niet-root, healthcheck)
├── docker-compose.yml     # compose: poort 8000, volumes rmm-config → /config en rmm-data → /data
├── .dockerignore          # houdt runtime-data (state.db, downloads, …) buiten de image
├── .gitignore
├── README.md
├── rmm/
│   ├── config.py          # instellingen + defaults
│   ├── db.py              # schema + async SQLite-wrapper
│   ├── engine.py          # workers, pauze/resume/stop, snelheid, dedup
│   ├── reddit.py          # API-client: sessie-cookie-auth, rate-limit, retry, paginatie
│   ├── media.py           # media-extractie per post
│   ├── downloader.py      # gestreamde downloads, sha256, pauze-check
│   ├── convert.py         # conversie foto→JPEG / video→MP4 / GIF (ffmpeg)
│   ├── sync.py            # sync-logica + opslagstructuur + subbeheer
│   └── util.py            # logging, sanitiseren, tijdsstempels
├── templates/             # Jinja2-pagina's (dashboard/subs/history/queue/gallery/settings)
└── static/                # style.css + app.js (geen build-stap)
```

---

> **Disclaimer**: voor persoonlijk gebruik. Eerbiedig de [API-regels](https://support.reddithelp.com/hc/en-us/articles/42728983564564)
> van Reddit (rate-limits, eerlijke User-Agent, geen misbruik) en download uitsluitend
> waarvoor je rechten hebt.
