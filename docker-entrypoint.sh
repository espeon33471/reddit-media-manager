#!/bin/sh
set -e

# App-user waar de daadwerkelijke app op draait (niet root).
RMM_USER="${RMM_USER:-rmm}"

# Waarom: Docker (vooral Docker Desktop) kan een (named) volume bij initiatie
# root-eigend laten zijn, ondanks het `chown` in de Dockerfile. Zolang /config
# of /data niet door de app-user geschreven kan worden, faalt het opslaan van
# settings/state met een 500 (PermissionError). We reparen het eigenaarschap
# daarom bij elke start, mits de container als root is gestart.
if [ "$(id -u)" = "0" ]; then
    chown -R "$RMM_USER:$RMM_USER" /config /data
    # Privileges verlagen naar de app-user en de CMD (uvicorn) starten.
    exec setpriv --reuid="$RMM_USER" --regid="$RMM_USER" --init-groups "$@"
fi

# Reeds niet-root (bijv. `docker run --user ...`): gewoon de CMD starten.
exec "$@"
