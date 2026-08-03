-- Journal d'une session (voir session::logs::SessionLog et
-- session::store::SessionStore::{insert_log,list_log,list_log_after}), en
-- complément de `marie_session_frames`/`marie_session_hitls` (voir 0005/
-- 0007) : une entrée par `SessionLogId`, pas un tableau JSONB unique sur
-- `marie_sessions`, puisqu'un même id peut recevoir plusieurs écritures
-- successives (voir la doc de `SessionLog` — texte ajouté au fil de l'eau,
-- pas seulement des lignes complètes ajoutées une fois pour toutes).
--
-- `(session_id, id)` est la clé primaire — même logique de portée que
-- `marie_session_frames.(session_id, frame_id)` (voir 0005) : un
-- `SessionLogId` n'a de sens qu'au sein de sa session. `content` porte le
-- `SessionLogContent` sérialisé tel quel (texte brut, log de tool, log
-- d'agent) plutôt qu'une colonne par variante.
CREATE TABLE IF NOT EXISTS marie_session_logs (
    session_id TEXT NOT NULL,
    id TEXT NOT NULL,
    content JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (session_id, id),

    CONSTRAINT fk_session_logs_session
    FOREIGN KEY (session_id) REFERENCES marie_sessions(id)
);

-- Sert `list_log`/`list_log_after`, toutes deux triées et filtrées sur
-- `created_at` pour une session donnée.
CREATE INDEX IF NOT EXISTS marie_session_logs_created_idx ON marie_session_logs (session_id, created_at);
