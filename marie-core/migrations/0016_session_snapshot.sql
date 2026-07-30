-- Clichés versionnés d'un frame (voir session::snapshot::Snapshot et
-- session::store::SessionStore::{latest_snapshot,snapshot_at,upsert_snapshot}),
-- en complément de `marie_session_frames` (voir `0015_session_frame.sql`) :
-- un frame a un seul état courant, mais accumule un cliché par `superstep`
-- au fil de son exécution (voir `Snapshot::superstep`), d'où sa propre table
-- plutôt qu'une colonne de plus sur `marie_session_frames`.
--
-- `(session_id, frame_id, superstep)` est la clé primaire : c'est ce
-- triplet — ni `frame_id` seul, ni `(session_id, frame_id)` — qui identifie
-- un cliché de façon unique, puisque plusieurs supersteps coexistent pour un
-- même frame (même logique de portée que `marie_session_frames.(session_id,
-- frame_id)`, voir `0015_session_frame.sql`). `superstep` est stocké en
-- BIGINT (Postgres n'a pas d'entier non signé) alors que le domaine le porte
-- en `u32` (voir `Snapshot::superstep`) — seul `data`, qui porte le
-- `Snapshot` sérialisé tel quel, garde le type d'origine.
CREATE TABLE IF NOT EXISTS marie_session_snapshots (
    session_id TEXT NOT NULL,
    frame_id TEXT NOT NULL,
    superstep BIGINT NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (session_id, frame_id, superstep),

    CONSTRAINT fk_session_snapshots_session
    FOREIGN KEY (session_id) REFERENCES marie_sessions(id)
);

-- Sert `SessionStore::latest_snapshot` (plus grand `superstep` pour un
-- `(session_id, frame_id)` donné) sans scanner toute la table.
CREATE INDEX IF NOT EXISTS marie_session_snapshots_latest_idx
    ON marie_session_snapshots (session_id, frame_id, superstep DESC);
