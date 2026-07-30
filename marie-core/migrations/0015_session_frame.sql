-- Stockage par-frame d'une session (voir session::store::SessionStore::
-- get_frame/upsert_frame/delete_frame), en complément de la colonne `frames`
-- de `marie_sessions` (voir `0014_session_tree.sql`) : contrairement à la
-- session entière (remplacée en bloc, voir `SessionStore::upsert_session`),
-- un frame est amené à être lu/écrit individuellement le long de son
-- exécution (voir `session::controller::SessionHandler`), d'où une ligne par
-- frame plutôt qu'un `ON CONFLICT` sur tout l'arbre.
--
-- `(session_id, frame_id)` est la clé primaire : un `FrameId` n'a de sens
-- qu'au sein de sa session, donc c'est la paire — pas `frame_id` seul — qui
-- doit être unique (même logique que `fs_alias.(scope, from_path)`, voir
-- `0010_fs_alias.sql`). `node` porte le `FrameNode` sérialisé tel quel
-- (statut, politique, canaux hérités, liens d'arbre) plutôt que d'éclater
-- chaque champ en colonne, puisque `session::frames` seul en possède les
-- champs privés (liens parent/siblings/enfants).
CREATE TABLE IF NOT EXISTS marie_session_frames (
    session_id TEXT NOT NULL,
    frame_id TEXT NOT NULL,
    node JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (session_id, frame_id),

    CONSTRAINT fk_session_frames_session
    FOREIGN KEY (session_id) REFERENCES marie_sessions(id)
);
