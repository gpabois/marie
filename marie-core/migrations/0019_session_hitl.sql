-- Requêtes human-in-the-loop d'une session (voir hitl::HitlFrame et
-- session::store::SessionStore::upsert_hitl), en complément de
-- `marie_session_frames` (voir `0015_session_frame.sql`) : un `HitlFrame`
-- vit dans sa propre table plutôt que comme variante JSONB de
-- `marie_session_frames.node`, car il est mis à jour indépendamment de son
-- frame porteur (la réponse arrive après coup, sans toucher le frame).
--
-- `(session_id, id)` est la clé primaire — même logique de portée que
-- `marie_session_frames.(session_id, frame_id)` (voir `0015_session_frame.sql`) :
-- un `HitlId` n'a de sens qu'au sein de sa session. `frame_id` référence le
-- frame propriétaire (`marie_session_frames`) sans porter de contrainte de
-- clé étrangère composite : `session::hitls::Hitls` peut créer un `HitlFrame`
-- avant que son `FrameNode` correspondant ne soit lui-même persisté.
-- `frame` porte le `HitlFrame` sérialisé tel quel (données de la question et
-- réponse éventuelle), sur le même modèle que `marie_session_frames.node`.
CREATE TABLE IF NOT EXISTS marie_session_hitls (
    session_id TEXT NOT NULL,
    id TEXT NOT NULL,
    frame_id TEXT NOT NULL,
    frame JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (session_id, id),

    CONSTRAINT fk_session_hitls_session
    FOREIGN KEY (session_id) REFERENCES marie_sessions(id)
);
