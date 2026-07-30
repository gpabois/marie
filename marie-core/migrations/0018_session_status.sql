-- Ajoute le statut de haut niveau d'une session (voir
-- session::model::SessionStatus) à marie_sessions — décorrélé de
-- session::frames::FrameStatus, qui suit l'état d'un frame individuel dans
-- marie_session_frames (voir 0015_session_frame.sql), pas la session dans
-- son ensemble.
--
-- Stocké en JSONB, comme node/data des autres tables de ce module, plutôt
-- qu'un TEXT à discriminant seul : SessionStatus::Failed porte un message
-- d'erreur, pas seulement un nom de variante.
ALTER TABLE marie_sessions ADD COLUMN IF NOT EXISTS status JSONB NOT NULL DEFAULT '"Pending"'::jsonb;
