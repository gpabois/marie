use marie_core::agent::context::ContextEntry;
use marie_core::agent::frame::AgentFrame;
use marie_core::agent::role::Role;
use marie_core::agent::status::AgentStatus;
use marie_core::id::{ID, IdGenerator};
use marie_core::session::SessionApi;
use marie_core::session::crdt::YrsSession;
use yrs::{Doc, StateVector, Transact, updates::decoder::Decode};

fn frame(session_id: ID, local_id: ID) -> AgentFrame {
    AgentFrame {
        session_id,
        id: local_id,
        model: "gpt-test".to_string(),
        status: AgentStatus::Initial,
        allowed_tools: vec!["search".to_string()],
        context: vec![ContextEntry { role: Role::User, content: "bonjour".to_string() }].into(),
        stdio: String::new(),
        stderr: String::new(),
    }
}

#[test]
fn test_put_and_read_frame() {
    let ids = IdGenerator::default();
    let session_id = ids.next_id();
    let local_id = ids.next_id();

    let mut session = YrsSession::new(session_id);
    session.put_frame(local_id, &frame(session_id, local_id)).unwrap();
    session.append_stdio(local_id, "salut").unwrap();
    session.push_context_entry(local_id, &ContextEntry { role: Role::Assistant, content: "salut !".to_string() }).unwrap();

    let got = session.frame(local_id).unwrap();
    assert_eq!(got.model_id, "gpt-test");
    assert_eq!(got.stdio, "salut");
    assert_eq!(got.context.len(), 2);
    assert_eq!(got.context[1].content, "salut !");
}

#[test]
fn test_sync_via_diff() {
    let ids = IdGenerator::default();
    let session_id = ids.next_id();
    let local_id = ids.next_id();

    let mut owner = YrsSession::new(session_id);
    owner.put_frame(local_id, &frame(session_id, local_id)).unwrap();
    owner.append_stdio(local_id, "partie 1").unwrap();

    // Le nouveau worker part d'un vecteur d'état vide (jamais vu cette session) :
    // il ne doit pas appeler `new` (qui créerait sa propre racine, en conflit avec
    // celle reçue) mais reconstruire son handle depuis un `Doc` vierge une fois le
    // diff appliqué — voir `YrsSession::open`.
    let remote_sv = StateVector::default();
    let diff = owner.diff_since(&remote_sv);

    let remote_doc = Doc::new();
    remote_doc.transact_mut().apply_update(yrs::Update::decode_v1(&diff).unwrap()).unwrap();
    let mut receiver = YrsSession::open(remote_doc).unwrap();

    let got = receiver.frame(local_id).unwrap();
    assert_eq!(got.stdio, "partie 1");

    // Nouvelle écriture côté propriétaire d'origine : seul le delta doit transiter.
    owner.append_stdio(local_id, " partie 2").unwrap();
    let diff2 = owner.diff_since(&receiver.state_vector());
    receiver.apply_diff(&diff2).unwrap();

    assert_eq!(receiver.frame(local_id).unwrap().stdio, "partie 1 partie 2");
}

#[test]
fn test_open_round_trip() {
    let ids = IdGenerator::default();
    let session_id = ids.next_id();
    let local_id = ids.next_id();

    let mut session = YrsSession::new(session_id);
    session.put_frame(local_id, &frame(session_id, local_id)).unwrap();

    let diff = session.diff_since(&StateVector::default());
    let doc = Doc::new();
    doc.transact_mut().apply_update(yrs::Update::decode_v1(&diff).unwrap()).unwrap();

    let reopened = YrsSession::open(doc).unwrap();
    assert_eq!(reopened.id(), session_id);
    assert_eq!(reopened.frame(local_id).unwrap().model_id, "gpt-test");
}
