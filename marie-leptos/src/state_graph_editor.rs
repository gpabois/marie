use leptos::{html, prelude::*};
use wasm_bindgen::JsCast as _;
use web_sys::{CanvasRenderingContext2d, HtmlCanvasElement, MouseEvent};

use crate::types::{EdgeView, NodeView};

const NODE_WIDTH: f64 = 140.0;
const NODE_HEIGHT: f64 = 48.0;
const NODE_RADIUS: f64 = 8.0;
/// Rayon de la poignée de connexion, dessinée à droite du nœud (voir
/// [`draw_node`]) — c'est en la saisissant qu'on tire une arête vers un
/// autre nœud, par opposition à saisir le corps du nœud (voir
/// [`on_mouse_down`]) qui le déplace.
const HANDLE_RADIUS: f64 = 7.0;
const HANDLE_HIT_RADIUS: f64 = HANDLE_RADIUS + 4.0;
const HANDLE_OFFSET_X: f64 = NODE_WIDTH / 2.0 + 18.0;
const EDGE_HIT_DISTANCE: f64 = 6.0;

/// Éditeur visuel d'un `marie_core::mode::state_graph::StateGraph`, rendu
/// sur un `<canvas>` : double-clic sur une zone vide pour créer un nœud,
/// glisser depuis sa poignée (le petit cercle à sa droite) jusqu'à un autre
/// nœud pour les relier, glisser depuis son corps pour le déplacer,
/// clic-droit sur un nœud ou une arête pour le/la supprimer.
///
/// `nodes`/`edges` sont des `RwSignal` plutôt que des `Signal` en lecture
/// seule : c'est délibérément l'appelant qui les possède (comme partout
/// ailleurs dans ce crate, voir `session_panel` — headless au sens où ce
/// composant ne détient aucun état caché), pour qu'il puisse les lire/écrire
/// depuis sa propre UI en plus de cet éditeur — ex. un panneau d'inspection
/// pour renommer un nœud, éditer son `action`/le `guard` d'une arête, ou
/// choisir le nœud d'entrée (voir `entry`, affiché mais non modifiable
/// depuis le canvas : aucune des interactions ci-dessus ne le change).
///
/// Ce composant ne crée que des nœuds/arêtes vierges (`action`/`guard` à
/// `None`, `id` généré) : leur donner un contenu réel reste à la charge du
/// consommateur, via les mêmes signaux.
#[component]
pub fn StateGraphEditor(
    #[prop(into)] nodes: RwSignal<Vec<NodeView>>,
    #[prop(into)] edges: RwSignal<Vec<EdgeView>>,
    /// Nœud d'entrée du graphe, mis en évidence — voir la doc de [`Self`].
    #[prop(into, default = Signal::stored(None))]
    entry: Signal<Option<String>>,
    #[prop(default = 900.0)] width: f64,
    #[prop(default = 600.0)] height: f64,
) -> impl IntoView {
    let canvas_ref = NodeRef::<html::Canvas>::new();
    let drag = RwSignal::<Option<DragState>>::new(None);

    // Redessine à chaque changement de nœuds/arêtes/glisser-déposer en
    // cours, ou dès que le canvas est monté (`canvas_ref` est lui-même
    // réactif : `None` au premier passage, avant montage, puis `Some` une
    // fois prêt, ce qui redéclenche cet effet).
    Effect::new(move |_| {
        let Some(canvas) = canvas_ref.get() else { return };
        let ctx = context_2d(&canvas);
        draw(&ctx, width, height, &nodes.get(), &edges.get(), entry.get().as_deref(), drag.get().as_ref());
    });

    let on_mouse_down = move |ev: MouseEvent| {
        let (x, y) = local_point(&ev);
        let current_nodes = nodes.get_untracked();

        if let Some(index) = handle_at(&current_nodes, x, y) {
            drag.set(Some(DragState::DrawingEdge { from_index: index, cursor: (x, y) }));
        } else if let Some(index) = node_at(&current_nodes, x, y) {
            let node = &current_nodes[index];
            drag.set(Some(DragState::MovingNode { index, grab_dx: x - node.x, grab_dy: y - node.y }));
        }
    };

    let on_mouse_move = move |ev: MouseEvent| {
        let Some(state) = drag.get_untracked() else { return };
        let (x, y) = local_point(&ev);

        match state {
            DragState::MovingNode { index, grab_dx, grab_dy } => {
                nodes.update(|list| {
                    if let Some(node) = list.get_mut(index) {
                        node.x = x - grab_dx;
                        node.y = y - grab_dy;
                    }
                });
            }
            DragState::DrawingEdge { from_index, .. } => {
                drag.set(Some(DragState::DrawingEdge { from_index, cursor: (x, y) }));
            }
        }
    };

    let on_mouse_up = move |ev: MouseEvent| {
        if let Some(DragState::DrawingEdge { from_index, .. }) = drag.get_untracked() {
            let (x, y) = local_point(&ev);
            let current_nodes = nodes.get_untracked();

            if let Some(target_index) = node_at(&current_nodes, x, y) {
                if target_index != from_index {
                    let from = current_nodes[from_index].id.clone();
                    let to = current_nodes[target_index].id.clone();
                    let already_exists = edges.get_untracked().iter().any(|edge| edge.from == from && edge.to == to);
                    if !already_exists {
                        edges.update(|list| list.push(EdgeView { from, to, guard: None }));
                    }
                }
            }
        }

        drag.set(None);
    };

    let on_dbl_click = move |ev: MouseEvent| {
        let (x, y) = local_point(&ev);
        let current_nodes = nodes.get_untracked();

        // Double-clic sur un nœud existant : pas de création, seulement sur
        // une zone vide.
        if node_at(&current_nodes, x, y).is_some() {
            return;
        }

        let id = unique_node_id(&current_nodes);
        nodes.update(|list| list.push(NodeView { id, x, y, action: None }));
    };

    let on_context_menu = move |ev: MouseEvent| {
        ev.prevent_default();
        let (x, y) = local_point(&ev);
        let current_nodes = nodes.get_untracked();

        if let Some(index) = node_at(&current_nodes, x, y) {
            let id = current_nodes[index].id.clone();
            nodes.update(|list| list.retain(|node| node.id != id));
            edges.update(|list| list.retain(|edge| edge.from != id && edge.to != id));
            return;
        }

        if let Some(index) = edge_at(&current_nodes, &edges.get_untracked(), x, y) {
            edges.update(|list| {
                list.remove(index);
            });
        }
    };

    view! {
        <canvas
            node_ref=canvas_ref
            width=width as u32
            height=height as u32
            on:mousedown=on_mouse_down
            on:mousemove=on_mouse_move
            on:mouseup=on_mouse_up
            on:mouseleave=move |_| drag.set(None)
            on:dblclick=on_dbl_click
            on:contextmenu=on_context_menu
        />
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum DragState {
    MovingNode { index: usize, grab_dx: f64, grab_dy: f64 },
    DrawingEdge { from_index: usize, cursor: (f64, f64) },
}

/// Position de `ev`, relative au coin haut-gauche de son élément cible
/// (`offsetX`/`offsetY` : pas besoin de `getBoundingClientRect`, le
/// navigateur fait déjà ce calcul pour nous).
fn local_point(ev: &MouseEvent) -> (f64, f64) {
    (f64::from(ev.offset_x()), f64::from(ev.offset_y()))
}

fn context_2d(canvas: &HtmlCanvasElement) -> CanvasRenderingContext2d {
    canvas.get_context("2d").expect("le contexte '2d' est toujours supporté").expect("contexte '2d' absent").dyn_into().expect("get_context(\"2d\") renvoie toujours un CanvasRenderingContext2d")
}

fn node_at(nodes: &[NodeView], x: f64, y: f64) -> Option<usize> {
    nodes.iter().position(|node| (x - node.x).abs() <= NODE_WIDTH / 2.0 && (y - node.y).abs() <= NODE_HEIGHT / 2.0)
}

fn handle_at(nodes: &[NodeView], x: f64, y: f64) -> Option<usize> {
    nodes.iter().position(|node| {
        let (hx, hy) = (node.x + HANDLE_OFFSET_X, node.y);
        ((x - hx).powi(2) + (y - hy).powi(2)).sqrt() <= HANDLE_HIT_RADIUS
    })
}

fn edge_at(nodes: &[NodeView], edges: &[EdgeView], x: f64, y: f64) -> Option<usize> {
    edges.iter().position(|edge| {
        let (Some(from), Some(to)) = (nodes.iter().find(|node| node.id == edge.from), nodes.iter().find(|node| node.id == edge.to)) else {
            return false;
        };
        distance_to_segment(x, y, from.x, from.y, to.x, to.y) <= EDGE_HIT_DISTANCE
    })
}

fn distance_to_segment(px: f64, py: f64, x1: f64, y1: f64, x2: f64, y2: f64) -> f64 {
    let (dx, dy) = (x2 - x1, y2 - y1);
    let len_sq = dx * dx + dy * dy;
    let t = if len_sq == 0.0 { 0.0 } else { ((px - x1) * dx + (py - y1) * dy) / len_sq };
    let t = t.clamp(0.0, 1.0);
    let (cx, cy) = (x1 + t * dx, y1 + t * dy);
    ((px - cx).powi(2) + (py - cy).powi(2)).sqrt()
}

/// Premier identifiant libre de la forme `node-{n}` — les nœuds créés depuis
/// le canvas n'ont pas de nom porteur de sens à proposer ; renommer reste à
/// la charge du consommateur (voir la doc de [`StateGraphEditor`]).
fn unique_node_id(existing: &[NodeView]) -> String {
    let mut n = existing.len() + 1;
    loop {
        let candidate = format!("node-{n}");
        if !existing.iter().any(|node| node.id == candidate) {
            return candidate;
        }
        n += 1;
    }
}

fn draw(
    ctx: &CanvasRenderingContext2d,
    width: f64,
    height: f64,
    nodes: &[NodeView],
    edges: &[EdgeView],
    entry: Option<&str>,
    drag: Option<&DragState>,
) {
    ctx.clear_rect(0.0, 0.0, width, height);

    for edge in edges {
        let (Some(from), Some(to)) = (nodes.iter().find(|node| node.id == edge.from), nodes.iter().find(|node| node.id == edge.to)) else {
            continue;
        };
        draw_edge(ctx, from.x, from.y, to.x, to.y, edge.guard.is_some());
    }

    if let Some(DragState::DrawingEdge { from_index, cursor }) = drag {
        if let Some(from) = nodes.get(*from_index) {
            draw_pending_edge(ctx, from.x, from.y, cursor.0, cursor.1);
        }
    }

    for node in nodes {
        draw_node(ctx, node, entry == Some(node.id.as_str()));
    }
}

fn draw_node(ctx: &CanvasRenderingContext2d, node: &NodeView, is_entry: bool) {
    let (x, y) = (node.x, node.y);

    ctx.set_fill_style_str(if is_entry { "#e0f2fe" } else { "#ffffff" });
    ctx.set_stroke_style_str(if is_entry { "#0284c7" } else { "#475569" });
    ctx.set_line_width(if is_entry { 2.5 } else { 1.5 });

    ctx.begin_path();
    let _ = ctx.round_rect_with_f64(x - NODE_WIDTH / 2.0, y - NODE_HEIGHT / 2.0, NODE_WIDTH, NODE_HEIGHT, NODE_RADIUS);
    ctx.fill();
    ctx.stroke();

    ctx.set_fill_style_str("#0f172a");
    ctx.set_text_align("center");
    ctx.set_text_baseline("middle");
    ctx.set_font("13px sans-serif");
    let _ = ctx.fill_text(&node.id, x, y - if node.action.is_some() { 8.0 } else { 0.0 });

    if let Some(action) = &node.action {
        ctx.set_font("11px sans-serif");
        ctx.set_fill_style_str("#64748b");
        let _ = ctx.fill_text(action.label(), x, y + 10.0);
    }

    // Poignée de connexion (voir la doc de [`HANDLE_RADIUS`]).
    ctx.begin_path();
    let _ = ctx.arc(x + HANDLE_OFFSET_X, y, HANDLE_RADIUS, 0.0, std::f64::consts::TAU);
    ctx.set_fill_style_str("#94a3b8");
    ctx.fill();
}

/// Arête `from -> to`, pleine si gardée (`guard.is_some()`), pointillée
/// sinon — reflet visuel de la sémantique du domaine (voir
/// `marie_core::mode::state_graph::Edge::guard` : une arête sans garde est
/// celle empruntée par défaut, si aucune arête gardée sortant du même nœud
/// n'a matché).
fn draw_edge(ctx: &CanvasRenderingContext2d, x1: f64, y1: f64, x2: f64, y2: f64, guarded: bool) {
    let (sx, sy) = edge_anchor(x1, y1, x2, y2);
    let (tx, ty) = edge_anchor(x2, y2, x1, y1);

    set_edge_stroke(ctx, guarded);
    ctx.begin_path();
    ctx.move_to(sx, sy);
    ctx.line_to(tx, ty);
    ctx.stroke();
    clear_line_dash(ctx);

    draw_arrow_head(ctx, sx, sy, tx, ty);
}

/// Arête en cours de tracé (voir [`DragState::DrawingEdge`]), de la poignée
/// du nœud source jusqu'au curseur — jamais d'ancrage sur un nœud cible
/// puisqu'il n'y en a pas encore.
fn draw_pending_edge(ctx: &CanvasRenderingContext2d, from_x: f64, from_y: f64, cursor_x: f64, cursor_y: f64) {
    ctx.set_stroke_style_str("#94a3b8");
    ctx.set_line_width(1.5);
    let _ = ctx.set_line_dash(&js_sys::Array::of2(&4.0.into(), &4.0.into()));
    ctx.begin_path();
    ctx.move_to(from_x + HANDLE_OFFSET_X, from_y);
    ctx.line_to(cursor_x, cursor_y);
    ctx.stroke();
    clear_line_dash(ctx);
}

fn set_edge_stroke(ctx: &CanvasRenderingContext2d, guarded: bool) {
    ctx.set_stroke_style_str("#475569");
    ctx.set_line_width(1.5);
    let dash = if guarded { js_sys::Array::new() } else { js_sys::Array::of2(&5.0.into(), &4.0.into()) };
    let _ = ctx.set_line_dash(&dash);
}

fn clear_line_dash(ctx: &CanvasRenderingContext2d) {
    let _ = ctx.set_line_dash(&js_sys::Array::new());
}

/// Point d'intersection entre le segment `from -> to` et le rectangle du
/// nœud `from`, pour ancrer une arête sur son bord plutôt que son centre.
fn edge_anchor(from_x: f64, from_y: f64, to_x: f64, to_y: f64) -> (f64, f64) {
    let (dx, dy) = (to_x - from_x, to_y - from_y);
    if dx == 0.0 && dy == 0.0 {
        return (from_x, from_y);
    }

    let scale_x = (NODE_WIDTH / 2.0) / dx.abs().max(f64::EPSILON);
    let scale_y = (NODE_HEIGHT / 2.0) / dy.abs().max(f64::EPSILON);
    let scale = scale_x.min(scale_y);
    (from_x + dx * scale, from_y + dy * scale)
}

fn draw_arrow_head(ctx: &CanvasRenderingContext2d, from_x: f64, from_y: f64, to_x: f64, to_y: f64) {
    let angle = (to_y - from_y).atan2(to_x - from_x);
    const SIZE: f64 = 8.0;
    const SPREAD: f64 = 0.5;

    ctx.begin_path();
    ctx.move_to(to_x, to_y);
    ctx.line_to(to_x - SIZE * (angle - SPREAD).cos(), to_y - SIZE * (angle - SPREAD).sin());
    ctx.line_to(to_x - SIZE * (angle + SPREAD).cos(), to_y - SIZE * (angle + SPREAD).sin());
    ctx.close_path();
    ctx.set_fill_style_str("#475569");
    ctx.fill();
}
