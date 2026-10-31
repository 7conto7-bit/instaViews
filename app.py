# -*- coding: utf-8 -*-
import os
import hmac
import json
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta
from functools import wraps

import requests
from flask import Flask, request, render_template_string, jsonify, Response

app = Flask(__name__)

# Fuso horário de Brasília (UTC-3)
BRT = timezone(timedelta(hours=-3))
GRAPH_API_VERSION = "v21.0"

# ==============================================================================
# ARQUIVO .JSON LOCAL (IGUAL AO PRIMEIRO PAINEL)
# ==============================================================================
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Se estiver rodando na Vercel (onde a pasta raiz é somente leitura), usa /tmp.
# Caso contrário, salva na mesma pasta do script como no primeiro painel.
if os.environ.get("VERCEL") or not os.access(BASE_DIR, os.W_OK):
    ACCOUNTS_FILE = os.path.join("/tmp", "contas_monitoradas.json")
else:
    ACCOUNTS_FILE = os.path.join(BASE_DIR, "contas_monitoradas.json")

ACCOUNTS_LOCK = threading.RLock()
SYNC_LOCK = threading.Lock()


def format_number_br(val):
    if val is None or val == "":
        return "—"
    try:
        n = int(val)
        return f"{n:,}".replace(",", ".")
    except Exception:
        return str(val)


app.jinja_env.filters["fmt_num"] = format_number_br


def load_json(filepath, default):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def save_json(filepath, data):
    """Sobrescreve o arquivo .json de forma atômica, guardando apenas o estado mais recente."""
    tmp_path = f"{filepath}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        os.replace(tmp_path, filepath)
    except Exception:
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)


def get_saved_state():
    with ACCOUNTS_LOCK:
        return load_json(ACCOUNTS_FILE, {
            "last_global_sync_ts": 0,
            "last_global_sync_str": "Ainda não sincronizado",
            "accounts": {}
        })


def save_current_state(state_data):
    with ACCOUNTS_LOCK:
        save_json(ACCOUNTS_FILE, state_data)


# ==============================================================================
# SEGURANÇA: LOGIN OBRIGATÓRIO (DASHBOARD_USER e DASHBOARD_PASS) E VARIÁVEIS
# ==============================================================================

def get_env_accounts():
    """
    Lê as contas e tokens da variável de ambiente ACCOUNTS_CONFIG_JSON.
    Assim os tokens nunca ficam expostos no código nem no .json salvo.
    """
    raw = os.environ.get("ACCOUNTS_CONFIG_JSON", "").strip()
    if not raw:
        return {}
    try:
        data = json.loads(raw)
        accounts_dict = {}
        if isinstance(data, list):
            for item in data:
                uid = str(item.get("ig_user_id", "")).strip()
                if uid and item.get("access_token"):
                    accounts_dict[uid] = item
        elif isinstance(data, dict):
            for uid, item in data.items():
                if isinstance(item, dict) and item.get("access_token"):
                    item["ig_user_id"] = str(item.get("ig_user_id") or uid)
                    accounts_dict[str(uid)] = item
        return accounts_dict
    except Exception as e:
        print(f"[ERRO] Falha ao ler ACCOUNTS_CONFIG_JSON: {e}", flush=True)
        return {}


def check_basic_auth(username, password):
    expected_user = os.environ.get("DASHBOARD_USER", "").strip()
    expected_pass = os.environ.get("DASHBOARD_PASS", "").strip()
    # Se as variáveis ainda não foram configuradas no servidor, bloqueia por segurança
    if not expected_user or not expected_pass:
        return False
    return (
        hmac.compare_digest(username or "", expected_user)
        and hmac.compare_digest(password or "", expected_pass)
    )


def requires_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        expected_user = os.environ.get("DASHBOARD_USER", "").strip()
        expected_pass = os.environ.get("DASHBOARD_PASS", "").strip()
        if not expected_user or not expected_pass:
            return Response(
                "Erro de Segurança: Configure as variáveis de ambiente DASHBOARD_USER e DASHBOARD_PASS no servidor.",
                503
            )
        auth = request.authorization
        if not auth or not check_basic_auth(auth.username, auth.password):
            return Response(
                "Acesso restrito. Digite seu usuário e senha.",
                401,
                {"WWW-Authenticate": 'Basic realm="Painel Monitor 24h"'}
            )
        return f(*args, **kwargs)
    return decorated


@app.after_request
def add_security_headers(response):
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "no-referrer"
    response.headers["Robots-Tag"] = "noindex, nofollow"
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "style-src 'self' 'unsafe-inline'; "
        "script-src 'self' 'unsafe-inline'; "
        "connect-src 'self'; "
        "img-src 'self' data:;"
    )
    return response


# ==============================================================================
# COLETOR DE MÉTRICAS (ATUALIZA APENAS OS DADOS ATUAIS NO .JSON)
# ==============================================================================

def _fetch_single_media_views(media_item, access_token):
    media_id = media_item.get("id")
    if not media_id:
        return 0

    insights_obj = media_item.get("insights", {})
    if isinstance(insights_obj, dict):
        for metric_entry in insights_obj.get("data", []):
            vals = metric_entry.get("values", [])
            if vals and isinstance(vals[0].get("value"), (int, float)):
                return int(vals[0]["value"])

    for direct_key in ("views", "play_count", "ig_play_count", "video_views"):
        if isinstance(media_item.get(direct_key), (int, float)):
            return int(media_item[direct_key])

    insights_url = f"https://graph.instagram.com/{GRAPH_API_VERSION}/{media_id}/insights"
    for metric_name in ("views", "plays", "ig_reels_aggregated_all_plays_count", "video_views", "impressions"):
        try:
            r = requests.get(
                insights_url,
                params={"metric": metric_name, "access_token": access_token},
                timeout=12
            )
            if r.status_code == 200:
                d = r.json().get("data", [])
                if d and d[0].get("values"):
                    val = d[0]["values"][0].get("value", 0)
                    if isinstance(val, (int, float)):
                        return int(val)
        except Exception:
            continue
    return 0


def _classify_instagram_account_status(profile_ok, profile_err_obj, pub_limit_res):
    if not profile_ok:
        err = profile_err_obj if isinstance(profile_err_obj, dict) else {}
        code = err.get("code")
        subcode = err.get("error_subcode")
        msg = (err.get("message") or "").lower()

        if code == 190:
            if subcode in (459, 464) or "checkpoint" in msg or "confirm your identity" in msg or "verify" in msg:
                return ("Requer Verificação (Checkpoint)", "warning", err.get("message", "Conta solicitou confirmação/checkpoint no app."))
            if subcode == 458 or "not authorized" in msg or "revoked" in msg:
                return ("Desconectada (Acesso Revogado)", "error", err.get("message", "Permissão revogada no aplicativo."))
            if subcode in (463, 467):
                return ("Sessão Expirada", "error", err.get("message", "Token de acesso expirou."))
            return ("Caiu / Sessão Inválida", "error", err.get("message", "Token inválido ou conta inacessível na Meta."))

        if code in (368, 4, 17, 32, 613) or "restricted" in msg or "temporarily blocked" in msg or "spam" in msg:
            return ("Restrita / Bloqueio Temporário", "warning", err.get("message", "Ação restrita temporariamente pelo Instagram."))

        if "disabled" in msg or "deactivated" in msg or "suspended" in msg or "ban" in msg:
            return ("Bloqueada / Desativada", "error", err.get("message", "Conta desativada ou suspensa no Instagram."))

        if code in (10, 200) or "permission" in msg:
            return ("Restrita (Sem Permissão)", "warning", err.get("message", "Permissões insuficientes na conta."))

        if err:
            return ("Caiu / Alerta na Conta", "error", err.get("message", "Erro retornado pela API do Instagram."))
        return ("Sem Resposta da Rede", "warning", "Não foi possível contatar a API do Instagram no momento.")

    if isinstance(pub_limit_res, dict):
        if "error" in pub_limit_res:
            perr = pub_limit_res.get("error") or {}
            pcode = perr.get("code")
            pmsg = (perr.get("message") or "").lower()
            if pcode == 368 or "restricted" in pmsg or "spam" in pmsg or "blocked" in pmsg:
                return ("Restrita para Publicação", "warning", perr.get("message", "Conta com restrição temporária de publicação."))
        elif "data" in pub_limit_res and pub_limit_res["data"]:
            item = pub_limit_res["data"][0] if isinstance(pub_limit_res["data"], list) else {}
            quota_usage = item.get("quota_usage")
            cfg = item.get("config") or {}
            quota_total = cfg.get("quota_total")
            if isinstance(quota_usage, int) and isinstance(quota_total, int) and quota_total > 0:
                if quota_usage >= quota_total:
                    return ("Limite Diário Atingido", "warning", f"Cota 24h atingida ({quota_usage}/{quota_total}).")
                return ("Ativa", "success", f"Conta ativa (Cota 24h: {quota_usage}/{quota_total} usados).")

    return ("Ativa", "success", "Conta ativa, verificada e normal na API oficial do Instagram.")


def fetch_account_current_metrics(acc_config, last_json_entry):
    """
    Busca os números atuais na Meta e retorna o objeto atualizado para ser salvo no .json.
    - Se a conta responder: substitui todos os números pelos atuais da consulta.
    - Se a conta tiver caído: mantém os números que já estavam no .json da última consulta válida,
      atualizando apenas o status para indicar a queda.
    """
    ig_user_id = str(acc_config.get("ig_user_id", ""))
    access_token = acc_config.get("access_token", "")
    now_str = datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S")

    current = dict(last_json_entry) if isinstance(last_json_entry, dict) else {}
    current["ig_user_id"] = ig_user_id
    current["username"] = acc_config.get("username") or current.get("username") or f"user_{ig_user_id}"
    current["label"] = acc_config.get("label") or current.get("label") or "Conta"
    current["connected_at"] = acc_config.get("connected_at") or current.get("connected_at") or "—"
    current["token_type_label"] = acc_config.get("token_type_label") or current.get("token_type_label") or "Longa Duração (60 dias)"

    profile_ok = False
    last_profile_error = {}
    followers_count = None
    media_count = None

    for endpoint in (
        f"https://graph.instagram.com/{GRAPH_API_VERSION}/{ig_user_id}",
        f"https://graph.instagram.com/{GRAPH_API_VERSION}/me"
    ):
        try:
            res_profile = requests.get(
                endpoint,
                params={
                    "fields": "id,user_id,username,name,account_type,followers_count,media_count",
                    "access_token": access_token
                },
                timeout=15
            )
            p_data = res_profile.json()
            if res_profile.status_code == 200 and "error" not in p_data:
                profile_ok = True
                if p_data.get("username"):
                    current["username"] = p_data["username"]
                if "followers_count" in p_data:
                    followers_count = int(p_data["followers_count"])
                if "media_count" in p_data:
                    media_count = int(p_data["media_count"])
                if followers_count is not None:
                    break
            else:
                if isinstance(p_data, dict) and "error" in p_data:
                    last_profile_error = p_data["error"]
        except Exception as e:
            last_profile_error = {"message": str(e)}

    pub_limit_data = None
    if profile_ok:
        try:
            res_limit = requests.get(
                f"https://graph.instagram.com/{GRAPH_API_VERSION}/{ig_user_id}/content_publishing_limit",
                params={"fields": "config,quota_usage", "access_token": access_token},
                timeout=12
            )
            pub_limit_data = res_limit.json()
        except Exception:
            pub_limit_data = None

    status_label, status_badge, status_detail = _classify_instagram_account_status(
        profile_ok, last_profile_error, pub_limit_data
    )
    current["account_status"] = status_label
    current["account_status_badge"] = status_badge
    current["account_status_detail"] = status_detail

    # Se a conta caiu, NÃO zera os números do .json! Mantém os últimos números válidos.
    if not profile_ok:
        current.setdefault("followers_count", 0)
        current.setdefault("total_views", 0)
        current.setdefault("media_count", 0)
        current.setdefault("posts_list", [])
        current.setdefault("metrics_updated_at", "Sem coleta antes da queda")
        current.pop("access_token", None)
        return current

    # Conta ativa: puxa a lista atual de mídias e calcula visualizações
    all_media = []
    media_url = f"https://graph.instagram.com/{GRAPH_API_VERSION}/{ig_user_id}/media"
    inline_ok = False

    try:
        next_url = media_url
        params = {
            "fields": "id,caption,media_type,media_product_type,permalink,shortcode,timestamp,like_count,comments_count,insights.metric(views)",
            "limit": 50,
            "access_token": access_token
        }
        pages = 0
        while next_url and pages < 20:
            pages += 1
            res_m = requests.get(next_url, params=params, timeout=20)
            data_m = res_m.json()
            if "data" in data_m:
                inline_ok = True
                all_media.extend(data_m["data"])
                next_url = data_m.get("paging", {}).get("next")
                params = None
            else:
                break
    except Exception:
        pass

    if not inline_ok:
        all_media = []
        try:
            next_url = media_url
            params = {
                "fields": "id,caption,media_type,media_product_type,permalink,shortcode,timestamp,like_count,comments_count",
                "limit": 50,
                "access_token": access_token
            }
            pages = 0
            while next_url and pages < 20:
                pages += 1
                res_m = requests.get(next_url, params=params, timeout=20)
                data_m = res_m.json()
                if "data" in data_m:
                    all_media.extend(data_m["data"])
                    next_url = data_m.get("paging", {}).get("next")
                    params = None
                else:
                    break
        except Exception:
            pass

    total_views = 0
    media_views_map = {}
    items_needing_individual_fetch = []

    for m_item in all_media:
        mid = m_item.get("id")
        insights_obj = m_item.get("insights", {})
        found_inline = False
        if isinstance(insights_obj, dict) and "data" in insights_obj:
            for metric_entry in insights_obj.get("data", []):
                vals = metric_entry.get("values", [])
                if vals and isinstance(vals[0].get("value"), (int, float)):
                    v_count = int(vals[0]["value"])
                    total_views += v_count
                    if mid:
                        media_views_map[mid] = v_count
                    found_inline = True
                    break
        if not found_inline:
            items_needing_individual_fetch.append(m_item)

    if items_needing_individual_fetch:
        with ThreadPoolExecutor(max_workers=6) as pool:
            future_by_id = {
                item.get("id"): pool.submit(_fetch_single_media_views, item, access_token)
                for item in items_needing_individual_fetch if item.get("id")
            }
            for mid, fut in future_by_id.items():
                try:
                    v_count = int(fut.result() or 0)
                    media_views_map[mid] = v_count
                    total_views += v_count
                except Exception:
                    media_views_map[mid] = 0

    posts_ranked = []
    for m_item in all_media:
        mid = m_item.get("id", "")
        v_count = int(media_views_map.get(mid, 0))
        shortcode = m_item.get("shortcode", "")
        permalink = (m_item.get("permalink") or "").strip()
        if not permalink and shortcode:
            permalink = f"https://www.instagram.com/reel/{shortcode}/"

        reels_link = permalink
        if shortcode:
            reels_link = f"https://www.instagram.com/reel/{shortcode}/"
        elif "/p/" in permalink:
            reels_link = permalink.replace("/p/", "/reel/")

        raw_caption = (m_item.get("caption") or "").strip().replace("\n", " ")
        short_caption = (raw_caption[:65] + "...") if len(raw_caption) > 65 else (raw_caption or "Sem legenda")
        ts_raw = m_item.get("timestamp", "")
        ts_fmt = ts_raw[:16].replace("T", " ") if ts_raw else ""

        posts_ranked.append({
            "id": mid,
            "views": v_count,
            "permalink": permalink or reels_link,
            "reels_url": reels_link or permalink,
            "shortcode": shortcode,
            "caption": short_caption,
            "media_type": m_item.get("media_product_type") or m_item.get("media_type") or "REELS",
            "timestamp": ts_fmt
        })

    posts_ranked.sort(key=lambda x: x.get("views", 0), reverse=True)

    # Sobrescreve com os dados atuais da consulta
    current["posts_list"] = posts_ranked
    current["followers_count"] = followers_count if followers_count is not None else 0
    current["total_views"] = total_views
    current["media_count"] = media_count if media_count is not None else len(all_media)
    current["metrics_updated_at"] = now_str

    # Nunca salva o token dentro do .json de visualização
    current.pop("access_token", None)
    return current


def refresh_all_accounts_to_json():
    """
    Atualiza todas as contas e sobrescreve o arquivo .json local com os números atuais.
    """
    with SYNC_LOCK:
        env_accounts = get_env_accounts()
        saved_state = get_saved_state()
        saved_accounts = saved_state.get("accounts", {}) if isinstance(saved_state, dict) else {}

        if not env_accounts:
            return saved_state

        acc_ids = list(env_accounts.keys())
        new_accounts_map = {}

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {
                acc_id: pool.submit(
                    fetch_account_current_metrics,
                    env_accounts[acc_id],
                    saved_accounts.get(acc_id, {})
                )
                for acc_id in acc_ids
            }
            for acc_id, fut in futures.items():
                try:
                    new_accounts_map[acc_id] = fut.result()
                except Exception as e:
                    print(f"[AVISO MÉTRICAS] Falha ao atualizar conta {acc_id}: {e}", flush=True)
                    if acc_id in saved_accounts:
                        new_accounts_map[acc_id] = saved_accounts[acc_id]

        now_ts = time.time()
        now_str = datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S")

        updated_state = {
            "last_global_sync_ts": now_ts,
            "last_global_sync_str": now_str,
            "accounts": new_accounts_map
        }
        save_current_state(updated_state)
        return updated_state


# ==============================================================================
# THREAD LOCAL DE 5 EM 5 MINUTOS (CASO RODE EM SERVIDOR LIGADO 24H)
# + SUPORTE A CRON EXTERNO (CASO RODE NA VERCEL)
# ==============================================================================

def background_5min_loop():
    """Roda a cada 5 minutos (300s) automaticamente enquanto o processo estiver ativo."""
    while True:
        try:
            state = get_saved_state()
            last_ts = state.get("last_global_sync_ts", 0)
            if time.time() - last_ts >= 295 or not state.get("accounts"):
                refresh_all_accounts_to_json()
        except Exception as e:
            print(f"[ERRO LOOP 5 MIN]: {e}", flush=True)
        time.sleep(30)


bg_thread = threading.Thread(target=background_5min_loop, daemon=True)
bg_thread.start()


# ==============================================================================
# TEMPLATE HTML SOMENTE LEITURA (TABELA 3. CONTAS CONECTADAS)
# ==============================================================================

HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Monitor 24h — Contas Conectadas</title>
    <style>
        :root {
            --bg: #0f172a;
            --card: #1e293b;
            --text: #f8fafc;
            --muted: #94a3b8;
            --border: #334155;
            --success: #22c55e;
            --warning: #f59e0b;
            --info: #38bdf8;
            --error: #ef4444;
        }
        * { box-sizing: border-box; font-family: 'Segoe UI', system-ui, sans-serif; }
        body { background: var(--bg); color: var(--text); margin: 0; padding: 24px; }
        .container { max-width: 1180px; margin: 0 auto; display: grid; gap: 20px; }
        .card { background: var(--card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; }
        h1, h2 { margin: 0; }
        .hint { font-size: 0.82rem; color: var(--muted); margin-top: 6px; line-height: 1.4; }
        table { width: 100%; border-collapse: collapse; margin-top: 12px; font-size: 0.88rem; }
        th, td { border-bottom: 1px solid var(--border); padding: 10px; text-align: left; vertical-align: middle; }
        th { color: var(--muted); }
        .badge { padding: 4px 8px; border-radius: 6px; font-size: 0.78rem; font-weight: 600; display: inline-block; }
        .badge.success { background: rgba(34, 197, 94, 0.2); color: #4ade80; }
        .badge.warning { background: rgba(245, 158, 11, 0.2); color: #fbbf24; }
        .badge.info { background: rgba(56, 189, 248, 0.2); color: #38bdf8; }
        .badge.error { background: rgba(239, 68, 68, 0.2); color: #f87171; }
        .metric-val { font-weight: 700; color: #f8fafc; font-size: 0.95rem; }
        .posts-toggle-btn {
            margin-top: 0; padding: 3px 7px; width: auto; font-size: 0.78rem; border-radius: 5px;
            background: #334155; color: #38bdf8; border: 1px solid #475569; cursor: pointer;
            line-height: 1; display: inline-flex; align-items: center; justify-content: center; transition: 0.15s;
        }
        .posts-toggle-btn:hover { background: #475569; color: #ffffff; border-color: #38bdf8; }
        .posts-dropdown-row { background: #0b1120; }
        .posts-dropdown-box {
            padding: 12px 14px; border-radius: 8px; background: #111a2e;
            border: 1px solid var(--border); max-height: 280px; overflow-y: auto;
        }
        .post-rank-item {
            display: flex; justify-content: space-between; align-items: center;
            gap: 12px; padding: 8px 10px; border-bottom: 1px solid #1e293b; font-size: 0.84rem;
        }
        .post-rank-item:last-child { border-bottom: none; }
        .post-rank-link { color: #38bdf8; text-decoration: none; font-weight: 600; word-break: break-all; }
        .post-rank-link:hover { text-decoration: underline; color: #7dd3fc; }
    </style>
</head>
<body>
<div class="container">
    <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px;">
        <div>
            <h1>Painel de Monitoramento 24h (Somente Visualização)</h1>
            <div class="hint">
                🔒 <b>Banco Local (.json) • Atualização automática a cada 5 minutos:</b> Exibe apenas os dados mais recentes de cada conta. Se uma conta cair, os últimos números permanecem salvos no <code>.json</code>.
            </div>
        </div>
        <div style="display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
            <div class="badge info" style="font-size:0.86rem; padding:8px 12px;">
                🔄 Última Atualização (5 min): <b>{{ last_sync }}</b>
            </div>
            <div class="badge success" style="font-size:0.82rem; padding:8px 12px;">
                ⏱️ Recarregando tela em: <span id="reloadCountdown">60s</span>
            </div>
        </div>
    </div>

    <!-- 3. CONTAS CONECTADAS (SOMENTE LEITURA) -->
    <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:10px;">
            <h2>3. Contas Conectadas ({{ accounts|length }})</h2>
            <div style="display:flex; gap:14px; font-size:0.88rem; color:var(--muted);">
                <span>Seguidores Totais: <b style="color:#4ade80;">{{ total_followers | fmt_num }}</b></span>
                <span>Visualizações Totais: <b style="color:#38bdf8;">{{ total_views | fmt_num }}</b></span>
            </div>
        </div>

        {% if accounts %}
        <div style="overflow-x:auto;">
            <table>
                <thead>
                    <tr>
                        <th style="width:55px; text-align:center;">Posts</th>
                        <th>@Username</th>
                        <th>Status da Conta (Instagram)</th>
                        <th>Seguidores</th>
                        <th>Visualizações Totais (Posts)</th>
                        <th>IG User ID</th>
                        <th>Etiqueta</th>
                        <th>Última Coleta Válida</th>
                        <th>Status do Token</th>
                    </tr>
                </thead>
                <tbody>
                    {% for acc_id, acc in accounts.items() %}
                    {% set posts_ordenados = acc.get('posts_list', []) %}
                    <tr>
                        <td style="text-align:center; white-space:nowrap;">
                            <button type="button" class="posts-toggle-btn" onclick="togglePostsConta('{{ acc_id }}', this)" title="Abrir/fechar lista de posts ordenados por visualizações (maior no topo)">
                                ↓
                            </button>
                        </td>
                        <td><b>@{{ acc.username }}</b></td>
                        <td>
                            <span class="badge {{ acc.get('account_status_badge', 'success') }}" title="{{ acc.get('account_status_detail', '') }}">
                                {{ acc.get('account_status', 'Ativa') }}
                            </span>
                        </td>
                        <td>
                            <span class="metric-val">{{ acc.get('followers_count') | fmt_num }}</span>
                        </td>
                        <td>
                            <span class="metric-val" style="color:#38bdf8;">{{ acc.get('total_views') | fmt_num }}</span>
                            {% if acc.get('media_count') is not none %}
                            <span style="color:var(--muted); font-size:0.78rem; display:block;">({{ acc.get('media_count') }} post(s))</span>
                            {% endif %}
                        </td>
                        <td>{{ acc.ig_user_id }}</td>
                        <td>{{ acc.label }}</td>
                        <td>{{ acc.get('metrics_updated_at', '—') }}</td>
                        <td><span class="badge success">{{ acc.get('token_type_label', 'Longa Duração (60 dias)') }}</span></td>
                    </tr>
                    <tr id="posts_row_{{ acc_id }}" class="posts-dropdown-row" style="display:none;">
                        <td colspan="9" style="padding:10px 16px;">
                            <div class="posts-dropdown-box">
                                <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px; flex-wrap:wrap; gap:8px;">
                                    <b style="color:#38bdf8; font-size:0.88rem;">
                                        📊 Posts / Reels de @{{ acc.username }} — Ordenados por Visualizações (Maior no topo ↓ Menor embaixo)
                                    </b>
                                    <span style="color:var(--muted); font-size:0.78rem;">
                                        Total listado: {{ posts_ordenados|length }} post(s)
                                    </span>
                                </div>
                                {% if posts_ordenados %}
                                    {% for p in posts_ordenados %}
                                    <div class="post-rank-item">
                                        <div style="display:flex; align-items:center; gap:10px; flex-wrap:wrap;">
                                            <span class="badge info" style="min-width:34px; text-align:center;">#{{ loop.index }}</span>
                                            <span style="font-weight:700; color:#4ade80; min-width:115px;">
                                                👁️ {{ p.get('views', 0) | fmt_num }} views
                                            </span>
                                            <a href="{{ p.get('reels_url') or p.get('permalink') or '#' }}" target="_blank" rel="noopener noreferrer" class="post-rank-link">
                                                🔗 {{ p.get('reels_url') or p.get('permalink') or ('ID: ' ~ p.get('id')) }}
                                            </a>
                                        </div>
                                        <div style="color:var(--muted); font-size:0.78rem; text-align:right;">
                                            {{ p.get('caption', '') }}
                                            {% if p.get('timestamp') %}
                                            <span style="margin-left:6px; color:#64748b;">({{ p.get('timestamp') }})</span>
                                            {% endif %}
                                        </div>
                                    </div>
                                    {% endfor %}
                                {% else %}
                                    <div class="hint" style="margin:0; color:#fbbf24;">
                                        Nenhum post carregado para esta conta.
                                    </div>
                                {% endif %}
                            </div>
                        </td>
                    </tr>
                    {% endfor %}
                </tbody>
            </table>
        </div>
        {% else %}
        <p class="hint" style="margin-top:16px;">Nenhuma conta configurada em <code>ACCOUNTS_CONFIG_JSON</code> ainda.</p>
        {% endif %}
    </div>
</div>

<script>
    function togglePostsConta(accId, btnEl) {
        const row = document.getElementById('posts_row_' + accId);
        if (!row) return;
        const isHidden = (row.style.display === 'none' || row.style.display === '');
        row.style.display = isHidden ? 'table-row' : 'none';
        if (btnEl) {
            btnEl.innerText = isHidden ? '↑' : '↓';
        }
        try {
            const openState = JSON.parse(sessionStorage.getItem('open_posts_rows') || '{}');
            openState[accId] = isHidden;
            sessionStorage.setItem('open_posts_rows', JSON.stringify(openState));
        } catch (e) {}
    }

    window.addEventListener('DOMContentLoaded', () => {
        try {
            const openState = JSON.parse(sessionStorage.getItem('open_posts_rows') || '{}');
            Object.keys(openState).forEach(accId => {
                if (openState[accId]) {
                    const row = document.getElementById('posts_row_' + accId);
                    if (row) row.style.display = 'table-row';
                }
            });
        } catch (e) {}
    });

    let remaining = 60;
    setInterval(() => {
        remaining--;
        const el = document.getElementById('reloadCountdown');
        if (el) el.innerText = remaining + 's';
        if (remaining <= 0) {
            window.location.reload();
        }
    }, 1000);
</script>
</body>
</html>
"""


# ==============================================================================
# ROTAS FLASK (SOMENTE LEITURA + GATILHO DE 5 MINUTOS)
# ==============================================================================

@app.route("/", methods=["GET"])
@requires_auth
def index():
    state = get_saved_state()
    last_ts = state.get("last_global_sync_ts", 0)

    # Se já passaram 5 minutos (300s) desde a última gravação no .json, atualiza agora
    if time.time() - last_ts >= 300 or not state.get("accounts"):
        state = refresh_all_accounts_to_json()

    accounts = state.get("accounts", {})
    last_sync_str = state.get("last_global_sync_str", "—")

    total_followers = sum(int(acc.get("followers_count") or 0) for acc in accounts.values())
    total_views = sum(int(acc.get("total_views") or 0) for acc in accounts.values())

    return render_template_string(
        HTML_TEMPLATE,
        accounts=accounts,
        last_sync=last_sync_str,
        total_followers=total_followers,
        total_views=total_views
    )


@app.route("/api/cron/sync", methods=["GET", "POST"])
def cron_sync():
    """
    Endpoint protegido por senha (CRON_SECRET) para ser chamado a cada 5 minutos
    pelo cron-job.org na Vercel, mantendo o container acordado e o .json sempre atualizado.
    """
    cron_secret = os.environ.get("CRON_SECRET", "").strip()
    if not cron_secret:
        return jsonify({"ok": False, "error": "CRON_SECRET não definido."}), 403

    provided = request.args.get("key", "").strip()
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        provided = auth_header.replace("Bearer ", "", 1).strip()

    if not hmac.compare_digest(provided, cron_secret):
        return jsonify({"ok": False, "error": "Chave inválida."}), 401

    state = refresh_all_accounts_to_json()
    return jsonify({
        "ok": True,
        "updated_at": state.get("last_global_sync_str"),
        "accounts_count": len(state.get("accounts", {}))
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)