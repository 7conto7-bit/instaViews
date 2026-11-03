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
from flask import Flask, request, render_template_string, jsonify, Response, redirect, url_for

app = Flask(__name__)

# Fuso horário de Brasília (UTC-3)
BRT = timezone(timedelta(hours=-3))
GRAPH_API_VERSION = "v21.0"

# ==============================================================================
# CONFIGURAÇÃO UPSTASH REDIS (PERSISTÊNCIA DEFINITIVA NA VERCEL)
# ==============================================================================
UPSTASH_URL = os.environ.get("UPSTASH_REDIS_REST_URL", "").strip().rstrip("/")
UPSTASH_TOKEN = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "").strip()
REDIS_STATE_KEY = "ig_monitor:state_v1"

# Intervalo mínimo entre leituras diretas no Redis (em segundos) para poupar comandos
REDIS_READ_TTL = 300  # 5 minutos

# ==============================================================================
# ARQUIVOS .JSON (FALLBACK LOCAL + /TMP NA VERCEL)
# ==============================================================================
BASE_DIR = os.path.abspath(os.path.dirname(__file__))
BUNDLED_MONITOR_FILE = os.path.join(BASE_DIR, "contas_monitoradas.json")
CODIGO1_ACCOUNTS_FILE = os.path.join(BASE_DIR, "contas_conectadas.json")

if os.environ.get("VERCEL") or not os.access(BASE_DIR, os.W_OK):
    RUNTIME_ACCOUNTS_FILE = os.path.join("/tmp", "contas_monitoradas.json")
else:
    RUNTIME_ACCOUNTS_FILE = BUNDLED_MONITOR_FILE

ACCOUNTS_LOCK = threading.RLock()
SYNC_LOCK = threading.Lock()

# Cache em memória do processo
MEMORY_CACHE_ACCOUNTS = {}
MEMORY_LAST_SYNC_TS = 0.0
MEMORY_LAST_SYNC_STR = "Ainda não sincronizado"
LAST_REDIS_FETCH_TS = 0.0

# Flag que garante que o estado existente no Upstash foi lido com sucesso nesta instância
REDIS_BOOTSTRAPPED = False


def format_number_br(val):
    if val is None or val == "":
        return "—"
    try:
        n = int(round(float(val)))
        return f"{n:,}".replace(",", ".")
    except Exception:
        return str(val)


def format_rate_br(val):
    """Formata taxas de visualizações por minuto/hora no padrão brasileiro."""
    if val is None or val == "":
        return "0"
    try:
        num = float(val)
        if num <= 0:
            return "0"
        if num >= 100:
            n = int(round(num))
            return f"{n:,}".replace(",", ".")
        if num >= 10:
            s = f"{num:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
            return s[:-2] if s.endswith(",0") else s
        s = f"{num:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        if s.endswith(",00"):
            return s[:-3]
        if s.endswith("0") and "," in s:
            return s[:-1]
        return s
    except Exception:
        return str(val)


app.jinja_env.filters["fmt_num"] = format_number_br
app.jinja_env.filters["fmt_rate"] = format_rate_br


# ==============================================================================
# CÁLCULO DE VISUALIZAÇÕES POR MINUTO E POR HORA (POR CONTA E GLOBAL)
# ==============================================================================

def _safe_int(val):
    try:
        if val is None or val == "":
            return 0
        if isinstance(val, str):
            val = val.replace(".", "").replace(",", "").strip()
        return max(0, int(float(val)))
    except Exception:
        return 0


def _safe_float(val):
    try:
        if val is None or val == "":
            return 0.0
        if isinstance(val, str):
            val = val.replace(",", ".").strip()
        return max(0.0, float(val))
    except Exception:
        return 0.0


def _parse_br_datetime_to_ts(dt_str):
    if not dt_str or not isinstance(dt_str, str):
        return None
    s = dt_str.strip()
    for fmt in ("%d/%m/%Y %H:%M:%S", "%d/%m/%Y %H:%M"):
        try:
            dt = datetime.strptime(s, fmt).replace(tzinfo=BRT)
            return dt.timestamp()
        except Exception:
            continue
    return None


def _parse_post_timestamp_to_ts(ts_str):
    if not ts_str or not isinstance(ts_str, str):
        return None
    s = ts_str.strip()
    try:
        if "+" in s or s.endswith("Z"):
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.timestamp()
        # No media fetcher, o timestamp salvo em posts_list vem em UTC ("YYYY-MM-DD HH:MM")
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S"):
            try:
                dt = datetime.strptime(s[:19], fmt).replace(tzinfo=timezone.utc)
                return dt.timestamp()
            except Exception:
                continue
    except Exception:
        pass
    return None


def _merge_views_history(old_hist, new_hist, current_views=0, current_ts=None):
    """
    Mescla o histórico de coletas (timestamp + total_views) preservando até 288 pontos (24h).
    Garante que o total de views na linha do tempo nunca regrida para zero.
    """
    by_bucket = {}
    for src in (old_hist, new_hist):
        if not isinstance(src, list):
            continue
        for item in src:
            if not isinstance(item, dict):
                continue
            ts = _safe_float(item.get("ts"))
            v = _safe_int(item.get("views"))
            if ts > 0 and v >= 0:
                bucket = int(ts // 30)  # agrupa leituras dentro da mesma janela de 30s
                if bucket not in by_bucket or v > by_bucket[bucket]["views"]:
                    by_bucket[bucket] = {"ts": round(ts, 2), "views": v}

    if current_ts and current_ts > 0 and current_views > 0:
        bucket = int(current_ts // 30)
        if bucket not in by_bucket or current_views > by_bucket[bucket]["views"]:
            by_bucket[bucket] = {"ts": round(float(current_ts), 2), "views": int(current_views)}

    ordered = sorted(by_bucket.values(), key=lambda x: x["ts"])

    # Garante monotonicidade (nunca deixa um ponto posterior ter menos views que o anterior)
    running_max = 0
    cleaned = []
    for pt in ordered:
        running_max = max(running_max, _safe_int(pt.get("views")))
        cleaned.append({"ts": pt["ts"], "views": running_max})

    # Mantém últimas 24h (ou no máximo 288 pontos de 5 min)
    if len(cleaned) > 288:
        cleaned = cleaned[-288:]
    return cleaned


def _compute_account_velocity_rates(acc_dict):
    """
    Calcula `views_per_min` e `views_per_hour` da conta:
    1. Cálculo primário (Lifetime ativo dos posts / conta):
       Baseia-se no tempo ativo desde o primeiro post registrado (ou conexão da conta)
       até a última coleta válida (ou instante atual), garantindo valores reais imediatos.
    2. Cálculo por janela de histórico (Delta entre coletas no Redis):
       Se houver ganho real de visualizações registrado entre snapshots recentes no `views_history`,
       utiliza a maior taxa entre o ganho recente e a média de vida dos posts.
    """
    if not isinstance(acc_dict, dict):
        return 0.0, 0.0

    total_views = _safe_int(acc_dict.get("total_views"))
    if total_views <= 0:
        return 0.0, 0.0

    ref_end_ts = (
        _parse_br_datetime_to_ts(acc_dict.get("metrics_updated_at"))
        or time.time()
    )

    # 1. Descobre o timestamp do post mais antigo (ou data de conexão da conta)
    post_timestamps = []
    posts_list = acc_dict.get("posts_list")
    if isinstance(posts_list, list):
        for p in posts_list:
            if isinstance(p, dict):
                pts = _parse_post_timestamp_to_ts(p.get("timestamp"))
                if pts and pts > 0:
                    post_timestamps.append(pts)

    earliest_ts = min(post_timestamps) if post_timestamps else None
    if not earliest_ts:
        conn_ts = _parse_br_datetime_to_ts(acc_dict.get("connected_at"))
        if conn_ts:
            earliest_ts = conn_ts

    lifetime_vpm = 0.0
    lifetime_vph = 0.0
    if earliest_ts and ref_end_ts > earliest_ts:
        elapsed_minutes = max(1.0, (ref_end_ts - earliest_ts) / 60.0)
        elapsed_hours = max(1.0 / 60.0, elapsed_minutes / 60.0)
        lifetime_vpm = total_views / elapsed_minutes
        lifetime_vph = total_views / elapsed_hours

    # 2. Verifica velocidade recente pelo histórico de snapshots se houver delta positivo
    hist = acc_dict.get("views_history")
    delta_vpm = 0.0
    delta_vph = 0.0
    if isinstance(hist, list) and len(hist) >= 2:
        latest = hist[-1]
        latest_ts = _safe_float(latest.get("ts"))
        latest_views = _safe_int(latest.get("views"))

        # Ponto de referência para taxa por minuto (pelo menos 2 min atrás)
        ref_min_pt = hist[0]
        for pt in reversed(hist[:-1]):
            dt_sec = latest_ts - _safe_float(pt.get("ts"))
            if dt_sec >= 120:
                ref_min_pt = pt
                break

        dt_min = (latest_ts - _safe_float(ref_min_pt.get("ts"))) / 60.0
        dv_min = latest_views - _safe_int(ref_min_pt.get("views"))
        if dt_min >= 1.0 and dv_min > 0:
            delta_vpm = dv_min / dt_min

        # Ponto de referência para taxa por hora (busca ponto próximo de 60 min atrás)
        ref_hour_pt = hist[0]
        for pt in reversed(hist[:-1]):
            dt_sec = latest_ts - _safe_float(pt.get("ts"))
            if dt_sec >= 3300:  # ~55 min
                ref_hour_pt = pt
                break

        dt_hour = (latest_ts - _safe_float(ref_hour_pt.get("ts"))) / 3600.0
        dv_hour = latest_views - _safe_int(ref_hour_pt.get("views"))
        if dt_hour >= (1.0 / 60.0) and dv_hour > 0:
            delta_vph = dv_hour / dt_hour

    # Prioriza a velocidade do delta recente quando houver crescimento entre coletas;
    # caso contrário, usa a taxa consolidada dos posts da conta (para nunca zerar paradas entre coletas)
    final_vpm = delta_vpm if delta_vpm > 0 else lifetime_vpm
    final_vph = delta_vph if delta_vph > 0 else (final_vpm * 60.0 if final_vpm > 0 else lifetime_vph)

    # Se já havia um cálculo salvo válido e a conta caiu sem posts_list nem histórico, preserva o salvo
    if final_vpm <= 0 and _safe_float(acc_dict.get("views_per_min")) > 0:
        final_vpm = _safe_float(acc_dict.get("views_per_min"))
    if final_vph <= 0 and _safe_float(acc_dict.get("views_per_hour")) > 0:
        final_vph = _safe_float(acc_dict.get("views_per_hour"))

    return round(final_vpm, 2), round(final_vph, 2)


# ==============================================================================
# FUNÇÕES DE PERSISTÊNCIA NO UPSTASH REDIS (COM RETRY E PROTEÇÃO ANTI-SOBRESCRITA)
# ==============================================================================

def is_upstash_configured():
    return bool(UPSTASH_URL and UPSTASH_TOKEN)


def upstash_command(*args, retries=3):
    """
    Executa um único comando Redis via API REST do Upstash com retry automático.
    Retorna uma tupla: (sucesso_http: bool, resultado: any)
    """
    if not is_upstash_configured():
        return False, None

    for attempt in range(1, retries + 1):
        try:
            r = requests.post(
                UPSTASH_URL,
                headers={
                    "Authorization": f"Bearer {UPSTASH_TOKEN}",
                    "Content-Type": "application/json"
                },
                json=list(args),
                timeout=12
            )
            if r.status_code == 200:
                data = r.json()
                if "error" not in data:
                    return True, data.get("result")
                print(f"[ERRO UPSTASH PAYLOAD] {data.get('error')}", flush=True)
            else:
                print(f"[AVISO UPSTASH] Tentativa {attempt}/{retries} - HTTP {r.status_code}: {r.text}", flush=True)
        except Exception as e:
            print(f"[ERRO UPSTASH] Tentativa {attempt}/{retries} falhou: {e}", flush=True)

        if attempt < retries:
            time.sleep(0.8 * attempt)

    return False, None


def load_state_from_upstash():
    """
    Carrega o estado global salvo no Upstash Redis usando 1 comando (GET).
    Retorna: (leitura_concluida_com_sucesso: bool, dados_do_estado: dict)
    """
    if not is_upstash_configured():
        return False, {}

    ok, raw_state = upstash_command("GET", REDIS_STATE_KEY)
    if not ok:
        # Falha de rede ou credencial: retorna False para NÃO marcar REDIS_BOOTSTRAPPED
        # e impedir que a instância sobrescreva o Redis com dados zerados!
        return False, {}

    if raw_state is None:
        # Chave ainda não existe no banco (primeiro uso real), mas a comunicação funcionou
        return True, {}

    if isinstance(raw_state, str):
        try:
            parsed = json.loads(raw_state)
            if isinstance(parsed, dict):
                return True, parsed
        except Exception as e:
            print(f"[ERRO UPSTASH PARSE STATE]: {e}", flush=True)
            return False, {}
    elif isinstance(raw_state, dict):
        return True, raw_state

    return False, {}


def save_state_to_upstash(state_data):
    """
    Salva o estado completo no Upstash Redis usando 1 comando (SET).
    PROTEÇÃO CRÍTICA: Nunca sobrescreve o Upstash se a instância atual ainda não
    tiver conseguido ler o estado anterior do Upstash (evita zerar contas no cold start).
    """
    global LAST_REDIS_FETCH_TS, REDIS_BOOTSTRAPPED, MEMORY_LAST_SYNC_TS, MEMORY_LAST_SYNC_STR
    if not is_upstash_configured() or not isinstance(state_data, dict):
        return False

    try:
        # Se por algum motivo esta instância ainda não leu o Redis com sucesso,
        # obriga a ler agora antes de salvar para NUNCA perder dados antigos de contas caídas.
        if not REDIS_BOOTSTRAPPED:
            ok_read, remote_state = load_state_from_upstash()
            if not ok_read:
                print(
                    "[PROTEÇÃO ANTI-ZERAMENTO] Abortando escrita no Upstash pois a leitura prévia falhou. "
                    "Isso protege os dados históricos no Redis contra sobrescrita a frio.",
                    flush=True
                )
                return False

            # Mescla imediatamente os dados remotos na memória antes de prosseguir
            remote_accs = remote_state.get("accounts", {}) if isinstance(remote_state, dict) else {}
            if isinstance(remote_accs, dict):
                for r_uid, r_acc in remote_accs.items():
                    if isinstance(r_acc, dict):
                        uid_s = str(r_uid)
                        MEMORY_CACHE_ACCOUNTS[uid_s] = merge_best_account_data(
                            r_acc, MEMORY_CACHE_ACCOUNTS.get(uid_s, {})
                        )
            if float(remote_state.get("last_global_sync_ts") or 0) > MEMORY_LAST_SYNC_TS:
                MEMORY_LAST_SYNC_TS = float(remote_state["last_global_sync_ts"])
                MEMORY_LAST_SYNC_STR = remote_state.get("last_global_sync_str", MEMORY_LAST_SYNC_STR)

            REDIS_BOOTSTRAPPED = True

        new_accs = state_data.get("accounts", {}) if isinstance(state_data.get("accounts"), dict) else {}
        cleaned_accs = {}

        # Garante que todas as contas da memória + novas contas sejam preservadas pelo maior valor
        all_uids = set(MEMORY_CACHE_ACCOUNTS.keys()) | set(str(k) for k in new_accs.keys())
        for uid_str in all_uids:
            mem_acc = MEMORY_CACHE_ACCOUNTS.get(uid_str, {})
            inc_acc = new_accs.get(uid_str, {})
            clean_merged = merge_best_account_data(mem_acc, inc_acc)
            clean_merged.pop("access_token", None)
            cleaned_accs[uid_str] = clean_merged
            MEMORY_CACHE_ACCOUNTS[uid_str] = clean_merged

        sorted_accs = sort_accounts_by_views_desc(cleaned_accs)
        state_to_save = {
            "last_global_sync_ts": max(
                float(state_data.get("last_global_sync_ts") or 0),
                float(MEMORY_LAST_SYNC_TS or 0)
            ),
            "last_global_sync_str": (
                state_data.get("last_global_sync_str")
                if (state_data.get("last_global_sync_str") and state_data.get("last_global_sync_str") != "Ainda não sincronizado")
                else (MEMORY_LAST_SYNC_STR or "—")
            ),
            "accounts": sorted_accs
        }

        ok_set, _ = upstash_command("SET", REDIS_STATE_KEY, json.dumps(state_to_save, ensure_ascii=False))
        if ok_set:
            state_data["accounts"] = sorted_accs
            LAST_REDIS_FETCH_TS = time.time()
            REDIS_BOOTSTRAPPED = True
            return True
        return False
    except Exception as e:
        print(f"[ERRO AO SALVAR NO UPSTASH]: {e}", flush=True)
        return False


# ==============================================================================
# FUNÇÕES DE ARQUIVO E MERGE DE DADOS (BLINDAGEM ANTI-ZERAMENTO)
# ==============================================================================

def load_json(filepath, default):
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return default
    return default


def save_json(filepath, data):
    """Sobrescreve o arquivo .json de forma atômica."""
    tmp_path = f"{filepath}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        os.replace(tmp_path, filepath)
    except Exception:
        try:
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4, ensure_ascii=False)
        except Exception:
            pass


def sort_accounts_by_views_desc(accounts_dict):
    """
    Ordena as contas colocando as com MAIS visualizações totais no topo
    e as com MENOS visualizações para baixo.
    """
    if not isinstance(accounts_dict, dict):
        return {}
    sorted_items = sorted(
        accounts_dict.items(),
        key=lambda item: (
            int((item[1] or {}).get("total_views") or 0),
            int((item[1] or {}).get("followers_count") or 0)
        ),
        reverse=True
    )
    return dict(sorted_items)


def merge_best_account_data(base_acc, new_acc):
    """
    Combina duas versões dos dados de uma mesma conta garantindo que NUNCA
    se percam seguidores, visualizações, histórico de velocidade ou lista de posts
    caso uma fonte esteja zerada. Usa sempre max() entre o histórico salvo e a leitura nova.
    """
    if not isinstance(base_acc, dict):
        base_acc = {}
    if not isinstance(new_acc, dict):
        new_acc = {}

    merged = dict(base_acc)
    for k, v in new_acc.items():
        if v is not None and v != "":
            merged[k] = v

    # NUNCA permite que seguidores, views ou media_count diminuam para zero
    old_followers = _safe_int(base_acc.get("followers_count"))
    new_followers = _safe_int(new_acc.get("followers_count"))
    merged["followers_count"] = max(old_followers, new_followers)

    old_views = _safe_int(base_acc.get("total_views"))
    new_views = _safe_int(new_acc.get("total_views"))
    merged["total_views"] = max(old_views, new_views)

    old_media = _safe_int(base_acc.get("media_count"))
    new_media = _safe_int(new_acc.get("media_count"))
    merged["media_count"] = max(old_media, new_media)

    # Mescla a lista de posts preservando todos os posts já vistos e o maior número de views de cada um
    old_posts = base_acc.get("posts_list") if isinstance(base_acc.get("posts_list"), list) else []
    new_posts = new_acc.get("posts_list") if isinstance(new_acc.get("posts_list"), list) else []

    if len(new_posts) > 0 or len(old_posts) > 0:
        posts_by_id = {}
        for p in old_posts:
            if isinstance(p, dict) and p.get("id"):
                pid = str(p["id"])
                p_copy = dict(p)
                p_copy["views"] = _safe_int(p.get("views"))
                posts_by_id[pid] = p_copy

        for p in new_posts:
            if isinstance(p, dict) and p.get("id"):
                pid = str(p["id"])
                if pid in posts_by_id:
                    best_p = dict(posts_by_id[pid])
                    best_p.update(p)
                    best_p["views"] = max(_safe_int(posts_by_id[pid].get("views")), _safe_int(p.get("views")))
                    posts_by_id[pid] = best_p
                else:
                    p_copy = dict(p)
                    p_copy["views"] = _safe_int(p.get("views"))
                    posts_by_id[pid] = p_copy

        merged_posts_list = sorted(posts_by_id.values(), key=lambda x: _safe_int(x.get("views")), reverse=True)
        merged["posts_list"] = merged_posts_list

        # Garante que total_views nunca seja menor que a soma dos posts salvos
        sum_posts_views = sum(_safe_int(x.get("views")) for x in merged_posts_list)
        merged["total_views"] = max(merged["total_views"], sum_posts_views)
        merged["media_count"] = max(merged["media_count"], len(merged_posts_list))
    else:
        merged["posts_list"] = []

    # Preserva o horário real da última coleta válida se a conta tiver caído
    old_ts = str(base_acc.get("metrics_updated_at") or "").strip()
    new_ts = str(new_acc.get("metrics_updated_at") or "").strip()
    invalid_markers = ("", "—", "Sem coleta antes da queda")
    if new_ts not in invalid_markers:
        merged["metrics_updated_at"] = new_ts
    elif old_ts not in invalid_markers:
        merged["metrics_updated_at"] = old_ts
    else:
        merged["metrics_updated_at"] = old_ts or new_ts or "—"

    # Preserva o username real (nunca troca um @username válido por user_123456)
    old_user = str(base_acc.get("username") or "").strip()
    new_user = str(new_acc.get("username") or "").strip()
    if (not new_user or new_user.startswith("user_")) and old_user and not old_user.startswith("user_"):
        merged["username"] = old_user

    # Mescla o histórico de pontos de visualizações e recalcula Views/Min e Views/Hora
    snap_ts = _parse_br_datetime_to_ts(merged.get("metrics_updated_at"))
    merged["views_history"] = _merge_views_history(
        base_acc.get("views_history"),
        new_acc.get("views_history"),
        current_views=merged["total_views"],
        current_ts=snap_ts
    )
    v_min, v_hour = _compute_account_velocity_rates(merged)
    merged["views_per_min"] = v_min
    merged["views_per_hour"] = v_hour

    return merged


def get_env_accounts():
    """
    Lê as contas de ACCOUNTS_CONFIG_JSON e também mescla com quaisquer métricas
    salvas em `contas_conectadas.json` da pasta do projeto.
    """
    raw = os.environ.get("ACCOUNTS_CONFIG_JSON", "").strip()
    accounts_dict = {}

    if raw:
        try:
            data = json.loads(raw)
            if isinstance(data, dict) and "accounts" in data and isinstance(data["accounts"], dict):
                data = data["accounts"]

            if isinstance(data, list):
                for item in data:
                    if isinstance(item, dict):
                        uid = str(item.get("ig_user_id", "")).strip()
                        if uid:
                            accounts_dict[uid] = dict(item)
            elif isinstance(data, dict):
                for uid, item in data.items():
                    if isinstance(item, dict):
                        item_copy = dict(item)
                        uid_clean = str(item_copy.get("ig_user_id") or uid).strip()
                        item_copy["ig_user_id"] = uid_clean
                        accounts_dict[uid_clean] = item_copy
        except Exception as e:
            print(f"[ERRO] Falha ao ler ACCOUNTS_CONFIG_JSON: {e}", flush=True)

    if os.path.exists(CODIGO1_ACCOUNTS_FILE):
        try:
            c1_data = load_json(CODIGO1_ACCOUNTS_FILE, {})
            if isinstance(c1_data, dict):
                c1_accs = c1_data.get("accounts", c1_data if "last_global_sync_ts" not in c1_data else {})
                if isinstance(c1_accs, dict):
                    for uid, item in c1_accs.items():
                        if isinstance(item, dict):
                            uid_str = str(item.get("ig_user_id") or uid).strip()
                            if uid_str in accounts_dict:
                                token_backup = accounts_dict[uid_str].get("access_token") or item.get("access_token")
                                accounts_dict[uid_str] = merge_best_account_data(item, accounts_dict[uid_str])
                                if token_backup:
                                    accounts_dict[uid_str]["access_token"] = token_backup
                            else:
                                item_copy = dict(item)
                                item_copy["ig_user_id"] = uid_str
                                accounts_dict[uid_str] = item_copy
        except Exception as e:
            print(f"[ERRO] Falha ao ler contas_conectadas.json: {e}", flush=True)

    return accounts_dict


def get_saved_state(force_redis=False):
    """
    Carrega e mescla todas as fontes de histórico (Upstash Redis, /tmp, JSON local e RAM).
    Só marca `REDIS_BOOTSTRAPPED = True` se a leitura do Upstash realmente tiver sucesso!
    """
    global MEMORY_LAST_SYNC_TS, MEMORY_LAST_SYNC_STR, LAST_REDIS_FETCH_TS, REDIS_BOOTSTRAPPED

    with ACCOUNTS_LOCK:
        merged_accounts = {}
        last_ts = MEMORY_LAST_SYNC_TS
        last_str = MEMORY_LAST_SYNC_STR

        # 1. Dados vindos da configuração/ambiente ou Código 1
        env_accs = get_env_accounts()
        for uid, acc in env_accs.items():
            uid_s = str(uid).strip()
            clean_acc = dict(acc)
            clean_acc.pop("access_token", None)
            merged_accounts[uid_s] = merge_best_account_data(merged_accounts.get(uid_s, {}), clean_acc)

        # 2. Dados do arquivo contas_monitoradas.json na raiz do projeto (se existir)
        if os.path.exists(BUNDLED_MONITOR_FILE):
            bundled = load_json(BUNDLED_MONITOR_FILE, {})
            if isinstance(bundled, dict):
                b_accs = bundled.get("accounts", bundled if "last_global_sync_ts" not in bundled else {})
                if isinstance(b_accs, dict):
                    for uid, acc in b_accs.items():
                        if isinstance(acc, dict):
                            uid_s = str(acc.get("ig_user_id") or uid).strip()
                            acc_clean = dict(acc)
                            acc_clean.pop("access_token", None)
                            merged_accounts[uid_s] = merge_best_account_data(merged_accounts.get(uid_s, {}), acc_clean)
                if float(bundled.get("last_global_sync_ts") or 0) > last_ts:
                    last_ts = float(bundled["last_global_sync_ts"])
                    last_str = bundled.get("last_global_sync_str", last_str)

        # 3. Dados do arquivo runtime (/tmp ou local)
        if os.path.exists(RUNTIME_ACCOUNTS_FILE):
            runtime = load_json(RUNTIME_ACCOUNTS_FILE, {})
            if isinstance(runtime, dict):
                r_accs = runtime.get("accounts", runtime if "last_global_sync_ts" not in runtime else {})
                if isinstance(r_accs, dict):
                    for uid, acc in r_accs.items():
                        if isinstance(acc, dict):
                            uid_s = str(acc.get("ig_user_id") or uid).strip()
                            acc_clean = dict(acc)
                            acc_clean.pop("access_token", None)
                            merged_accounts[uid_s] = merge_best_account_data(merged_accounts.get(uid_s, {}), acc_clean)
                if float(runtime.get("last_global_sync_ts") or 0) > last_ts:
                    last_ts = float(runtime["last_global_sync_ts"])
                    last_str = runtime.get("last_global_sync_str", last_str)

        # 4. Dados persistentes do UPSTASH REDIS
        now = time.time()
        should_query_redis = (
            is_upstash_configured()
            and (force_redis or not REDIS_BOOTSTRAPPED or (now - LAST_REDIS_FETCH_TS >= REDIS_READ_TTL))
        )

        if should_query_redis:
            ok_redis, redis_state = load_state_from_upstash()
            if ok_redis:
                LAST_REDIS_FETCH_TS = now
                REDIS_BOOTSTRAPPED = True
                if isinstance(redis_state, dict):
                    redis_accs = redis_state.get("accounts", {})
                    if isinstance(redis_accs, dict):
                        for uid, acc in redis_accs.items():
                            if isinstance(acc, dict):
                                uid_s = str(acc.get("ig_user_id") or uid).strip()
                                acc_clean = dict(acc)
                                acc_clean.pop("access_token", None)
                                merged_accounts[uid_s] = merge_best_account_data(
                                    merged_accounts.get(uid_s, {}), acc_clean
                                )
                    if float(redis_state.get("last_global_sync_ts") or 0) > last_ts:
                        last_ts = float(redis_state["last_global_sync_ts"])
                        last_str = redis_state.get("last_global_sync_str", last_str)

        # 5. Cache em memória RAM
        for uid, acc in MEMORY_CACHE_ACCOUNTS.items():
            if isinstance(acc, dict):
                uid_s = str(uid).strip()
                merged_accounts[uid_s] = merge_best_account_data(merged_accounts.get(uid_s, {}), acc)

        # Atualiza a memória RAM com o melhor estado consolidado
        for uid, acc in merged_accounts.items():
            MEMORY_CACHE_ACCOUNTS[str(uid)] = acc

        MEMORY_LAST_SYNC_TS = last_ts
        MEMORY_LAST_SYNC_STR = last_str

        sorted_accs = sort_accounts_by_views_desc(merged_accounts)
        result_state = {
            "last_global_sync_ts": last_ts,
            "last_global_sync_str": last_str,
            "accounts": sorted_accs
        }

        if should_query_redis and REDIS_BOOTSTRAPPED:
            save_json(RUNTIME_ACCOUNTS_FILE, result_state)
            if RUNTIME_ACCOUNTS_FILE != BUNDLED_MONITOR_FILE and os.access(BASE_DIR, os.W_OK):
                save_json(BUNDLED_MONITOR_FILE, result_state)

        return result_state


def save_current_state(state_data):
    """Salva o estado simultaneamente na Memória, no /tmp, no disco local e no Upstash Redis."""
    global MEMORY_LAST_SYNC_TS, MEMORY_LAST_SYNC_STR

    with ACCOUNTS_LOCK:
        if isinstance(state_data, dict):
            if float(state_data.get("last_global_sync_ts") or 0) > MEMORY_LAST_SYNC_TS:
                MEMORY_LAST_SYNC_TS = float(state_data["last_global_sync_ts"])
                MEMORY_LAST_SYNC_STR = state_data.get("last_global_sync_str", MEMORY_LAST_SYNC_STR)

            if isinstance(state_data.get("accounts"), dict):
                for uid, acc in state_data["accounts"].items():
                    uid_s = str(uid).strip()
                    clean_acc = dict(acc)
                    clean_acc.pop("access_token", None)
                    MEMORY_CACHE_ACCOUNTS[uid_s] = merge_best_account_data(
                        MEMORY_CACHE_ACCOUNTS.get(uid_s, {}), clean_acc
                    )
                state_data["accounts"] = sort_accounts_by_views_desc(MEMORY_CACHE_ACCOUNTS)

        # 1. Salva no Upstash Redis (com trava anti-sobrescrita a frio)
        if is_upstash_configured():
            save_state_to_upstash(state_data)

        # 2. Salva no arquivo de runtime (/tmp na Vercel ou local)
        save_json(RUNTIME_ACCOUNTS_FILE, state_data)

        # 3. Se a pasta do projeto tiver permissão de escrita (PC local ou VPS), salva nela também
        if RUNTIME_ACCOUNTS_FILE != BUNDLED_MONITOR_FILE and os.access(BASE_DIR, os.W_OK):
            save_json(BUNDLED_MONITOR_FILE, state_data)


# ==============================================================================
# SEGURANÇA: LOGIN OBRIGATÓRIO (DASHBOARD_USER e DASHBOARD_PASS)
# ==============================================================================

def check_basic_auth(username, password):
    expected_user = os.environ.get("DASHBOARD_USER", "").strip()
    expected_pass = os.environ.get("DASHBOARD_PASS", "").strip()
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
# COLETOR DE MÉTRICAS BLINDADO (NUNCA ZERA SEGUIDORES OU VIEWS SE A CONTA CAIR)
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
    Busca os números atuais na Meta e NUNCA apaga nem reduz para zero os seguidores,
    visualizações totais, histórico de velocidade ou lista de posts que já estavam salvos.
    """
    ig_user_id = str(acc_config.get("ig_user_id", "")).strip()
    access_token = acc_config.get("access_token", "")
    now_ts = time.time()
    now_str = datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S")

    previous_best = merge_best_account_data(acc_config, last_json_entry)
    prev_followers = _safe_int(previous_best.get("followers_count"))
    prev_views = _safe_int(previous_best.get("total_views"))
    prev_media_count = _safe_int(previous_best.get("media_count"))
    prev_posts_list = previous_best.get("posts_list") if isinstance(previous_best.get("posts_list"), list) else []
    prev_updated_at = previous_best.get("metrics_updated_at") or "Sem coleta antes da queda"

    current = dict(previous_best)
    current["ig_user_id"] = ig_user_id
    current["username"] = previous_best.get("username") or acc_config.get("username") or f"user_{ig_user_id}"
    current["label"] = acc_config.get("label") or previous_best.get("label") or "Conta"
    current["connected_at"] = acc_config.get("connected_at") or previous_best.get("connected_at") or "—"
    current["token_type_label"] = acc_config.get("token_type_label") or previous_best.get("token_type_label") or "Longa Duração (60 dias)"

    if not access_token:
        current["account_status"] = previous_best.get("account_status", "Sem Token / Histórico")
        current["account_status_badge"] = previous_best.get("account_status_badge", "warning")
        current["followers_count"] = prev_followers
        current["total_views"] = prev_views
        current["media_count"] = prev_media_count
        current["posts_list"] = prev_posts_list
        current["metrics_updated_at"] = prev_updated_at
        v_min, v_hour = _compute_account_velocity_rates(current)
        current["views_per_min"] = v_min
        current["views_per_hour"] = v_hour
        current.pop("access_token", None)
        return current

    profile_ok = False
    last_profile_error = {}
    fetched_followers = None
    fetched_media_count = None

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
                if "followers_count" in p_data and p_data["followers_count"] is not None:
                    fetched_followers = _safe_int(p_data["followers_count"])
                if "media_count" in p_data and p_data["media_count"] is not None:
                    fetched_media_count = _safe_int(p_data["media_count"])
                if fetched_followers is not None:
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

    if not profile_ok:
        current["followers_count"] = prev_followers
        current["total_views"] = prev_views
        current["media_count"] = prev_media_count
        current["posts_list"] = sorted(prev_posts_list, key=lambda x: _safe_int(x.get("views")), reverse=True)
        current["metrics_updated_at"] = prev_updated_at
        v_min, v_hour = _compute_account_velocity_rates(current)
        current["views_per_min"] = v_min
        current["views_per_hour"] = v_hour
        current.pop("access_token", None)
        return current

    all_media = []
    media_url = f"https://graph.instagram.com/{GRAPH_API_VERSION}/{ig_user_id}/media"
    inline_ok = False
    media_fetch_error = False

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
                    if "error" in data_m:
                        media_fetch_error = True
                    break
        except Exception:
            media_fetch_error = True

    prev_views_by_post_id = {
        str(p.get("id")): _safe_int(p.get("views"))
        for p in prev_posts_list if isinstance(p, dict) and p.get("id")
    }

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
                    v_count = max(int(vals[0]["value"]), prev_views_by_post_id.get(str(mid), 0))
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
                    fetched_v = int(fut.result() or 0)
                    best_v = max(fetched_v, prev_views_by_post_id.get(str(mid), 0))
                    media_views_map[mid] = best_v
                    total_views += best_v
                except Exception:
                    fallback_v = prev_views_by_post_id.get(str(mid), 0)
                    media_views_map[mid] = fallback_v
                    total_views += fallback_v

    posts_ranked = []
    for m_item in all_media:
        mid = m_item.get("id", "")
        v_count = int(media_views_map.get(mid, prev_views_by_post_id.get(str(mid), 0)))
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

    posts_ranked.sort(key=lambda x: _safe_int(x.get("views")), reverse=True)

    best_total_views = max(total_views, prev_views)
    updated_history = _merge_views_history(
        previous_best.get("views_history"),
        [],
        current_views=best_total_views,
        current_ts=now_ts
    )

    current = merge_best_account_data(
        previous_best,
        {
            **current,
            "posts_list": posts_ranked if len(posts_ranked) > 0 else prev_posts_list,
            "followers_count": max(fetched_followers or 0, prev_followers),
            "total_views": best_total_views,
            "media_count": max(fetched_media_count or 0, len(posts_ranked), prev_media_count),
            "metrics_updated_at": now_str if not media_fetch_error else prev_updated_at,
            "views_history": updated_history
        }
    )

    current.pop("access_token", None)
    return current


def refresh_all_accounts_to_json():
    """
    Atualiza todas as contas preservando os melhores dados históricos e salva
    tudo no Upstash Redis com 1 único comando SET ao final.
    """
    with SYNC_LOCK:
        # 1. Obriga a carregar do Upstash primeiro se ainda não tiver feito bootstrap com sucesso
        saved_state = get_saved_state(force_redis=not REDIS_BOOTSTRAPPED)
        saved_accounts = saved_state.get("accounts", {}) if isinstance(saved_state, dict) else {}
        env_accounts = get_env_accounts()

        if not env_accounts:
            if saved_accounts:
                saved_state["accounts"] = sort_accounts_by_views_desc(saved_accounts)
                save_current_state(saved_state)
            return saved_state

        acc_ids = list(env_accounts.keys())
        new_accounts_map = dict(saved_accounts)

        with ThreadPoolExecutor(max_workers=5) as pool:
            futures = {
                acc_id: pool.submit(
                    fetch_account_current_metrics,
                    env_accounts[acc_id],
                    saved_accounts.get(str(acc_id), {})
                )
                for acc_id in acc_ids
            }
            for acc_id, fut in futures.items():
                uid_s = str(acc_id).strip()
                try:
                    updated_acc = fut.result()
                    new_accounts_map[uid_s] = merge_best_account_data(
                        saved_accounts.get(uid_s, {}), updated_acc
                    )
                except Exception as e:
                    print(f"[AVISO MÉTRICAS] Falha ao atualizar conta {uid_s}: {e}", flush=True)
                    if uid_s in saved_accounts:
                        new_accounts_map[uid_s] = saved_accounts[uid_s]

        sorted_accounts_map = sort_accounts_by_views_desc(new_accounts_map)

        now_ts = time.time()
        now_str = datetime.now(BRT).strftime("%d/%m/%Y %H:%M:%S")

        updated_state = {
            "last_global_sync_ts": now_ts,
            "last_global_sync_str": now_str,
            "accounts": sorted_accounts_map
        }
        save_current_state(updated_state)
        return updated_state


# ==============================================================================
# THREAD LOCAL DE 5 EM 5 MINUTOS (AGUARDA BOOT PARA EVITAR RACE CONDITION)
# ==============================================================================

def background_5min_loop():
    # Aguarda 5 segundos no boot para não disputar corrida com a primeira requisição HTTP
    time.sleep(5)
    while True:
        try:
            if not REDIS_BOOTSTRAPPED:
                state = get_saved_state(force_redis=True)
                last_ts = state.get("last_global_sync_ts", 0)
            else:
                last_ts = MEMORY_LAST_SYNC_TS

            if time.time() - last_ts >= 295 or not MEMORY_CACHE_ACCOUNTS:
                refresh_all_accounts_to_json()
        except Exception as e:
            print(f"[ERRO LOOP 5 MIN]: {e}", flush=True)
        time.sleep(60)


bg_thread = threading.Thread(target=background_5min_loop, daemon=True)
bg_thread.start()


# ==============================================================================
# TEMPLATE HTML (CONTAS ORDENADAS POR VIEWS + VIEWS/MIN E VIEWS/HORA + TOTAIS)
# ==============================================================================

HTML_TEMPLATE = r"""
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Monitor 24h — Contas Conectadas (Ordenadas por Visualizações)</title>
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
        .container { max-width: 1280px; margin: 0 auto; display: grid; gap: 20px; }
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
        button.small-btn {
            margin-top: 0; padding: 7px 13px; width: auto; font-size: 0.82rem; border-radius: 6px;
            background: #3b82f6; color: white; border: none; font-weight: 600; cursor: pointer; transition: 0.2s;
        }
        button.small-btn:hover { background: #2563eb; }
        button.small-btn.secondary { background: #0f766e; }
        button.small-btn.secondary:hover { background: #115e59; }
        .posts-toggle-btn {
            margin-top: 0;
            padding: 3px 7px;
            width: auto;
            font-size: 0.78rem;
            border-radius: 5px;
            background: #334155;
            color: #38bdf8;
            border: 1px solid #475569;
            cursor: pointer;
            line-height: 1;
            display: inline-flex;
            align-items: center;
            justify-content: center;
            transition: 0.15s;
        }
        .posts-toggle-btn:hover {
            background: #475569;
            color: #ffffff;
            border-color: #38bdf8;
        }
        .posts-dropdown-row {
            background: #0b1120;
        }
        .posts-dropdown-box {
            padding: 12px 14px;
            border-radius: 8px;
            background: #111a2e;
            border: 1px solid var(--border);
            max-height: 280px;
            overflow-y: auto;
        }
        .post-rank-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            gap: 12px;
            padding: 8px 10px;
            border-bottom: 1px solid #1e293b;
            font-size: 0.84rem;
        }
        .post-rank-item:last-child {
            border-bottom: none;
        }
        .post-rank-link {
            color: #38bdf8;
            text-decoration: none;
            font-weight: 600;
            word-break: break-all;
        }
        .post-rank-link:hover {
            text-decoration: underline;
            color: #7dd3fc;
        }
    </style>
</head>
<body>
<div class="container">
    <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:12px;">
        <div>
            <h1>Painel de Monitoramento 24h (Ordenado por Visualizações)</h1>
            <div class="hint">
                🔒 <b>Proteção Anti-Zeramento Ativa:</b> Se uma conta cair ou bloquear a API, os últimos seguidores, visualizações, taxas por minuto/hora e lista de posts ficam salvos no <b>Upstash Redis</b> e nunca são zerados.
            </div>
        </div>
        <div style="display:flex; gap:8px; flex-wrap:wrap; align-items:center;">
            {% if upstash_active and redis_bootstrapped %}
            <div class="badge success" style="font-size:0.82rem; padding:8px 12px;">
                ☁️ Banco Persistente: <b>Upstash Redis Sincronizado</b>
            </div>
            {% elif upstash_active %}
            <div class="badge warning" style="font-size:0.82rem; padding:8px 12px;">
                ⚠️ Upstash Configurado (Aguardando Conexão...)
            </div>
            {% else %}
            <div class="badge warning" style="font-size:0.82rem; padding:8px 12px;">
                ⚠️ Upstash Redis Não Configurado (Usando /tmp)
            </div>
            {% endif %}
            <div class="badge info" style="font-size:0.86rem; padding:8px 12px;">
                🔄 Última Atualização: <b>{{ last_sync }}</b>
            </div>
            <div class="badge success" style="font-size:0.82rem; padding:8px 12px;">
                ⏱️ Recarregando tela em: <span id="reloadCountdown">60s</span>
            </div>
        </div>
    </div>

    <!-- 3. CONTAS CONECTADAS (ORDENADAS DA MAIOR VISUALIZAÇÃO NO TOPO PARA A MENOR EMBAIXO) -->
    <div class="card">
        <div style="display:flex; justify-content:space-between; align-items:center; flex-wrap:wrap; gap:10px;">
            <div>
                <h2>3. Contas Conectadas ({{ accounts|length }}) — Mais Visualizações no Topo ↓</h2>
                <div style="display:flex; gap:16px; flex-wrap:wrap; font-size:0.88rem; color:var(--muted); margin-top:6px;">
                    <span>Seguidores Totais: <b style="color:#4ade80;">{{ total_followers | fmt_num }}</b></span>
                    <span>Visualizações Totais: <b style="color:#38bdf8;">{{ total_views | fmt_num }}</b></span>
                    <span>Views / Min Totais: <b style="color:#fbbf24;">{{ total_views_per_min | fmt_rate }}/min</b></span>
                    <span>Views / Hora Totais: <b style="color:#c084fc;">{{ total_views_per_hour | fmt_rate }}/h</b></span>
                </div>
            </div>
            <div style="display:flex; gap:8px; flex-wrap:wrap;">
                <button type="button" class="small-btn secondary" onclick="copiarSnapshotVercel(this)" title="Copia o JSON completo com tokens + seguidores + views atuais">
                    💾 Copiar Snapshot JSON
                </button>
                <button type="button" class="small-btn" onclick="atualizarMetricasContas(this)">
                    🔄 Atualizar Seguidores, Visualizações e Posts
                </button>
            </div>
        </div>

        {% if accounts %}
        <div style="overflow-x:auto;">
            <table>
                <thead>
                    <tr>
                        <th style="width:90px; text-align:center;">Rank / Posts</th>
                        <th>@Username</th>
                        <th>Status da Conta (Instagram)</th>
                        <th>Seguidores</th>
                        <th>Visualizações Totais (Posts) ↓</th>
                        <th>Views / Min</th>
                        <th>Views / Hora</th>
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
                            <div style="display:inline-flex; align-items:center; gap:6px;">
                                <span class="badge info" style="min-width:32px; text-align:center;">#{{ loop.index }}</span>
                                <button type="button" id="btn_toggle_{{ acc_id }}" class="posts-toggle-btn" onclick="togglePostsConta('{{ acc_id }}', this)" title="Abrir/fechar lista de posts ordenados por visualizações (maior no topo)">
                                    ↓
                                </button>
                            </div>
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
                        <td>
                            <span class="metric-val" style="color:#fbbf24;">{{ acc.get('views_per_min', 0) | fmt_rate }}/min</span>
                        </td>
                        <td>
                            <span class="metric-val" style="color:#c084fc;">{{ acc.get('views_per_hour', 0) | fmt_rate }}/h</span>
                        </td>
                        <td>{{ acc.ig_user_id }}</td>
                        <td>{{ acc.label }}</td>
                        <td>{{ acc.get('metrics_updated_at', '—') }}</td>
                        <td><span class="badge success">{{ acc.get('token_type_label', 'Longa Duração (60 dias)') }}</span></td>
                    </tr>
                    <tr id="posts_row_{{ acc_id }}" class="posts-dropdown-row" style="display:none;">
                        <td colspan="11" style="padding:10px 16px;">
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
                                        Nenhum post carregado ainda ou esta conta caiu antes da primeira sincronização de posts.
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
        <p class="hint" style="margin-top:16px;">Nenhuma conta configurada em <code>ACCOUNTS_CONFIG_JSON</code> (ou <code>contas_conectadas.json</code>) ainda.</p>
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

    function atualizarMetricasContas(btn) {
        btn.disabled = true;
        btn.innerText = '⏳ Atualizando métricas e salvando no Upstash...';
        window.location.href = '/atualizar_metricas';
    }

    async function copiarSnapshotVercel(btn) {
        const origText = btn.innerText;
        btn.disabled = true;
        btn.innerText = '⏳ Gerando JSON...';
        try {
            const res = await fetch('/api/export_snapshot');
            const data = await res.json();
            await navigator.clipboard.writeText(JSON.stringify(data));
            alert('✅ Snapshot JSON copiado para a área de transferência!');
        } catch (e) {
            alert('Erro ao copiar JSON: ' + e);
        } finally {
            btn.disabled = false;
            btn.innerText = origText;
        }
    }

    window.addEventListener('DOMContentLoaded', () => {
        try {
            const openState = JSON.parse(sessionStorage.getItem('open_posts_rows') || '{}');
            Object.keys(openState).forEach(accId => {
                if (openState[accId]) {
                    const row = document.getElementById('posts_row_' + accId);
                    const btn = document.getElementById('btn_toggle_' + accId);
                    if (row) row.style.display = 'table-row';
                    if (btn) btn.innerText = '↑';
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
# ROTAS FLASK
# ==============================================================================

@app.route("/", methods=["GET"])
@requires_auth
def index():
    # Se ainda não inicializou do Upstash nesta instância, força a leitura primeiro
    state = get_saved_state(force_redis=not REDIS_BOOTSTRAPPED)
    last_ts = state.get("last_global_sync_ts", 0)
    accounts = state.get("accounts", {})

    if time.time() - last_ts >= 300 or not accounts:
        state = refresh_all_accounts_to_json()

    accounts = sort_accounts_by_views_desc(state.get("accounts", {}))
    last_sync_str = state.get("last_global_sync_str", "—")

    # Garante que todas as contas tenham views_per_min e views_per_hour calculados
    for acc in accounts.values():
        if isinstance(acc, dict):
            v_min, v_hour = _compute_account_velocity_rates(acc)
            acc["views_per_min"] = v_min
            acc["views_per_hour"] = v_hour

    total_followers = sum(_safe_int(acc.get("followers_count")) for acc in accounts.values())
    total_views = sum(_safe_int(acc.get("total_views")) for acc in accounts.values())
    total_views_per_min = round(sum(_safe_float(acc.get("views_per_min")) for acc in accounts.values()), 2)
    total_views_per_hour = round(sum(_safe_float(acc.get("views_per_hour")) for acc in accounts.values()), 2)

    return render_template_string(
        HTML_TEMPLATE,
        accounts=accounts,
        last_sync=last_sync_str,
        total_followers=total_followers,
        total_views=total_views,
        total_views_per_min=total_views_per_min,
        total_views_per_hour=total_views_per_hour,
        upstash_active=is_upstash_configured(),
        redis_bootstrapped=REDIS_BOOTSTRAPPED
    )


@app.route("/atualizar_metricas", methods=["GET", "POST"])
@requires_auth
def atualizar_metricas():
    refresh_all_accounts_to_json()
    return redirect(url_for("index"))


@app.route("/api/export_snapshot", methods=["GET"])
@requires_auth
def export_snapshot():
    """
    Exporta o dicionário completo combinando os access_tokens de ACCOUNTS_CONFIG_JSON
    com as últimas métricas e posts coletados.
    """
    env_accs = get_env_accounts()
    state = get_saved_state(force_redis=not REDIS_BOOTSTRAPPED)
    saved_accs = state.get("accounts", {})

    export_dict = {}
    all_uids = set(env_accs.keys()) | set(saved_accs.keys())
    for uid in all_uids:
        uid_s = str(uid).strip()
        env_item = env_accs.get(uid_s, {})
        saved_item = saved_accs.get(uid_s, {})
        merged = merge_best_account_data(env_item, saved_item)
        if env_item.get("access_token"):
            merged["access_token"] = env_item["access_token"]
        export_dict[uid_s] = merged

    return jsonify(export_dict)


@app.route("/api/cron/sync", methods=["GET", "POST"])
def cron_sync():
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
        "upstash_persisted": is_upstash_configured() and REDIS_BOOTSTRAPPED,
        "updated_at": state.get("last_global_sync_str"),
        "accounts_count": len(state.get("accounts", {}))
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)
