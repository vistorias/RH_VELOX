# -*- coding: utf-8 -*-
# ============================================================
# Painel de RH — VELOX (multi-meses via Índice no Google Sheets)
# + Aba Treinamentos (lê "TREINAMENTOS" ou "TREINAMENTO")
# + Padronização de FUNÇÃO (ex: VISTORIADOR*, ANALISTA I/II -> ANALISTA)
# ============================================================

import os
import io
import re
import json
import unicodedata
import calendar
from datetime import datetime, date
from typing import Optional, Tuple, List

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt

import gspread
from google.oauth2 import service_account as gcreds
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ------------------ CONFIG ------------------
st.set_page_config(page_title="Painel de RH", layout="wide")
st.title("Painel de RH")

st.markdown(
    """
<style>
.card-wrap{display:flex;gap:14px;flex-wrap:wrap;margin:10px 0 6px;}
.card{background:#f7f7f9;border-radius:12px;box-shadow:0 1px 4px rgba(0,0,0,.06);padding:14px 16px;min-width:210px;flex:1;text-align:center}
.card h4{margin:0 0 6px;font-size:13px;color:#7a1f1f;font-weight:800}
.card h2{margin:0;font-size:26px;font-weight:900;color:#222}
.card .sub{margin-top:8px;display:inline-block;padding:6px 10px;border-radius:8px;font-size:12px;font-weight:800}
.sub.ok{background:#e8f5ec;color:#197a31;border:1px solid #cce9d4}
.sub.bad{background:#fdeaea;color:#a31616;border:1px solid #f2cccc}
.sub.neu{background:#f1f1f4;color:#444;border:1px solid #e4e4e8}
.section{font-size:18px;font-weight:900;margin:18px 0 8px}
.small{color:#666;font-size:12px}
hr{border:none;border-top:1px solid #eee;margin:14px 0}
</style>
""",
    unsafe_allow_html=True,
)

fast_mode = st.toggle("Modo rápido (pular gráficos/tabelas pesadas)", value=False)


# ------------------ CREDENCIAIS ------------------
def _get_clients():
    idx_id = st.secrets.get("rh_index_sheet_id", "").strip()
    if not idx_id:
        st.error("Faltou `rh_index_sheet_id` no secrets.toml.")
        st.stop()

    try:
        sa_block = st.secrets["gcp_service_account"]
    except Exception:
        st.error("Não encontrei [gcp_service_account] no secrets.toml.")
        st.stop()

    if "json_path" in sa_block:
        path = sa_block["json_path"]
        if not os.path.isabs(path):
            path = os.path.join(os.path.dirname(__file__), path)
        with open(path, "r", encoding="utf-8") as f:
            info = json.load(f)
    else:
        info = dict(sa_block)

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    creds = gcreds.Credentials.from_service_account_info(info, scopes=scopes)
    gc = gspread.authorize(creds)
    drive = build("drive", "v3", credentials=creds, cache_discovery=False)

    return idx_id, gc, drive, info.get("client_email", "")


RH_INDEX_ID, client, DRIVE, SA_EMAIL = _get_clients()


# ------------------ HELPERS ------------------
ID_RE = re.compile(r"/d/([a-zA-Z0-9-_]+)")


def _sheet_id(s: str) -> Optional[str]:
    s = (s or "").strip()
    m = ID_RE.search(s)
    if m:
        return m.group(1)
    return s if re.fullmatch(r"[A-Za-z0-9-_]{20,}", s) else None


def _strip_accents(s: str) -> str:
    if s is None:
        return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(ch))


def _norm_key(s: str) -> str:
    s = _strip_accents(s).upper().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def _upper(x):
    return str(x).upper().strip() if pd.notna(x) else ""


def _yes(v) -> bool:
    return str(v).strip().upper() in {"S", "SIM", "Y", "YES", "TRUE", "1"}


def business_days_count(dini: date, dfim: date) -> int:
    if not (isinstance(dini, date) and isinstance(dfim, date) and dini <= dfim):
        return 0
    return len(pd.bdate_range(dini, dfim))


def _find_col(cols, *names) -> Optional[str]:
    norm = {re.sub(r"\W+", "", _strip_accents(c).upper()): c for c in cols}
    for nm in names:
        key = re.sub(r"\W+", "", _strip_accents(nm).upper())
        if key in norm:
            return norm[key]
    return None


def parse_month_to_ym(m: str) -> Optional[str]:
    if m is None:
        return None
    s = str(m).strip()
    if not s:
        return None

    mm_yyyy = re.match(r"^(\d{1,2})\s*[\/\-]\s*(\d{4})$", s)
    if mm_yyyy:
        mm = int(mm_yyyy.group(1))
        yy = int(mm_yyyy.group(2))
        if 1 <= mm <= 12:
            return f"{yy:04d}-{mm:02d}"

    yyyy_mm = re.match(r"^(\d{4})\s*[\/\-]\s*(\d{1,2})$", s)
    if yyyy_mm:
        yy = int(yyyy_mm.group(1))
        mm = int(yyyy_mm.group(2))
        if 1 <= mm <= 12:
            return f"{yy:04d}-{mm:02d}"

    try:
        dt = pd.to_datetime(s, errors="raise")
        return f"{int(dt.year):04d}-{int(dt.month):02d}"
    except Exception:
        return None


def to_ts(x) -> pd.Timestamp:
    if x is None or (isinstance(x, str) and not x.strip()):
        return pd.NaT
    if pd.isna(x):
        return pd.NaT
    try:
        if isinstance(x, pd.Timestamp):
            return x.normalize()
        if isinstance(x, datetime):
            return pd.Timestamp(x.date())
        if isinstance(x, date):
            return pd.Timestamp(x)
        if isinstance(x, (int, float)) and not isinstance(x, bool):
            base = pd.Timestamp("1899-12-30")
            return (base + pd.to_timedelta(int(x), unit="D")).normalize()
        return pd.to_datetime(x, errors="coerce").normalize()
    except Exception:
        return pd.NaT


def fmt_pct(x):
    return "—" if pd.isna(x) else f"{x:.1f}%".replace(".", ",")


def normalize_funcao(raw: str) -> str:
    """
    Padroniza funções, mantendo as principais.
    Exemplos:
      - VISTORIADOR, VISTORIADOR MÓVEL, VISTORIADORA -> VISTORIADOR
      - ANALISTA, ANALISTA I, ANALISTA II -> ANALISTA
    """
    s = _norm_key(raw or "")
    if not s or s in {"NAN", "NONE"}:
        return ""

    # remove pontuações comuns e deixa só letras/números/espaço
    s_clean = re.sub(r"[^A-Z0-9 ]+", " ", s)
    s_clean = re.sub(r"\s+", " ", s_clean).strip()

    # Vistoriador
    if s_clean.startswith("VISTORIADOR") or s_clean.startswith("VISTORIADORA"):
        return "VISTORIADOR"

    # Analista (analista i/ii/iii, analista 1/2 etc.)
    if s_clean.startswith("ANALISTA"):
        return "ANALISTA"

    # Serviços gerais (se vier variação)
    if "SERVICOS GERAIS" in s_clean or "SERVIÇOS GERAIS" in s:
        return "SERVIÇOS GERAIS"

    # Atendente (variações)
    if s_clean.startswith("ATENDENTE"):
        return "ATENDENTE"

    # Supervisor
    if s_clean.startswith("SUPERVISOR"):
        return "SUPERVISOR"

    # Gerente
    if s_clean.startswith("GERENTE"):
        return "GERENTE"

    return s  # default (já em upper/sem acento)


def normalize_nome(raw: str) -> str:
    s = _strip_accents(raw or "").upper().strip()
    s = re.sub(r"\s+", " ", s)
    return s


def find_sheet_title(candidates: List[str], *wanted: str) -> Optional[str]:
    """
    Encontra nome de aba tolerante a acentos, case e espaços.
    Ex: wanted=("BASE GERAL",) encontra "Base Geral" ou "BASE  GERAL".
    """
    norm_map = {_norm_key(t): t for t in candidates}
    for w in wanted:
        key = _norm_key(w)
        if key in norm_map:
            return norm_map[key]
    # fallback: contém
    for t in candidates:
        tkey = _norm_key(t)
        for w in wanted:
            wkey = _norm_key(w)
            if wkey in tkey:
                return t
    return None


# ------------------ DRIVE (cache) ------------------
@st.cache_data(ttl=300, show_spinner=False)
def _drive_get_file_metadata(file_id: str) -> dict:
    return DRIVE.files().get(fileId=file_id, fields="id,name,mimeType").execute()


@st.cache_data(ttl=300, show_spinner=False)
def _drive_download_bytes(file_id: str) -> bytes:
    req = DRIVE.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()


# ------------------ REGRAS GERÊNCIA (VELOX) ------------------
CITY_TO_GERENTE = {
    "IMPERATRIZ": "JORGE ALEXANDRE",
    "ESTREITO": "JORGE ALEXANDRE",
    "SÃO LUÍS": "MOISÉS NASCIMENTO",
    "SAO LUIS": "MOISÉS NASCIMENTO",
    "PEDREIRAS": "MOISÉS NASCIMENTO",
    "GRAJAÚ": "MOISÉS NASCIMENTO",
    "GRAJAU": "MOISÉS NASCIMENTO",
}


def infer_gerente(cidade: str, supervisor: str) -> str:
    sup = _upper(supervisor)
    if sup and sup not in {"NAN", "NONE"}:
        return sup
    return CITY_TO_GERENTE.get(_upper(cidade), "NÃO DEFINIDO")


# ------------------ LEITURA ÍNDICE ------------------
@st.cache_data(ttl=300, show_spinner=False)
def read_index(sheet_id: str, tab: Optional[str] = None) -> pd.DataFrame:
    sh = client.open_by_key(sheet_id)
    if tab is None:
        tab = sh.worksheets()[0].title
    ws = sh.worksheet(tab)
    rows = ws.get_all_records()
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["URL", "MÊS", "ATIVO"])
    df.columns = [str(c).strip().upper() for c in df.columns]
    for need in ["URL", "MÊS", "ATIVO"]:
        if need not in df.columns:
            df[need] = ""
    return df


# ------------------ LEITURA BASE GERAL (MÊS) ------------------
def _coerce_google_ws_to_df(ws) -> pd.DataFrame:
    rows = ws.get_all_records()
    return pd.DataFrame(rows) if rows else pd.DataFrame()


def _read_sheet_any(file_id: str, mime: str, sheet_wanted: Tuple[str, ...]) -> Optional[pd.DataFrame]:
    """
    Lê uma aba por nome tolerante (case/acentos/espaços).
    Retorna None se não encontrar a aba.
    """
    if mime == "application/vnd.google-apps.spreadsheet":
        sh = client.open_by_key(file_id)
        titles = [w.title for w in sh.worksheets()]
        found = find_sheet_title(titles, *sheet_wanted)
        if not found:
            return None
        ws = sh.worksheet(found)
        return _coerce_google_ws_to_df(ws)

    content = _drive_download_bytes(file_id)
    xls = pd.ExcelFile(io.BytesIO(content), engine="openpyxl")
    found = find_sheet_title(xls.sheet_names, *sheet_wanted)
    if not found:
        return None
    return pd.read_excel(io.BytesIO(content), sheet_name=found, engine="openpyxl")


@st.cache_data(ttl=300, show_spinner=False)
def read_rh_month_base(file_id: str, ym_from_index: str) -> Tuple[pd.DataFrame, str]:
    meta = _drive_get_file_metadata(file_id)
    title = meta.get("name", file_id)
    mime = meta.get("mimeType", "")

    df_raw = _read_sheet_any(file_id, mime, ("BASE GERAL",))
    if df_raw is None or df_raw.empty:
        out = pd.DataFrame()
        out["YM"] = []
        out["SRC_FILE"] = []
        return out, title

    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    cols = list(df_raw.columns)

    c_cidade = _find_col(cols, "CIDADE", "UNIDADE")
    c_nome = _find_col(cols, "NOME DO COLABORADOR", "COLABORADOR", "NOME")
    c_cpf = _find_col(cols, "CPF")
    c_nasc = _find_col(cols, "DATA DE NASCIMENTO", "NASCIMENTO")
    c_funcao = _find_col(cols, "FUNÇÃO", "FUNCAO", "CARGO")
    c_adm = _find_col(cols, "DATA DE ADMISSÃO", "DATA DE ADMISSAO", "ADMISSAO")
    c_dem = _find_col(cols, "DATA DE DEMISSÃO", "DATA DE DEMISSAO", "DEMISSAO")
    c_motivo = _find_col(cols, "MOTIVO DEMISSÃO", "MOTIVO DEMISSAO", "MOTIVO DA DEMISSÃO", "MOTIVO DA DEMISSAO")
    c_status = _find_col(cols, "STATUS")
    c_du = _find_col(cols, "DIAS ÚTEIS MÊS", "DIAS UTEIS MES", "DIAS_UTEIS_MES")
    c_faltas = _find_col(cols, "TOTAL DE FALTAS", "FALTAS")
    c_superv = _find_col(cols, "SUPERVISOR", "GERENTE")

    out = pd.DataFrame()
    out["CIDADE"] = df_raw[c_cidade] if c_cidade else ""
    out["NOME"] = df_raw[c_nome] if c_nome else ""
    out["CPF"] = df_raw[c_cpf] if c_cpf else ""
    out["NASCIMENTO"] = df_raw[c_nasc] if c_nasc else ""
    out["FUNCAO"] = df_raw[c_funcao] if c_funcao else ""
    out["ADMISSAO"] = df_raw[c_adm] if c_adm else ""
    out["DEMISSAO"] = df_raw[c_dem] if c_dem else ""
    out["MOTIVO_DEMISSAO"] = df_raw[c_motivo] if c_motivo else ""
    out["STATUS"] = df_raw[c_status] if c_status else ""
    out["DIAS_UTEIS_MES"] = df_raw[c_du] if c_du else np.nan
    out["FALTAS_MES"] = df_raw[c_faltas] if c_faltas else 0
    out["SUPERVISOR"] = df_raw[c_superv] if c_superv else ""

    out["CIDADE"] = out["CIDADE"].astype(str).map(_upper)
    out["NOME"] = out["NOME"].astype(str).str.strip()
    out["CPF"] = out["CPF"].astype(str).str.strip()

    # padronização FUNÇÃO (aqui é o ajuste pedido)
    out["FUNCAO"] = out["FUNCAO"].astype(str).map(normalize_funcao)

    out["STATUS"] = out["STATUS"].astype(str).map(_upper)
    out["SUPERVISOR"] = out["SUPERVISOR"].astype(str).map(_upper)

    out["ADMISSAO"] = out["ADMISSAO"].apply(to_ts)
    out["DEMISSAO"] = out["DEMISSAO"].apply(to_ts)
    out["NASCIMENTO"] = out["NASCIMENTO"].apply(to_ts)

    out["DIAS_UTEIS_MES"] = pd.to_numeric(out["DIAS_UTEIS_MES"], errors="coerce")
    out["FALTAS_MES"] = pd.to_numeric(out["FALTAS_MES"], errors="coerce").fillna(0).astype(int)

    out = out[out["NOME"].astype(str).str.strip() != ""].copy()

    out["YM"] = ym_from_index
    out["SRC_FILE"] = title

    return out, title


# ------------------ LEITURA TREINAMENTOS (MÊS) ------------------
@st.cache_data(ttl=300, show_spinner=False)
def read_rh_month_training(file_id: str, ym_from_index: str) -> Tuple[pd.DataFrame, str]:
    meta = _drive_get_file_metadata(file_id)
    title = meta.get("name", file_id)
    mime = meta.get("mimeType", "")

    # importante: aceitar "TREINAMENTOS" e "TREINAMENTO"
    df_raw = _read_sheet_any(file_id, mime, ("TREINAMENTOS", "TREINAMENTO"))
    if df_raw is None or df_raw.empty:
        out = pd.DataFrame()
        out["YM"] = []
        out["SRC_FILE"] = []
        return out, title

    df_raw.columns = [str(c).strip() for c in df_raw.columns]
    cols = list(df_raw.columns)

    c_cidade = _find_col(cols, "CIDADE", "UNIDADE")
    c_nome = _find_col(cols, "NOME", "NOME DO COLABORADOR", "COLABORADOR")
    c_conv = _find_col(cols, "CONVOCADO TREINAMENTO", "CONVOCADO_TREINAMENTO", "CONVOCADO")
    c_pres = _find_col(cols, "PRESENCA TREINAMENTO", "PRESENÇA TREINAMENTO", "PRESENCA", "PRESENÇA")
    c_flag = _find_col(cols, "TREINAMENTO", "TREINAMENTO REALIZADO", "REALIZOU TREINAMENTO")
    c_mes = _find_col(cols, "MÊS", "MES")
    c_nome_t = _find_col(cols, "NOME DO TREINAMENTO TRIMESTRAL", "NOME DO TREINAMENTO", "TREINAMENTO (NOME)")
    c_area = _find_col(cols, "AREA/SETOR", "ÁREA/SETOR", "SETOR", "AREA")
    c_solic = _find_col(cols, "SOLICITAÇÃO DE TREINAMENTO PELO GESTOR", "SOLICITACAO DE TREINAMENTO PELO GESTOR", "SOLICITACAO")

    out = pd.DataFrame()
    out["CIDADE"] = df_raw[c_cidade] if c_cidade else ""
    out["NOME"] = df_raw[c_nome] if c_nome else ""
    out["CONVOCADO"] = df_raw[c_conv] if c_conv else np.nan
    out["PRESENCA"] = df_raw[c_pres] if c_pres else ""
    out["TREINAMENTO_FLAG"] = df_raw[c_flag] if c_flag else np.nan
    out["MES_TEXTO"] = df_raw[c_mes] if c_mes else ""
    out["NOME_TREINAMENTO"] = df_raw[c_nome_t] if c_nome_t else ""
    out["AREA_SETOR"] = df_raw[c_area] if c_area else ""
    out["SOLICITACAO_GESTOR"] = df_raw[c_solic] if c_solic else ""

    out["CIDADE"] = out["CIDADE"].astype(str).map(_upper)
    out["NOME"] = out["NOME"].astype(str).str.strip()
    out["NOME_NORM"] = out["NOME"].map(normalize_nome)

    # conv/presença como booleanos
    out["CONVOCADO"] = pd.to_numeric(out["CONVOCADO"], errors="coerce")
    out["FOI_CONVOCADO"] = out["CONVOCADO"].fillna(0).astype(float) > 0

    pres = out["PRESENCA"].astype(str).map(_upper)
    out["PRESENCA_BOOL"] = pres.isin(["SIM", "S", "YES", "Y", "TRUE", "1"])

    flag = pd.to_numeric(out["TREINAMENTO_FLAG"], errors="coerce").fillna(0).astype(float)
    # alguns arquivos podem usar 1/0 em TREINAMENTO
    out["TREINAMENTO_BOOL"] = (flag > 0) | out["PRESENCA_BOOL"]

    out = out[out["NOME"].astype(str).str.strip() != ""].copy()

    out["YM"] = ym_from_index
    out["SRC_FILE"] = title
    out["GERENTE"] = out["CIDADE"].map(lambda c: CITY_TO_GERENTE.get(_upper(c), "NÃO DEFINIDO"))

    return out, title


# ------------------ LOAD ÍNDICE ------------------
idx = read_index(RH_INDEX_ID).copy()
idx["URL"] = idx["URL"].astype(str)
idx["MÊS"] = idx["MÊS"].astype(str).str.strip()
idx["ATIVO"] = idx["ATIVO"].astype(str)

idx = idx[idx["ATIVO"].map(_yes)].copy()
if idx.empty:
    st.error("Seu índice não tem linhas ATIVAS (ATIVO = S).")
    st.stop()

idx["YM"] = idx["MÊS"].apply(parse_month_to_ym)
idx = idx[~idx["YM"].isna()].copy()
if idx.empty:
    st.error("A coluna MÊS do índice não está em um formato reconhecido (ex: 12/2025).")
    st.stop()


# ------------------ LOAD MESES ------------------
ok_msgs, err_msgs = [], []
all_months = []
all_train = []

for _, r in idx.iterrows():
    fid = _sheet_id(r.get("URL", ""))
    ym = r.get("YM", "")
    if not fid or not ym:
        continue

    # BASE
    try:
        d, ttl = read_rh_month_base(fid, ym_from_index=ym)
        if not d.empty:
            all_months.append(d)
        ok_msgs.append(f"{ym} — {ttl} (BASE: {len(d)} linhas)")
    except Exception as e:
        err_msgs.append((fid, ym, f"BASE: {e}"))

    # TREINAMENTOS (não quebra o painel se falhar)
    try:
        tdf, ttl2 = read_rh_month_training(fid, ym_from_index=ym)
        if tdf is not None and not tdf.empty:
            all_train.append(tdf)
            ok_msgs.append(f"{ym} — {ttl2} (TREIN: {len(tdf)} linhas)")
    except Exception as e:
        # aqui é esperado falhar em meses que não tenham a aba
        err_msgs.append((fid, ym, f"TREINAMENTOS: {e}"))

if not all_months:
    st.error("Não consegui ler nenhuma BASE GERAL dos meses (verifique links e permissões).")
    with st.expander("Erros"):
        for fid, ym, e in err_msgs:
            st.write(f"{ym} — {fid}")
            st.write(e)
    st.stop()

df = pd.concat(all_months, ignore_index=True)

# gerente final (coluna padronizada)
df["GERENTE"] = df.apply(lambda r: infer_gerente(r.get("CIDADE", ""), r.get("SUPERVISOR", "")), axis=1)

ym_all = sorted(df["YM"].dropna().unique().tolist())
if not ym_all:
    st.error("Não encontrei meses válidos para o filtro.")
    st.stop()

df_train = pd.concat(all_train, ignore_index=True) if len(all_train) else pd.DataFrame()


# ------------------ UI MÊS ------------------
label_map = {f"{m[5:]}/{m[:4]}": m for m in ym_all}  # MM/YYYY -> YYYY-MM
sel_label = st.selectbox("Mês de referência", options=list(label_map.keys()), index=len(ym_all) - 1)
ym_sel = label_map[sel_label]

ref_year, ref_month = int(ym_sel[:4]), int(ym_sel[5:7])
df_m = df[df["YM"] == ym_sel].copy()

# período no mês
month_start = date(ref_year, ref_month, 1)
last_day = calendar.monthrange(ref_year, ref_month)[1]
month_end = date(ref_year, ref_month, last_day)

drange = st.date_input(
    "Período (dentro do mês)",
    value=(month_start, month_end),
    min_value=month_start,
    max_value=month_end,
    format="DD/MM/YYYY",
)
start_d, end_d = (drange if isinstance(drange, tuple) and len(drange) == 2 else (month_start, month_end))

start_ts = pd.Timestamp(start_d).normalize()
end_ts = pd.Timestamp(end_d).normalize()


# ------------------ FILTROS ------------------
cidades = sorted(df_m["CIDADE"].dropna().unique().tolist())
funcoes = sorted(df_m["FUNCAO"].dropna().unique().tolist())
status_opts = sorted(df_m["STATUS"].dropna().unique().tolist())
gerentes = sorted(df_m["GERENTE"].dropna().unique().tolist())

f1, f2, f3, f4 = st.columns(4)
with f1:
    f_cidade = st.multiselect("Cidade", cidades, default=cidades)
with f2:
    f_funcao = st.multiselect("Função", funcoes, default=funcoes)
with f3:
    f_status = st.multiselect("Status", status_opts, default=status_opts)
with f4:
    f_gerente = st.multiselect("Gerente", gerentes, default=gerentes)

view = df_m.copy()
if f_cidade:
    view = view[view["CIDADE"].isin([_upper(x) for x in f_cidade])]
if f_funcao:
    view = view[view["FUNCAO"].isin([_upper(x) for x in f_funcao])]
if f_status:
    view = view[view["STATUS"].isin([_upper(x) for x in f_status])]
if f_gerente:
    view = view[view["GERENTE"].isin([_upper(x) for x in f_gerente])]

if view.empty:
    st.info("Sem dados no recorte selecionado.")
    st.stop()


# ------------------ FUNÇÕES DE CÁLCULO ------------------
def is_active_asof(row, asof_d: date) -> bool:
    asof = pd.Timestamp(asof_d).normalize()
    adm = to_ts(row.get("ADMISSAO", pd.NaT))
    dem = to_ts(row.get("DEMISSAO", pd.NaT))
    if pd.isna(adm):
        return False
    if adm > asof:
        return False
    if not pd.isna(dem) and dem <= asof:
        return False
    return True


def count_active_asof(df_in: pd.DataFrame, asof_d: date) -> int:
    if df_in.empty:
        return 0
    mask = df_in.apply(lambda r: is_active_asof(r, asof_d), axis=1)
    return int(mask.sum())


def turnover_pct(n_adm, n_dem, hc_start, hc_end):
    hc_avg = (hc_start + hc_end) / 2 if (hc_start + hc_end) > 0 else 0
    return np.nan if hc_avg == 0 else (((n_adm + n_dem) / 2) / hc_avg) * 100


def abs_rate(faltas, hc_end, dias_uteis_mes):
    den = hc_end * dias_uteis_mes
    return np.nan if den == 0 else (faltas / den) * 100


def age_years(nasc_ts: pd.Timestamp, asof: pd.Timestamp) -> float:
    if pd.isna(nasc_ts):
        return np.nan
    return float((asof - nasc_ts).days / 365.25)


def tenure_months(adm_ts: pd.Timestamp, asof: pd.Timestamp) -> float:
    if pd.isna(adm_ts):
        return np.nan
    return float((asof - adm_ts).days / 30.4375)


def bar(df_plot, x, y, height=320, title=""):
    base = alt.Chart(df_plot).encode(
        x=alt.X(f"{x}:N", sort="-y", axis=alt.Axis(labelAngle=0, labelLimit=220), title=""),
        y=alt.Y(f"{y}:Q", title=""),
        tooltip=[x, y],
    )
    return (base.mark_bar() + base.mark_text(dy=-6).encode(text=alt.Text(f"{y}:Q", format=".0f"))).properties(
        height=height, title=title
    )


def line(df_plot, x, y, height=280, title=""):
    c = (
        alt.Chart(df_plot)
        .mark_line(point=True)
        .encode(
            x=alt.X(f"{x}:N", title=""),
            y=alt.Y(f"{y}:Q", title=""),
            tooltip=[x, y],
        )
        .properties(height=height, title=title)
    )
    return c


# ------------------ KPIs GERAIS (MÊS/RECORTE) ------------------
hc_start = count_active_asof(view, start_d)
hc_end = count_active_asof(view, end_d)
hc_avg = (hc_start + hc_end) / 2 if (hc_start + hc_end) > 0 else 0

adm_period = view[(view["ADMISSAO"].notna()) & (view["ADMISSAO"] >= start_ts) & (view["ADMISSAO"] <= end_ts)]
dem_period = view[(view["DEMISSAO"].notna()) & (view["DEMISSAO"] >= start_ts) & (view["DEMISSAO"] <= end_ts)]

n_adm = int(len(adm_period))
n_dem = int(len(dem_period))
turnover = turnover_pct(n_adm, n_dem, hc_start, hc_end)

du_mes = pd.to_numeric(view["DIAS_UTEIS_MES"], errors="coerce").dropna()
dias_uteis_mes = int(du_mes.mode().iloc[0]) if len(du_mes) else business_days_count(month_start, month_end)

faltas_total_mes = int(pd.to_numeric(view["FALTAS_MES"], errors="coerce").fillna(0).sum())
abs_pct = abs_rate(faltas_total_mes, hc_end, dias_uteis_mes)

pend_cadastro = int((view["ADMISSAO"].isna() | (view["FUNCAO"].astype(str).str.strip() == "")).sum())

ativo_count = int((view["STATUS"] == "ATIVO").sum()) if "STATUS" in view.columns else 0
deslig_count = int((view["STATUS"] == "DESLIGADO").sum()) if "STATUS" in view.columns else 0

cards_html = f"""
<div class="card-wrap">
  <div class='card'><h4>Headcount (fim)</h4><h2>{hc_end:,}</h2><span class='sub neu'>início: {hc_start:,} | médio: {hc_avg:.1f}</span></div>
  <div class='card'><h4>Ativos x Desligados</h4><h2>{ativo_count:,} / {deslig_count:,}</h2><span class='sub neu'>status do mês</span></div>
  <div class='card'><h4>Admissões</h4><h2>{n_adm:,}</h2></div>
  <div class='card'><h4>Demissões</h4><h2>{n_dem:,}</h2></div>
  <div class='card'><h4>Turnover</h4><h2>{fmt_pct(turnover)}</h2><span class='sub neu'>((adm+dem)/2)/HC médio</span></div>
  <div class='card'><h4>Absenteísmo</h4><h2>{fmt_pct(abs_pct)}</h2><span class='sub neu'>faltas: {faltas_total_mes:,} | DU: {dias_uteis_mes}</span></div>
  <div class='card'><h4>Pendências cadastro</h4><h2>{pend_cadastro:,}</h2></div>
</div>
"""
st.markdown(cards_html.replace(",", "."), unsafe_allow_html=True)


# ------------------ TABS ------------------
tab_over, tab_people, tab_gest, tab_abs, tab_turn, tab_train, tab_base = st.tabs(
    ["Visão Geral", "Pessoas", "Gestão/Liderança", "Absenteísmo", "Turnover", "Treinamentos", "Base"]
)

# ============================================================
# VISÃO GERAL
# ============================================================
with tab_over:
    st.markdown("<div class='section'>Distribuições (mês)</div>", unsafe_allow_html=True)
    c1, c2 = st.columns(2)

    tmp = view.copy()
    tmp["ATIVO_FIM"] = tmp.apply(lambda r: 1 if is_active_asof(r, end_d) else 0, axis=1)

    by_func = tmp.groupby("FUNCAO")["ATIVO_FIM"].sum().reset_index(name="QTD").sort_values("QTD", ascending=False)
    with c1:
        st.altair_chart(bar(by_func, "FUNCAO", "QTD", height=340, title="Headcount por função (fim do período)"), use_container_width=True)

    by_city = tmp.groupby("CIDADE")["ATIVO_FIM"].sum().reset_index(name="QTD").sort_values("QTD", ascending=False)
    with c2:
        st.altair_chart(bar(by_city, "CIDADE", "QTD", height=340, title="Headcount por cidade (fim do período)"), use_container_width=True)

    if not fast_mode:
        st.markdown("<div class='section'>Movimentações (período)</div>", unsafe_allow_html=True)
        m1, m2 = st.columns(2)

        a = adm_period.groupby("FUNCAO")["NOME"].size().reset_index(name="ADM")
        d = dem_period.groupby("FUNCAO")["NOME"].size().reset_index(name="DEM")
        md = a.merge(d, on="FUNCAO", how="outer").fillna(0)
        md["ADM"] = md["ADM"].astype(int)
        md["DEM"] = md["DEM"].astype(int)

        with m1:
            if len(md):
                md_long = md.melt(id_vars=["FUNCAO"], value_vars=["ADM", "DEM"], var_name="TIPO", value_name="QTD")
                chart = alt.Chart(md_long).mark_bar().encode(
                    x=alt.X("FUNCAO:N", sort="-y", axis=alt.Axis(labelAngle=0, labelLimit=220), title=""),
                    y=alt.Y("QTD:Q", title=""),
                    color=alt.Color("TIPO:N", legend=alt.Legend(title="")),
                    tooltip=["FUNCAO", "TIPO", "QTD"],
                ).properties(height=340, title="Admissões x Demissões por função")
                st.altair_chart(chart, use_container_width=True)
            else:
                st.info("Sem admissões/demissões no período.")

        a2 = adm_period.groupby("CIDADE")["NOME"].size().reset_index(name="ADM")
        d2 = dem_period.groupby("CIDADE")["NOME"].size().reset_index(name="DEM")
        md2 = a2.merge(d2, on="CIDADE", how="outer").fillna(0)
        md2["ADM"] = md2["ADM"].astype(int)
        md2["DEM"] = md2["DEM"].astype(int)

        with m2:
            if len(md2):
                md2_long = md2.melt(id_vars=["CIDADE"], value_vars=["ADM", "DEM"], var_name="TIPO", value_name="QTD")
                chart2 = alt.Chart(md2_long).mark_bar().encode(
                    x=alt.X("CIDADE:N", sort="-y", axis=alt.Axis(labelAngle=0, labelLimit=220), title=""),
                    y=alt.Y("QTD:Q", title=""),
                    color=alt.Color("TIPO:N", legend=alt.Legend(title="")),
                    tooltip=["CIDADE", "TIPO", "QTD"],
                ).properties(height=340, title="Admissões x Demissões por cidade")
                st.altair_chart(chart2, use_container_width=True)
            else:
                st.info("Sem admissões/demissões no período.")


# ============================================================
# PESSOAS
# ============================================================
with tab_people:
    st.markdown("<div class='section'>Tempo de casa e perfil etário (fim do período)</div>", unsafe_allow_html=True)

    ppl = view.copy()
    ppl["ATIVO_FIM"] = ppl.apply(lambda r: 1 if is_active_asof(r, end_d) else 0, axis=1)
    ppl = ppl[ppl["ATIVO_FIM"] == 1].copy()

    ppl["IDADE"] = ppl["NASCIMENTO"].apply(lambda x: age_years(to_ts(x), end_ts))
    ppl["TEMPO_CASA_MESES"] = ppl["ADMISSAO"].apply(lambda x: tenure_months(to_ts(x), end_ts))

    ppl["FAIXA_IDADE"] = pd.cut(
        ppl["IDADE"],
        bins=[0, 18, 25, 35, 45, 55, 200],
        labels=["<18", "18-25", "26-35", "36-45", "46-55", "56+"],
        right=True,
        include_lowest=True,
    )

    ppl["FAIXA_TEMPO"] = pd.cut(
        ppl["TEMPO_CASA_MESES"],
        bins=[-1, 3, 6, 12, 24, 36, 60, 9999],
        labels=["0-3m", "4-6m", "7-12m", "13-24m", "25-36m", "37-60m", "60m+"],
        right=True,
    )

    c1, c2 = st.columns(2)

    by_age = ppl.groupby("FAIXA_IDADE")["NOME"].size().reset_index(name="QTD")
    by_age["FAIXA_IDADE"] = by_age["FAIXA_IDADE"].astype(str)
    with c1:
        st.altair_chart(bar(by_age, "FAIXA_IDADE", "QTD", height=320, title="Perfil etário (ativos no fim)"), use_container_width=True)

    by_ten = ppl.groupby("FAIXA_TEMPO")["NOME"].size().reset_index(name="QTD")
    by_ten["FAIXA_TEMPO"] = by_ten["FAIXA_TEMPO"].astype(str)
    with c2:
        st.altair_chart(bar(by_ten, "FAIXA_TEMPO", "QTD", height=320, title="Tempo de casa (ativos no fim)"), use_container_width=True)

    st.markdown("<div class='section'>Distribuição (ativos no fim)</div>", unsafe_allow_html=True)
    c3, c4 = st.columns(2)
    by_city2 = ppl.groupby("CIDADE")["NOME"].size().reset_index(name="QTD").sort_values("QTD", ascending=False)
    by_func2 = ppl.groupby("FUNCAO")["NOME"].size().reset_index(name="QTD").sort_values("QTD", ascending=False)
    with c3:
        st.altair_chart(bar(by_city2, "CIDADE", "QTD", height=340, title="Ativos por cidade"), use_container_width=True)
    with c4:
        st.altair_chart(bar(by_func2, "FUNCAO", "QTD", height=340, title="Ativos por função"), use_container_width=True)


# ============================================================
# GESTÃO / LIDERANÇA
# ============================================================
with tab_gest:
    st.markdown("<div class='section'>Estrutura por gerente (fim do período)</div>", unsafe_allow_html=True)

    gdf = view.copy()
    gdf["ATIVO_FIM"] = gdf.apply(lambda r: 1 if is_active_asof(r, end_d) else 0, axis=1)

    hc_g = gdf.groupby("GERENTE")["ATIVO_FIM"].sum().reset_index(name="HC_FIM").sort_values("HC_FIM", ascending=False)

    adm_g = adm_period.copy()
    dem_g = dem_period.copy()
    adm_g["GERENTE"] = adm_g.apply(lambda r: infer_gerente(r.get("CIDADE", ""), r.get("SUPERVISOR", "")), axis=1)
    dem_g["GERENTE"] = dem_g.apply(lambda r: infer_gerente(r.get("CIDADE", ""), r.get("SUPERVISOR", "")), axis=1)
    mov_g = (
        adm_g.groupby("GERENTE")["NOME"].size().reset_index(name="ADM")
        .merge(dem_g.groupby("GERENTE")["NOME"].size().reset_index(name="DEM"), on="GERENTE", how="outer")
        .fillna(0)
    )
    mov_g["ADM"] = mov_g["ADM"].astype(int)
    mov_g["DEM"] = mov_g["DEM"].astype(int)

    falt_g = gdf.groupby("GERENTE")["FALTAS_MES"].sum().reset_index(name="FALTAS")
    abs_g = hc_g.merge(falt_g, on="GERENTE", how="left").fillna(0)
    abs_g["ABS_%"] = abs_g.apply(lambda r: abs_rate(int(r["FALTAS"]), int(r["HC_FIM"]), dias_uteis_mes), axis=1)

    gdf["ATIVO_INI"] = gdf.apply(lambda r: 1 if is_active_asof(r, start_d) else 0, axis=1)
    hc_ini = gdf.groupby("GERENTE")["ATIVO_INI"].sum().reset_index(name="HC_INI")
    hc_fim = gdf.groupby("GERENTE")["ATIVO_FIM"].sum().reset_index(name="HC_FIM")
    turn_g = hc_ini.merge(hc_fim, on="GERENTE", how="outer").fillna(0).merge(mov_g, on="GERENTE", how="left").fillna(0)
    turn_g["TURN_%"] = turn_g.apply(
        lambda r: turnover_pct(int(r["ADM"]), int(r["DEM"]), int(r["HC_INI"]), int(r["HC_FIM"])), axis=1
    )

    c1, c2 = st.columns(2)
    with c1:
        st.altair_chart(bar(hc_g, "GERENTE", "HC_FIM", height=320, title="Headcount por gerente (fim)"), use_container_width=True)
    with c2:
        tmp2 = mov_g.copy()
        if len(tmp2):
            tmp_long = tmp2.melt(id_vars=["GERENTE"], value_vars=["ADM", "DEM"], var_name="TIPO", value_name="QTD")
            chart = alt.Chart(tmp_long).mark_bar().encode(
                x=alt.X("GERENTE:N", sort="-y", axis=alt.Axis(labelAngle=0, labelLimit=260), title=""),
                y=alt.Y("QTD:Q", title=""),
                color=alt.Color("TIPO:N", legend=alt.Legend(title="")),
                tooltip=["GERENTE", "TIPO", "QTD"],
            ).properties(height=320, title="Movimentações por gerente (período)")
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("Sem movimentações.")

    c3, c4 = st.columns(2)
    with c3:
        abs_plot = abs_g.sort_values("ABS_%", ascending=False).copy()
        abs_plot["ABS_%"] = abs_plot["ABS_%"].fillna(0)
        chart = alt.Chart(abs_plot).mark_bar().encode(
            x=alt.X("GERENTE:N", sort="-y", axis=alt.Axis(labelAngle=0, labelLimit=260), title=""),
            y=alt.Y("ABS_%:Q", title=""),
            tooltip=["GERENTE", alt.Tooltip("ABS_%:Q", format=".2f")],
        ).properties(height=320, title="Absenteísmo por gerente (%)")
        st.altair_chart(chart, use_container_width=True)
    with c4:
        turn_plot = turn_g.sort_values("TURN_%", ascending=False).copy()
        chart = alt.Chart(turn_plot).mark_bar().encode(
            x=alt.X("GERENTE:N", sort="-y", axis=alt.Axis(labelAngle=0, labelLimit=260), title=""),
            y=alt.Y("TURN_%:Q", title=""),
            tooltip=["GERENTE", alt.Tooltip("TURN_%:Q", format=".2f")],
        ).properties(height=320, title="Turnover por gerente (%)")
        st.altair_chart(chart, use_container_width=True)

    st.markdown("<div class='section'>Regra de estrutura (fixa)</div>", unsafe_allow_html=True)
    st.write("Jorge Alexandre → Imperatriz e Estreito")
    st.write("Moisés Nascimento → São Luís, Pedreiras e Grajaú")


# ============================================================
# ABSENTEÍSMO
# ============================================================
with tab_abs:
    st.markdown("<div class='section'>Absenteísmo (mês)</div>", unsafe_allow_html=True)

    abs_city = view.copy()
    abs_city["ATIVO_FIM"] = abs_city.apply(lambda r: 1 if is_active_asof(r, end_d) else 0, axis=1)
    city_hc = abs_city.groupby("CIDADE")["ATIVO_FIM"].sum().reset_index(name="HC_FIM")
    city_f = abs_city.groupby("CIDADE")["FALTAS_MES"].sum().reset_index(name="FALTAS")
    abs_city2 = city_hc.merge(city_f, on="CIDADE", how="left").fillna(0)
    abs_city2["ABS_%"] = abs_city2.apply(lambda r: abs_rate(int(r["FALTAS"]), int(r["HC_FIM"]), dias_uteis_mes), axis=1)

    top = view.copy()
    top["FALTAS_MES"] = pd.to_numeric(top["FALTAS_MES"], errors="coerce").fillna(0).astype(int)
    top = top.sort_values("FALTAS_MES", ascending=False).head(15)

    c1, c2 = st.columns(2)
    with c1:
        plot = abs_city2.sort_values("ABS_%", ascending=False).copy()
        chart = alt.Chart(plot).mark_bar().encode(
            x=alt.X("CIDADE:N", sort="-y", axis=alt.Axis(labelAngle=0, labelLimit=240), title=""),
            y=alt.Y("ABS_%:Q", title=""),
            tooltip=["CIDADE", "HC_FIM", "FALTAS", alt.Tooltip("ABS_%:Q", format=".2f")],
        ).properties(height=340, title="Absenteísmo por cidade (%)")
        st.altair_chart(chart, use_container_width=True)

    with c2:
        chart = alt.Chart(top).mark_bar().encode(
            x=alt.X("NOME:N", sort="-y", axis=alt.Axis(labelAngle=0, labelLimit=260), title=""),
            y=alt.Y("FALTAS_MES:Q", title=""),
            tooltip=["NOME", "CIDADE", "FUNCAO", "GERENTE", "FALTAS_MES"],
        ).properties(height=340, title="Top 15 faltas por colaborador (mês)")
        st.altair_chart(chart, use_container_width=True)

    st.markdown("<div class='section'>Reincidência (comparando meses)</div>", unsafe_allow_html=True)

    hist = df.copy()
    hist["KEY"] = (hist["CPF"].replace({"": np.nan}) + "|" + hist["NOME"].astype(str)).fillna(hist["NOME"].astype(str))
    hist["FALTAS_MES"] = pd.to_numeric(hist["FALTAS_MES"], errors="coerce").fillna(0).astype(int)
    rec = (
        hist.groupby(["KEY"])["FALTAS_MES"]
        .apply(lambda s: int((s > 0).sum()))
        .reset_index(name="MESES_COM_FALTA")
        .sort_values("MESES_COM_FALTA", ascending=False)
    )
    rec = rec[rec["MESES_COM_FALTA"] >= 2].head(25)

    if len(rec):
        st.dataframe(rec, use_container_width=True, hide_index=True)
        st.caption("Reincidência = apareceu com faltas (>0) em 2+ meses no histórico carregado.")
    else:
        st.info("Sem reincidência (2+ meses com falta) no histórico carregado.")


# ============================================================
# TURNOVER
# ============================================================
with tab_turn:
    st.markdown("<div class='section'>Turnover mês a mês</div>", unsafe_allow_html=True)

    rows = []
    for ym in ym_all:
        df_ym = df[df["YM"] == ym].copy()
        if df_ym.empty:
            continue

        y, m = int(ym[:4]), int(ym[5:7])
        ms = date(y, m, 1)
        me = date(y, m, calendar.monthrange(y, m)[1])
        st_ts = pd.Timestamp(ms).normalize()
        en_ts = pd.Timestamp(me).normalize()

        hc_s = count_active_asof(df_ym, ms)
        hc_e = count_active_asof(df_ym, me)

        adm = df_ym[(df_ym["ADMISSAO"].notna()) & (df_ym["ADMISSAO"] >= st_ts) & (df_ym["ADMISSAO"] <= en_ts)]
        dem = df_ym[(df_ym["DEMISSAO"].notna()) & (df_ym["DEMISSAO"] >= st_ts) & (df_ym["DEMISSAO"] <= en_ts)]
        t = turnover_pct(int(len(adm)), int(len(dem)), hc_s, hc_e)

        rows.append(
            {
                "MÊS": f"{ym[5:]}/{ym[:4]}",
                "HC_INI": hc_s,
                "HC_FIM": hc_e,
                "ADM": int(len(adm)),
                "DEM": int(len(dem)),
                "TURN_%": float(t) if not pd.isna(t) else np.nan,
            }
        )

    tdf = pd.DataFrame(rows)
    if len(tdf):
        c1, c2 = st.columns(2)
        with c1:
            st.altair_chart(line(tdf, "MÊS", "TURN_%", height=300, title="Turnover (%) mês a mês"), use_container_width=True)
        with c2:
            md = tdf[["MÊS", "ADM", "DEM"]].copy()
            md_long = md.melt(id_vars=["MÊS"], value_vars=["ADM", "DEM"], var_name="TIPO", value_name="QTD")
            chart = alt.Chart(md_long).mark_bar().encode(
                x=alt.X("MÊS:N", title="", axis=alt.Axis(labelAngle=0)),
                y=alt.Y("QTD:Q", title=""),
                color=alt.Color("TIPO:N", legend=alt.Legend(title="")),
                tooltip=["MÊS", "TIPO", "QTD"],
            ).properties(height=300, title="Entradas x Saídas mês a mês")
            st.altair_chart(chart, use_container_width=True)

        st.markdown("<div class='section'>Motivos de desligamento (mês selecionado)</div>", unsafe_allow_html=True)
        dem_sel = dem_period.copy()
        dem_sel["MOTIVO_DEMISSAO"] = dem_sel["MOTIVO_DEMISSAO"].astype(str).str.strip()
        by_mot = (
            dem_sel.groupby("MOTIVO_DEMISSAO")["NOME"].size().reset_index(name="QTD").sort_values("QTD", ascending=False)
        )
        by_mot = by_mot[by_mot["MOTIVO_DEMISSAO"].astype(str).str.strip() != ""]
        if len(by_mot):
            st.altair_chart(bar(by_mot, "MOTIVO_DEMISSAO", "QTD", height=320, title="Motivos de desligamento"), use_container_width=True)
        else:
            st.info("Sem motivo de desligamento preenchido no mês/recorte.")

        st.markdown("<div class='section'>Cidades com maior impacto (demissões no período)</div>", unsafe_allow_html=True)
        by_city_dem = dem_period.groupby("CIDADE")["NOME"].size().reset_index(name="DEM").sort_values("DEM", ascending=False)
        if len(by_city_dem):
            st.altair_chart(bar(by_city_dem, "CIDADE", "DEM", height=300, title="Demissões por cidade (período)"), use_container_width=True)
        else:
            st.info("Sem demissões no período.")
    else:
        st.info("Não foi possível calcular histórico mês a mês.")


# ============================================================
# TREINAMENTOS
# ============================================================
with tab_train:
    st.markdown("<div class='section'>Treinamentos (mês)</div>", unsafe_allow_html=True)

    if df_train is None or df_train.empty:
        st.warning("Sem base de treinamentos carregada (aba TREINAMENTOS/TREINAMENTO não encontrada nos meses do índice).")
    else:
        tr = df_train[df_train["YM"] == ym_sel].copy()
        if tr.empty:
            st.warning("Sem linhas na aba de treinamentos para o mês selecionado.")
        else:
            # aplica filtros de cidade/gerente compatíveis com o recorte atual (quando fizer sentido)
            if f_cidade:
                tr = tr[tr["CIDADE"].isin([_upper(x) for x in f_cidade])]
            if f_gerente:
                tr = tr[tr["GERENTE"].isin([_upper(x) for x in f_gerente])]

            # tenta enriquecer com FUNCAO/CPF da BASE (match por nome + cidade)
            base_keys = view.copy()
            base_keys["NOME_NORM"] = base_keys["NOME"].map(normalize_nome)
            base_keys["CIDADE"] = base_keys["CIDADE"].map(_upper)
            base_keys = base_keys[["NOME_NORM", "CIDADE", "FUNCAO", "CPF", "GERENTE"]].drop_duplicates()

            tr = tr.merge(base_keys, on=["NOME_NORM", "CIDADE"], how="left", suffixes=("", "_BASE"))

            # KPIs
            convocados = int(tr["FOI_CONVOCADO"].sum()) if "FOI_CONVOCADO" in tr.columns else 0
            presentes = int((tr["FOI_CONVOCADO"] & tr["PRESENCA_BOOL"]).sum()) if "PRESENCA_BOOL" in tr.columns else 0
            cobertura = np.nan if convocados == 0 else (presentes / convocados) * 100

            # por treinamento (nome)
            tname = tr.copy()
            tname["NOME_TREINAMENTO"] = tname["NOME_TREINAMENTO"].astype(str).str.strip()
            tname.loc[tname["NOME_TREINAMENTO"] == "", "NOME_TREINAMENTO"] = "NÃO INFORMADO"

            by_t = (
                tname.groupby("NOME_TREINAMENTO")
                .agg(CONVOCADOS=("FOI_CONVOCADO", "sum"), PRESENTES=("PRESENCA_BOOL", "sum"))
                .reset_index()
            )
            by_t["COB_%"] = by_t.apply(lambda r: np.nan if r["CONVOCADOS"] == 0 else (r["PRESENTES"] / r["CONVOCADOS"]) * 100, axis=1)
            by_t = by_t.sort_values("CONVOCADOS", ascending=False)

            # por gerente
            by_g = (
                tr.groupby("GERENTE")
                .agg(CONVOCADOS=("FOI_CONVOCADO", "sum"), PRESENTES=("PRESENCA_BOOL", "sum"))
                .reset_index()
            )
            by_g["COB_%"] = by_g.apply(lambda r: np.nan if r["CONVOCADOS"] == 0 else (r["PRESENTES"] / r["CONVOCADOS"]) * 100, axis=1)
            by_g = by_g.sort_values("COB_%", ascending=False)

            # por cidade
            by_c = (
                tr.groupby("CIDADE")
                .agg(CONVOCADOS=("FOI_CONVOCADO", "sum"), PRESENTES=("PRESENCA_BOOL", "sum"))
                .reset_index()
            )
            by_c["COB_%"] = by_c.apply(lambda r: np.nan if r["CONVOCADOS"] == 0 else (r["PRESENTES"] / r["CONVOCADOS"]) * 100, axis=1)
            by_c = by_c.sort_values("COB_%", ascending=False)

            cards_t = f"""
<div class="card-wrap">
  <div class='card'><h4>Convocados</h4><h2>{convocados:,}</h2></div>
  <div class='card'><h4>Presentes</h4><h2>{presentes:,}</h2></div>
  <div class='card'><h4>Cobertura</h4><h2>{fmt_pct(cobertura)}</h2><span class='sub neu'>presentes/convocados</span></div>
</div>
"""
            st.markdown(cards_t.replace(",", "."), unsafe_allow_html=True)

            c1, c2 = st.columns(2)
            with c1:
                if len(by_t):
                    plot = by_t.copy()
                    plot["COB_%"] = plot["COB_%"].fillna(0)
                    ch = alt.Chart(plot).mark_bar().encode(
                        x=alt.X("NOME_TREINAMENTO:N", sort="-y", axis=alt.Axis(labelAngle=0, labelLimit=260), title=""),
                        y=alt.Y("COB_%:Q", title=""),
                        tooltip=["NOME_TREINAMENTO", "CONVOCADOS", "PRESENTES", alt.Tooltip("COB_%:Q", format=".2f")],
                    ).properties(height=320, title="Cobertura por treinamento (%)")
                    st.altair_chart(ch, use_container_width=True)
                else:
                    st.info("Sem dados para agrupar por treinamento.")

            with c2:
                if len(by_g):
                    plot = by_g.copy()
                    plot["COB_%"] = plot["COB_%"].fillna(0)
                    ch = alt.Chart(plot).mark_bar().encode(
                        x=alt.X("GERENTE:N", sort="-y", axis=alt.Axis(labelAngle=0, labelLimit=260), title=""),
                        y=alt.Y("COB_%:Q", title=""),
                        tooltip=["GERENTE", "CONVOCADOS", "PRESENTES", alt.Tooltip("COB_%:Q", format=".2f")],
                    ).properties(height=320, title="Cobertura por gerente (%)")
                    st.altair_chart(ch, use_container_width=True)
                else:
                    st.info("Sem dados para agrupar por gerente.")

            c3, c4 = st.columns(2)
            with c3:
                if len(by_c):
                    plot = by_c.copy()
                    plot["COB_%"] = plot["COB_%"].fillna(0)
                    ch = alt.Chart(plot).mark_bar().encode(
                        x=alt.X("CIDADE:N", sort="-y", axis=alt.Axis(labelAngle=0, labelLimit=240), title=""),
                        y=alt.Y("COB_%:Q", title=""),
                        tooltip=["CIDADE", "CONVOCADOS", "PRESENTES", alt.Tooltip("COB_%:Q", format=".2f")],
                    ).properties(height=320, title="Cobertura por cidade (%)")
                    st.altair_chart(ch, use_container_width=True)
                else:
                    st.info("Sem dados para agrupar por cidade.")

            with c4:
                # lista de faltantes (convocados e não presentes)
                falt = tr[(tr["FOI_CONVOCADO"] == True) & (tr["PRESENCA_BOOL"] == False)].copy()
                if len(falt):
                    cols_f = ["CIDADE", "GERENTE", "NOME", "FUNCAO", "NOME_TREINAMENTO", "AREA_SETOR", "SOLICITACAO_GESTOR"]
                    for cc in cols_f:
                        if cc not in falt.columns:
                            falt[cc] = ""
                    st.markdown("<div class='section'>Convocados ausentes</div>", unsafe_allow_html=True)
                    st.dataframe(falt[cols_f].sort_values(["CIDADE", "NOME"]), use_container_width=True, hide_index=True)
                else:
                    st.markdown("<div class='section'>Convocados ausentes</div>", unsafe_allow_html=True)
                    st.info("Nenhum convocado ausente no recorte.")

            st.markdown("<div class='section'>Base Treinamentos (recorte do mês)</div>", unsafe_allow_html=True)
            cols_show_t = [
                "YM", "CIDADE", "GERENTE", "NOME", "FUNCAO",
                "FOI_CONVOCADO", "PRESENCA_BOOL", "NOME_TREINAMENTO",
                "AREA_SETOR", "SOLICITACAO_GESTOR", "SRC_FILE"
            ]
            for c in cols_show_t:
                if c not in tr.columns:
                    tr[c] = ""
            st.dataframe(tr[cols_show_t], use_container_width=True, hide_index=True)


# ============================================================
# BASE + EXPORT
# ============================================================
with tab_base:
    st.markdown("<div class='section'>Base (recorte atual)</div>", unsafe_allow_html=True)

    cols_show = [
        "YM", "CIDADE", "GERENTE", "NOME", "CPF", "FUNCAO",
        "ADMISSAO", "DEMISSAO", "MOTIVO_DEMISSAO", "STATUS",
        "FALTAS_MES", "DIAS_UTEIS_MES", "SUPERVISOR", "SRC_FILE"
    ]
    for c in cols_show:
        if c not in view.columns:
            view[c] = ""

    df_show = view[cols_show].copy()
    for c in ["ADMISSAO", "DEMISSAO"]:
        df_show[c] = pd.to_datetime(df_show[c], errors="coerce").dt.strftime("%d/%m/%Y")

    # ajuste: garantir que a base sempre apareça
    st.dataframe(df_show, use_container_width=True, hide_index=True)

    # Export Excel
    try:
        import openpyxl  # noqa

        xbuf = io.BytesIO()
        with pd.ExcelWriter(xbuf, engine="openpyxl") as writer:
            df_show.to_excel(writer, index=False, sheet_name="BASE_RECORTE")

            resumo = pd.DataFrame(
                {
                    "Métrica": [
                        "Mês referência", "Período início", "Período fim",
                        "HC fim", "HC início", "HC médio",
                        "Ativos (status)", "Desligados (status)",
                        "Admissões", "Demissões",
                        "Turnover %", "Faltas mês", "Absenteísmo %", "Dias úteis mês"
                    ],
                    "Valor": [
                        sel_label, start_d.strftime("%d/%m/%Y"), end_d.strftime("%d/%m/%Y"),
                        hc_end, hc_start, round(hc_avg, 1),
                        ativo_count, deslig_count,
                        n_adm, n_dem,
                        None if pd.isna(turnover) else round(float(turnover), 2),
                        faltas_total_mes,
                        None if pd.isna(abs_pct) else round(float(abs_pct), 2),
                        dias_uteis_mes
                    ],
                }
            )
            resumo.to_excel(writer, index=False, sheet_name="RESUMO")

            if df_train is not None and not df_train.empty:
                trm = df_train[df_train["YM"] == ym_sel].copy()
                if not trm.empty:
                    keep = ["YM", "CIDADE", "GERENTE", "NOME", "FOI_CONVOCADO", "PRESENCA_BOOL", "NOME_TREINAMENTO", "AREA_SETOR", "SOLICITACAO_GESTOR"]
                    for k in keep:
                        if k not in trm.columns:
                            trm[k] = ""
                    trm[keep].to_excel(writer, index=False, sheet_name="TREINAMENTOS")

        xbuf.seek(0)
        st.download_button(
            "Baixar Excel (recorte + resumo)",
            data=xbuf,
            file_name=f"rh_recorte_{ym_sel}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        )
    except Exception:
        st.caption("<span class='small'>Exportação Excel indisponível no ambiente.</span>", unsafe_allow_html=True)

    with st.expander("Diagnóstico", expanded=False):
        st.write("Service account:", SA_EMAIL)
        st.write("Meses carregados (BASE):", ym_all)
        if df_train is not None and not df_train.empty:
            st.write("Meses com TREINAMENTOS:", sorted(df_train["YM"].dropna().unique().tolist()))
        else:
            st.write("TREINAMENTOS: nenhum mês carregado.")
        if ok_msgs:
            st.write("OK:")
            st.write("\n".join(ok_msgs))
        if err_msgs:
            st.write("Falhas:")
            for fid, ym, e in err_msgs:
                st.write(f"{ym} — {fid}")
                st.write(e)

    st.markdown("<hr>", unsafe_allow_html=True)
    st.caption("Obs.: Heatmap diário depende de uma base com granularidade por dia.")
