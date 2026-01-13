# -*- coding: utf-8 -*-
# ============================================================
# Painel de RH — Template replicável por marca (multi-meses)
# ============================================================

import os, io, json, re, unicodedata, calendar
from datetime import datetime, date
from typing import Optional, Tuple, Dict

import streamlit as st
import pandas as pd
import numpy as np
import altair as alt

import gspread
from oauth2client.service_account import ServiceAccountCredentials

from google.oauth2 import service_account as gcreds
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload


# ------------------ CONFIG BÁSICA ------------------
st.set_page_config(page_title="Painel de RH", layout="wide")
st.title("Painel de RH")

st.markdown(
    """
<style>
.card-wrap{display:flex;gap:16px;flex-wrap:wrap;margin:12px 0 6px;}
.card{background:#f7f7f9;border-radius:12px;box-shadow:0 1px 4px rgba(0,0,0,.06);padding:14px 16px;min-width:220px;flex:1;text-align:center}
.card h4{margin:0 0 6px;font-size:14px;color:#0f172a;font-weight:700}
.card h2{margin:0;font-size:26px;font-weight:900;color:#111827}
.card .sub{margin-top:8px;display:inline-block;padding:6px 10px;border-radius:8px;font-size:12px;font-weight:700}
.sub.ok{background:#e8f5ec;color:#197a31;border:1px solid #cce9d4}
.sub.bad{background:#fdeaea;color:#a31616;border:1px solid #f2cccc}
.sub.neu{background:#f1f1f4;color:#444;border:1px solid #e4e4e8}
.section{font-size:18px;font-weight:900;margin:20px 0 8px;color:#0f172a}
.small{color:#6b7280;font-size:13px}
hr{border:0;border-top:1px solid #e5e7eb;margin:18px 0}
</style>
""",
    unsafe_allow_html=True,
)

fast_mode = st.toggle("Modo rápido (pular gráficos/tabelas pesadas)", value=False)


# ------------------ CREDENCIAL ------------------
def _get_client_and_drive():
    try:
        block = st.secrets["gcp_service_account"]
    except Exception:
        st.error("Não encontrei [gcp_service_account] no secrets.toml.")
        st.stop()

    info = dict(block)

    scopes = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.readonly",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_dict(info, scopes)
    gc = gspread.authorize(creds)

    dscopes = ["https://www.googleapis.com/auth/drive.readonly"]
    gcred = gcreds.Credentials.from_service_account_info(info, scopes=dscopes)
    drive = build("drive", "v3", credentials=gcred, cache_discovery=False)

    return gc, drive


client, DRIVE = _get_client_and_drive()

RH_INDEX_ID = st.secrets.get("rh_index_sheet_id", "").strip()
BRAND_NAME = st.secrets.get("rh_brand_name", "").strip().upper()

if not RH_INDEX_ID:
    st.error("Faltou `rh_index_sheet_id` no secrets.toml.")
    st.stop()


# ------------------ HELPERS ------------------
ID_RE = re.compile(r"/d/([a-zA-Z0-9-_]+)")

def _sheet_id(s: str) -> Optional[str]:
    s = (s or "").strip()
    m = ID_RE.search(s)
    if m:
        return m.group(1)
    return s if re.fullmatch(r"[A-Za-z0-9-_]{20,}", s) else None

def _upper(x):
    return str(x).upper().strip() if pd.notna(x) else ""

def _yes(v) -> bool:
    return str(v).strip().upper() in {"S", "SIM", "Y", "YES", "TRUE", "1"}

def _strip_accents(s: str) -> str:
    if s is None: return ""
    return "".join(ch for ch in unicodedata.normalize("NFKD", str(s)) if not unicodedata.combining(ch))

def _norm_col(s: str) -> str:
    return re.sub(r"\W+", "", _strip_accents(str(s)).upper())

def _find_col(cols, *names) -> Optional[str]:
    norm = {_norm_col(c): c for c in cols}
    for nm in names:
        key = _norm_col(nm)
        if key in norm:
            return norm[key]
    return None

def parse_date_any(x):
    if pd.isna(x) or x == "":
        return pd.NaT
    if isinstance(x, (int, float)) and not isinstance(x, bool):
        try:
            return (pd.to_datetime("1899-12-30") + pd.to_timedelta(int(x), unit="D")).date()
        except Exception:
            pass
    s = str(x).strip()
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except Exception:
            pass
    try:
        return pd.to_datetime(s).date()
    except Exception:
        return pd.NaT

def _ym_token(x: str) -> Optional[str]:
    if not x: return None
    s = str(x).strip()
    if re.fullmatch(r"\d{2}/\d{4}", s):
        mm, yy = s.split("/")
        return f"{yy}-{int(mm):02d}"
    if re.fullmatch(r"\d{4}-\d{2}", s):
        return s
    return None

def _drive_get_file_metadata(file_id: str) -> dict:
    return DRIVE.files().get(fileId=file_id, fields="id, name, mimeType").execute()

def _drive_download_bytes(file_id: str) -> bytes:
    req = DRIVE.files().get_media(fileId=file_id)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, req, chunksize=1024 * 1024)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    return buf.getvalue()

def _safe_read_excel(content: bytes, sheet_name: str) -> pd.DataFrame:
    try:
        df = pd.read_excel(io.BytesIO(content), sheet_name=sheet_name, engine="openpyxl")
        if df is None:
            return pd.DataFrame()
        return df
    except Exception:
        return pd.DataFrame()

def _safe_read_gsheet(sh, tab: str) -> pd.DataFrame:
    try:
        ws = sh.worksheet(tab)
        rows = ws.get_all_records()
        return pd.DataFrame(rows) if rows else pd.DataFrame()
    except Exception:
        return pd.DataFrame()


# ------------------ ÍNDICE (cache) ------------------
@st.cache_data(ttl=300, show_spinner=False)
def read_index(sheet_id: str, tab: str = "ARQUIVOS") -> pd.DataFrame:
    sh = client.open_by_key(sheet_id)
    ws = sh.worksheet(tab)
    rows = ws.get_all_records()
    df = pd.DataFrame(rows) if rows else pd.DataFrame(columns=["URL", "MÊS", "ATIVO"])
    df.columns = [str(c).strip().upper() for c in df.columns]
    for need in ["URL", "MÊS", "ATIVO"]:
        if need not in df.columns:
            df[need] = ""
    return df


# ------------------ LEITURA DE UM MÊS RH (cache) ------------------
@st.cache_data(ttl=300, show_spinner=False)
def read_rh_month(file_id: str, ym: Optional[str]) -> Tuple[Dict[str, pd.DataFrame], str]:
    meta = _drive_get_file_metadata(file_id)
    title = meta.get("name", file_id)
    mime = meta.get("mimeType", "")

    tabs = ["BASE GERAL", "BASE PRESENÇA", "ABSENTEISMO E TURNOVER", "TREINAMENTOS", "VELOX"]
    out: Dict[str, pd.DataFrame] = {}

    if mime == "application/vnd.google-apps.spreadsheet":
        sh = client.open_by_key(file_id)
        for t in tabs:
            df = _safe_read_gsheet(sh, t)
            if not df.empty:
                out[t] = df.copy()
    else:
        if not (mime.startswith("application/vnd.openxmlformats-officedocument") or mime.startswith("application/vnd.ms-excel")):
            raise RuntimeError(f"Tipo de arquivo não suportado: {mime} ({title})")
        content = _drive_download_bytes(file_id)
        for t in tabs:
            df = _safe_read_excel(content, t)
            if not df.empty:
                out[t] = df.copy()

    # padronizações por aba
    for k, df in list(out.items()):
        df.columns = [str(c).strip() for c in df.columns]
        df["YM"] = ym or ""
        out[k] = df

    return out, title


# ------------------ CARREGAR MESES ATIVOS ------------------
idx = read_index(RH_INDEX_ID)
idx = idx[idx["ATIVO"].map(_yes)].copy()

if idx.empty:
    st.error("Seu índice está vazio ou sem meses ATIVOS.")
    st.stop()

packs = []
ok, fail = [], []

for _, r in idx.iterrows():
    sid = _sheet_id(r.get("URL", ""))
    ym = _ym_token(r.get("MÊS", ""))
    if not sid:
        continue
    try:
        pack, ttl = read_rh_month(sid, ym=ym)
        packs.append(pack)
        ok.append(f"{r.get('MÊS','')} — {ttl}")
    except Exception as e:
        fail.append((sid, e))

if not packs:
    st.error("Não consegui ler nenhum mês do RH a partir do índice.")
    if fail:
        with st.expander("Falhas (debug)"):
            for sid, e in fail:
                st.write(sid)
                st.exception(e)
    st.stop()


# ------------------ CONSOLIDAÇÃO ------------------
def _concat_from_packs(tab_name: str) -> pd.DataFrame:
    frames = []
    for pack in packs:
        if tab_name in pack and isinstance(pack[tab_name], pd.DataFrame) and not pack[tab_name].empty:
            frames.append(pack[tab_name])
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    df.columns = [str(c).strip() for c in df.columns]
    return df

df_base_geral = _concat_from_packs("BASE GERAL")
df_presenca   = _concat_from_packs("BASE PRESENÇA")
df_abs_turn   = _concat_from_packs("ABSENTEISMO E TURNOVER")
df_treinos    = _concat_from_packs("TREINAMENTOS")

# Normalizações mínimas (para filtros)
def _ensure_cols(df: pd.DataFrame, mapping: Dict[str, Tuple[str, ...]]) -> pd.DataFrame:
    if df.empty:
        return df
    cols = list(df.columns)
    for std, aliases in mapping.items():
        c = _find_col(cols, std, *aliases)
        if c and c != std:
            df = df.rename(columns={c: std})
        if std not in df.columns:
            df[std] = ""
    return df

# BASE GERAL: tenta padronizar colunas comuns
df_base_geral = _ensure_cols(df_base_geral, {
    "COLABORADOR": ("NOME", "NOME COMPLETO", "FUNCIONARIO", "FUNCIONÁRIO"),
    "EMPRESA": ("MARCA",),
    "UNIDADE": ("CIDADE", "FILIAL"),
    "CARGO": ("FUNCAO", "FUNÇÃO"),
    "SITUACAO": ("STATUS",),
    "DATA_ADMISSAO": ("ADMISSAO", "DATA DE ADMISSÃO", "DATAADMISSAO"),
    "DATA_DESLIGAMENTO": ("DESLIGAMENTO", "DATA DE DESLIGAMENTO", "DATADESLIGAMENTO"),
    "TEMPO_CASA": ("PERFIL", "NOVATO/VETERANO"),
})
if not df_base_geral.empty:
    df_base_geral["EMPRESA"] = df_base_geral["EMPRESA"].astype(str).map(_upper)
    df_base_geral["UNIDADE"] = df_base_geral["UNIDADE"].astype(str).map(_upper)
    df_base_geral["CARGO"] = df_base_geral["CARGO"].astype(str).map(_upper)
    df_base_geral["SITUACAO"] = df_base_geral["SITUACAO"].astype(str).map(_upper)
    df_base_geral["TEMPO_CASA"] = df_base_geral["TEMPO_CASA"].astype(str).map(_upper)
    df_base_geral["DATA_ADMISSAO"] = df_base_geral["DATA_ADMISSAO"].apply(parse_date_any)
    df_base_geral["DATA_DESLIGAMENTO"] = df_base_geral["DATA_DESLIGAMENTO"].apply(parse_date_any)

# PRESENÇA
df_presenca = _ensure_cols(df_presenca, {
    "COLABORADOR": ("NOME", "FUNCIONARIO", "FUNCIONÁRIO"),
    "EMPRESA": ("MARCA",),
    "UNIDADE": ("CIDADE",),
    "DATA": ("DIA",),
    "STATUS_PRESENCA": ("STATUS", "PRESENCA", "PRESENÇA"),
})
if not df_presenca.empty:
    df_presenca["EMPRESA"] = df_presenca["EMPRESA"].astype(str).map(_upper)
    df_presenca["UNIDADE"] = df_presenca["UNIDADE"].astype(str).map(_upper)
    df_presenca["DATA"] = df_presenca["DATA"].apply(parse_date_any)
    df_presenca["STATUS_PRESENCA"] = df_presenca["STATUS_PRESENCA"].astype(str).map(_upper)

# ABSENTEÍSMO E TURNOVER
df_abs_turn = _ensure_cols(df_abs_turn, {
    "EMPRESA": ("MARCA",),
    "UNIDADE": ("CIDADE",),
    "DATA": ("DIA", "PERIODO"),
    "TIPO": ("TIPO_EVENTO", "EVENTO"),
    "QTD": ("QUANTIDADE",),
})
if not df_abs_turn.empty:
    df_abs_turn["EMPRESA"] = df_abs_turn["EMPRESA"].astype(str).map(_upper)
    df_abs_turn["UNIDADE"] = df_abs_turn["UNIDADE"].astype(str).map(_upper)
    df_abs_turn["DATA"] = df_abs_turn["DATA"].apply(parse_date_any)
    df_abs_turn["TIPO"] = df_abs_turn["TIPO"].astype(str).map(_upper)
    df_abs_turn["QTD"] = pd.to_numeric(df_abs_turn["QTD"], errors="coerce").fillna(0).astype(int)

# TREINAMENTOS
df_treinos = _ensure_cols(df_treinos, {
    "COLABORADOR": ("NOME", "FUNCIONARIO", "FUNCIONÁRIO"),
    "EMPRESA": ("MARCA",),
    "UNIDADE": ("CIDADE",),
    "TREINAMENTO": ("CURSO", "TEMA"),
    "DATA": ("DIA",),
    "STATUS": ("SITUACAO",),
})
if not df_treinos.empty:
    df_treinos["EMPRESA"] = df_treinos["EMPRESA"].astype(str).map(_upper)
    df_treinos["UNIDADE"] = df_treinos["UNIDADE"].astype(str).map(_upper)
    df_treinos["TREINAMENTO"] = df_treinos["TREINAMENTO"].astype(str).str.strip()
    df_treinos["STATUS"] = df_treinos["STATUS"].astype(str).map(_upper)
    df_treinos["DATA"] = df_treinos["DATA"].apply(parse_date_any)


# ------------------ FILTROS (topo) ------------------
# Meses disponíveis
months = sorted([m for m in pd.unique(
    pd.concat([
        df_base_geral.get("YM", pd.Series([], dtype=str)),
        df_presenca.get("YM", pd.Series([], dtype=str)),
        df_abs_turn.get("YM", pd.Series([], dtype=str)),
        df_treinos.get("YM", pd.Series([], dtype=str)),
    ], ignore_index=True).astype(str)
) if m and m != "nan"])

if not months:
    st.error("Não encontrei YM (MÊS) nos dados consolidados.")
    st.stop()

def _label_from_ym(ym: str) -> str:
    try:
        y, m = ym.split("-")
        return f"{int(m):02d}/{y}"
    except Exception:
        return ym

label_map = {_label_from_ym(m): m for m in months}
sel_label = st.selectbox("Mês de referência", options=list(label_map.keys()), index=len(months)-1)
ym_sel = label_map[sel_label]

c1, c2, c3 = st.columns([1.4, 1.4, 1.8])

# Opções
emp_opts = []
if "EMPRESA" in df_base_geral.columns and not df_base_geral.empty:
    emp_opts = sorted([e for e in df_base_geral["EMPRESA"].dropna().unique().tolist() if str(e).strip()])

with c1:
    if emp_opts:
        default_emp = [BRAND_NAME] if BRAND_NAME and BRAND_NAME in emp_opts else emp_opts
        f_emp = st.multiselect("Empresa/Marca", options=emp_opts, default=default_emp)
    else:
        f_emp = []

unid_opts = []
if "UNIDADE" in df_base_geral.columns and not df_base_geral.empty:
    unid_opts = sorted([u for u in df_base_geral["UNIDADE"].dropna().unique().tolist() if str(u).strip()])
with c2:
    f_unid = st.multiselect("Unidade/Cidade", options=unid_opts, default=unid_opts)

with c3:
    perfil_sel = st.radio("Perfil (tempo de casa)", ["Todos", "Novatos", "Veteranos"], horizontal=True)

def _apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    out = df.copy()
    if "YM" in out.columns:
        out = out[out["YM"].astype(str) == str(ym_sel)]
    if f_emp and "EMPRESA" in out.columns:
        out = out[out["EMPRESA"].isin([_upper(x) for x in f_emp])]
    if f_unid and "UNIDADE" in out.columns:
        out = out[out["UNIDADE"].isin([_upper(x) for x in f_unid])]
    if perfil_sel != "Todos" and "TEMPO_CASA" in out.columns:
        alvo = "NOVATO" if perfil_sel == "Novatos" else "VETERANO"
        out = out[out["TEMPO_CASA"] == alvo]
    return out

bg = _apply_filters(df_base_geral)
pr = _apply_filters(df_presenca)
ab = _apply_filters(df_abs_turn)
tr = _apply_filters(df_treinos)


# ------------------ KPIs (cards) ------------------
# Colaboradores ativos (se houver coluna SITUACAO)
if not bg.empty:
    sit = bg["SITUACAO"].astype(str).map(_upper) if "SITUACAO" in bg.columns else pd.Series([""] * len(bg))
    ativos = bg[~sit.str.contains(r"DESLIG|INAT", regex=True, na=False)].copy()
else:
    ativos = bg.copy()

total_colab = int(ativos["COLABORADOR"].nunique()) if (not ativos.empty and "COLABORADOR" in ativos.columns) else 0

# admissões/desligamentos do mês (se tiver datas)
def _is_in_month(d: date, ym: str) -> bool:
    try:
        y, m = ym.split("-")
        return isinstance(d, date) and d.year == int(y) and d.month == int(m)
    except Exception:
        return False

admis = 0
desl = 0
if not bg.empty:
    if "DATA_ADMISSAO" in bg.columns:
        admis = int(sum(_is_in_month(d, ym_sel) for d in bg["DATA_ADMISSAO"].tolist()))
    if "DATA_DESLIGAMENTO" in bg.columns:
        desl = int(sum(_is_in_month(d, ym_sel) for d in bg["DATA_DESLIGAMENTO"].tolist()))

# turnover % (fórmula que você usa)
turnover_pct = np.nan
if total_colab > 0:
    turnover_pct = (((admis + desl) / 2) / total_colab) * 100

turnover_str = "—" if np.isnan(turnover_pct) else f"{turnover_pct:.1f}%".replace(".", ",")

# absenteísmo (se tiver base própria: tenta inferir)
abs_pct = np.nan
abs_note = "—"
if not pr.empty and "STATUS_PRESENCA" in pr.columns:
    # entende como falta qualquer status contendo FALTA/AUSÊNCI/ATESTADO (ajuste fino depois)
    stt = pr["STATUS_PRESENCA"].astype(str).map(_upper)
    total_reg = len(pr)
    faltas = int(stt.str.contains(r"FALTA|AUSEN|AUSÊN|ATEST", regex=True, na=False).sum())
    if total_reg > 0:
        abs_pct = faltas / total_reg * 100
        abs_note = f"{faltas} de {total_reg}"
abs_str = "—" if np.isnan(abs_pct) else f"{abs_pct:.1f}%".replace(".", ",")

# treinamentos concluídos
treinos_total = int(len(tr)) if not tr.empty else 0
treinos_ok = 0
if not tr.empty and "STATUS" in tr.columns:
    treinos_ok = int(tr["STATUS"].astype(str).map(_upper).isin(["CONCLUIDO", "CONCLUÍDO", "OK", "REALIZADO", "SIM"]).sum())

cards_html = f"""
<div class="card-wrap">
  <div class='card'>
    <h4>Colaboradores ativos</h4>
    <h2>{total_colab:,}</h2>
    <span class='sub neu'>mês: {sel_label}</span>
  </div>
  <div class='card'>
    <h4>Admissões (mês)</h4>
    <h2>{admis:,}</h2>
  </div>
  <div class='card'>
    <h4>Desligamentos (mês)</h4>
    <h2>{desl:,}</h2>
  </div>
  <div class='card'>
    <h4>Turnover (mês)</h4>
    <h2>{turnover_str}</h2>
    <span class='sub neu'>((adm+desl)/2)/ativos</span>
  </div>
  <div class='card'>
    <h4>Absenteísmo (proxy)</h4>
    <h2>{abs_str}</h2>
    <span class='sub neu'>{abs_note}</span>
  </div>
  <div class='card'>
    <h4>Treinamentos (mês)</h4>
    <h2>{treinos_ok:,}/{treinos_total:,}</h2>
    <span class='sub neu'>concluídos/total</span>
  </div>
</div>
""".replace(",", ".")
st.markdown(cards_html, unsafe_allow_html=True)


# ------------------ ABAS ------------------
tab1, tab2, tab3, tab4 = st.tabs(["Visão geral", "Turnover/Absenteísmo", "Treinamentos", "Detalhes"])

# ---------- VISÃO GERAL ----------
with tab1:
    st.markdown('<div class="section">Distribuição</div>', unsafe_allow_html=True)

    cA, cB = st.columns(2)

    with cA:
        if not ativos.empty and "UNIDADE" in ativos.columns:
            by_un = ativos.groupby("UNIDADE")["COLABORADOR"].nunique().reset_index(name="ATIVOS")
            by_un = by_un.sort_values("ATIVOS", ascending=False)
            ch = alt.Chart(by_un).mark_bar().encode(
                x=alt.X("UNIDADE:N", sort='-y', axis=alt.Axis(labelAngle=0, labelLimit=220), title="UNIDADE"),
                y=alt.Y("ATIVOS:Q", title="ATIVOS"),
                tooltip=["UNIDADE", "ATIVOS"],
            ).properties(height=320)
            st.subheader("Ativos por unidade")
            st.altair_chart(ch, use_container_width=True)
        else:
            st.info("Sem dados suficientes para 'Ativos por unidade'.")

    with cB:
        if not ativos.empty and "CARGO" in ativos.columns:
            by_cg = ativos.groupby("CARGO")["COLABORADOR"].nunique().reset_index(name="ATIVOS")
            by_cg = by_cg.sort_values("ATIVOS", ascending=False).head(15)
            ch = alt.Chart(by_cg).mark_bar().encode(
                x=alt.X("CARGO:N", sort='-y', axis=alt.Axis(labelAngle=0, labelLimit=240), title="CARGO"),
                y=alt.Y("ATIVOS:Q", title="ATIVOS"),
                tooltip=["CARGO", "ATIVOS"],
            ).properties(height=320)
            st.subheader("Top cargos (até 15)")
            st.altair_chart(ch, use_container_width=True)
        else:
            st.info("Sem dados suficientes para 'Top cargos'.")

    if not fast_mode:
        st.markdown('<div class="section">Movimentações do mês</div>', unsafe_allow_html=True)

        mov = []
        if not bg.empty and "COLABORADOR" in bg.columns:
            if "DATA_ADMISSAO" in bg.columns:
                a = bg[bg["DATA_ADMISSAO"].map(lambda d: _is_in_month(d, ym_sel))].copy()
                if not a.empty:
                    a["TIPO"] = "Admissão"
                    mov.append(a[["COLABORADOR", "UNIDADE", "CARGO", "TIPO"]].copy())
            if "DATA_DESLIGAMENTO" in bg.columns:
                d = bg[bg["DATA_DESLIGAMENTO"].map(lambda x: _is_in_month(x, ym_sel))].copy()
                if not d.empty:
                    d["TIPO"] = "Desligamento"
                    mov.append(d[["COLABORADOR", "UNIDADE", "CARGO", "TIPO"]].copy())
        if mov:
            movdf = pd.concat(mov, ignore_index=True)
            st.dataframe(movdf, use_container_width=True, hide_index=True)
        else:
            st.info("Sem dados de admissões/desligamentos no mês (ou sem colunas de datas).")


# ---------- TURNOVER / ABSENTEÍSMO ----------
with tab2:
    st.markdown('<div class="section">Turnover</div>', unsafe_allow_html=True)

    # Série por mês (se tiver BG consolidado com datas)
    if not df_base_geral.empty and "DATA_ADMISSAO" in df_base_geral.columns:
        base_all = df_base_geral.copy()
        if f_emp and "EMPRESA" in base_all.columns:
            base_all = base_all[base_all["EMPRESA"].isin([_upper(x) for x in f_emp])]
        if f_unid and "UNIDADE" in base_all.columns:
            base_all = base_all[base_all["UNIDADE"].isin([_upper(x) for x in f_unid])]
        if perfil_sel != "Todos" and "TEMPO_CASA" in base_all.columns:
            alvo = "NOVATO" if perfil_sel == "Novatos" else "VETERANO"
            base_all = base_all[base_all["TEMPO_CASA"] == alvo]

        # cria coluna YM a partir de datas (quando existir)
        def _ym_from_date(d):
            return f"{d.year}-{d.month:02d}" if isinstance(d, date) else ""
        base_all["_YM_ADM"] = base_all["DATA_ADMISSAO"].map(_ym_from_date) if "DATA_ADMISSAO" in base_all.columns else ""
        base_all["_YM_DES"] = base_all["DATA_DESLIGAMENTO"].map(_ym_from_date) if "DATA_DESLIGAMENTO" in base_all.columns else ""

        adm_s = base_all[base_all["_YM_ADM"] != ""].groupby("_YM_ADM")["COLABORADOR"].nunique().reset_index(name="ADM")
        des_s = base_all[base_all["_YM_DES"] != ""].groupby("_YM_DES")["COLABORADOR"].nunique().reset_index(name="DES")

        serie = adm_s.merge(des_s, left_on="_YM_ADM", right_on="_YM_DES", how="outer").fillna(0)
        serie["YM"] = serie["_YM_ADM"].where(serie["_YM_ADM"] != "", serie["_YM_DES"])
        serie = serie[["YM", "ADM", "DES"]].copy()
        serie["ADM"] = serie["ADM"].astype(int)
        serie["DES"] = serie["DES"].astype(int)
        serie = serie.sort_values("YM")

        # ativos por mês (proxy: usa ativos do mês (se existir YM em cada linha), senão usa o do filtro)
        # para manter simples, usa o total do mês selecionado como denominador.
        denom = max(total_colab, 1)
        serie["TURNOVER_%"] = (((serie["ADM"] + serie["DES"]) / 2) / denom * 100).round(1)

        ch = alt.Chart(serie).mark_line(point=True).encode(
            x=alt.X("YM:N", title="MÊS"),
            y=alt.Y("TURNOVER_%:Q", title="Turnover (%)"),
            tooltip=["YM", "ADM", "DES", alt.Tooltip("TURNOVER_%:Q", format=".1f")]
        ).properties(height=300)
        st.altair_chart(ch, use_container_width=True)
    else:
        st.info("Sem base com DATA_ADMISSAO/DATA_DESLIGAMENTO suficiente para série histórica de Turnover.")

    st.markdown('<div class="section">Absenteísmo</div>', unsafe_allow_html=True)

    if not pr.empty and "DATA" in pr.columns and "STATUS_PRESENCA" in pr.columns:
        tmp = pr.copy()
        tmp["DIA"] = pd.to_datetime(tmp["DATA"], errors="coerce")
        tmp = tmp[tmp["DIA"].notna()]
        tmp["FALTA"] = tmp["STATUS_PRESENCA"].astype(str).map(_upper).str.contains(r"FALTA|AUSEN|AUSÊN|ATEST", regex=True, na=False).astype(int)

        day = tmp.groupby(tmp["DIA"].dt.date)["FALTA"].agg(["sum", "count"]).reset_index()
        day = day.rename(columns={"sum": "FALTAS", "count": "REGISTROS", "DIA": "DATA"})
        day["ABS_%"] = (day["FALTAS"] / day["REGISTROS"] * 100).round(1)

        ch = alt.Chart(day).mark_bar().encode(
            x=alt.X("DATA:N", title="DATA", axis=alt.Axis(labelAngle=0)),
            y=alt.Y("ABS_%:Q", title="Absenteísmo (%)"),
            tooltip=["DATA", "FALTAS", "REGISTROS", alt.Tooltip("ABS_%:Q", format=".1f")]
        ).properties(height=280)
        st.altair_chart(ch, use_container_width=True)

        if not fast_mode:
            st.dataframe(day.sort_values("DATA", ascending=False), use_container_width=True, hide_index=True)
    else:
        st.info("Sem BASE PRESENÇA suficiente para calcular absenteísmo por dia.")


# ---------- TREINAMENTOS ----------
with tab3:
    st.markdown('<div class="section">Treinamentos do mês</div>', unsafe_allow_html=True)

    if tr.empty:
        st.info("Sem dados de TREINAMENTOS no mês/filtros.")
    else:
        # por treinamento
        by_t = tr.groupby("TREINAMENTO")["COLABORADOR"].nunique().reset_index(name="PESSOAS")
        by_t = by_t.sort_values("PESSOAS", ascending=False).head(15)

        c1, c2 = st.columns(2)

        with c1:
            ch = alt.Chart(by_t).mark_bar().encode(
                x=alt.X("TREINAMENTO:N", sort='-y', axis=alt.Axis(labelAngle=0, labelLimit=220), title="TREINAMENTO"),
                y=alt.Y("PESSOAS:Q", title="PESSOAS"),
                tooltip=["TREINAMENTO", "PESSOAS"]
            ).properties(height=320)
            st.subheader("Top treinamentos (até 15)")
            st.altair_chart(ch, use_container_width=True)

        with c2:
            # por unidade
            if "UNIDADE" in tr.columns:
                by_u = tr.groupby("UNIDADE")["COLABORADOR"].nunique().reset_index(name="PESSOAS")
                by_u = by_u.sort_values("PESSOAS", ascending=False)
                ch = alt.Chart(by_u).mark_bar().encode(
                    x=alt.X("UNIDADE:N", sort='-y', axis=alt.Axis(labelAngle=0, labelLimit=220), title="UNIDADE"),
                    y=alt.Y("PESSOAS:Q", title="PESSOAS"),
                    tooltip=["UNIDADE", "PESSOAS"]
                ).properties(height=320)
                st.subheader("Participação por unidade")
                st.altair_chart(ch, use_container_width=True)
            else:
                st.info("Sem UNIDADE em TREINAMENTOS.")

        if not fast_mode:
            st.markdown('<div class="section">Detalhamento (treinamentos)</div>', unsafe_allow_html=True)
            cols_show = [c for c in ["DATA","UNIDADE","COLABORADOR","CARGO","TREINAMENTO","STATUS"] if c in tr.columns]
            st.dataframe(tr[cols_show].sort_values(cols_show[0] if cols_show else tr.columns[0]), use_container_width=True, hide_index=True)


# ---------- DETALHES (base geral/presença/abs) ----------
with tab4:
    st.markdown('<div class="section">Base Geral (recorte)</div>', unsafe_allow_html=True)
    if bg.empty:
        st.info("Sem dados em BASE GERAL no mês/filtros.")
    else:
        cols = [c for c in ["EMPRESA","UNIDADE","COLABORADOR","CARGO","SITUACAO","DATA_ADMISSAO","DATA_DESLIGAMENTO","TEMPO_CASA","YM"] if c in bg.columns]
        st.dataframe(bg[cols].sort_values(["UNIDADE","COLABORADOR"]) if "UNIDADE" in bg.columns else bg[cols], use_container_width=True, hide_index=True)

    if not fast_mode:
        st.markdown('<div class="section">Presença (recorte)</div>', unsafe_allow_html=True)
        if pr.empty:
            st.info("Sem dados em BASE PRESENÇA no mês/filtros.")
        else:
            cols = [c for c in ["DATA","UNIDADE","COLABORADOR","STATUS_PRESENCA","YM"] if c in pr.columns]
            st.dataframe(pr[cols].sort_values(["DATA","UNIDADE","COLABORADOR"]) if "DATA" in pr.columns else pr[cols], use_container_width=True, hide_index=True)

        st.markdown('<div class="section">Absenteísmo/Turnover (recorte)</div>', unsafe_allow_html=True)
        if ab.empty:
            st.info("Sem dados em ABSENTEISMO E TURNOVER no mês/filtros.")
        else:
            cols = [c for c in ["DATA","EMPRESA","UNIDADE","TIPO","QTD","YM"] if c in ab.columns]
            st.dataframe(ab[cols].sort_values(["DATA","TIPO"]) if "DATA" in ab.columns else ab[cols], use_container_width=True, hide_index=True)
