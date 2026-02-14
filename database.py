"""
Módulo de banco de dados PostgreSQL (Supabase) para armazenar OS finalizadas e detalhes.
- ultimaatualizacao: apenas OS com status FINALIZADA e datahorainicio/datahorafim preenchidos.
- detalhesOS: itens de material/valor por OS.
Configuração: no Streamlit Cloud use Secrets (TOML) com DATABASE_URL; localmente use variável de ambiente.
"""
import os
from contextlib import contextmanager
from typing import List, Dict, Any

import psycopg2
from psycopg2.extras import RealDictCursor

_DEFAULT_URL = "postgresql://postgres:[YOUR-PASSWORD]@db.rvxnkscptvpxobzvvole.supabase.co:5432/postgres"


def _get_database_url() -> str:
    """Obtém a connection string: Streamlit Cloud usa st.secrets; localmente usa os.environ."""
    url = None
    try:
        import streamlit as st
        if hasattr(st, "secrets") and st.secrets and st.secrets.get("DATABASE_URL"):
            url = st.secrets["DATABASE_URL"]
    except Exception:
        pass
    if not url:
        url = os.environ.get("DATABASE_URL", _DEFAULT_URL)
    # Supabase exige SSL em conexões externas
    if url and "supabase.co" in url and "sslmode=" not in url:
        url = url + ("&" if "?" in url else "?") + "sslmode=require"
    return url

# Campos da API last-update (registro de OS)
OS_COLUMNS = [
    "numeroos", "datahoraos", "datahorainicio", "datahorafim",
    "placaequipamento", "marcaequipamento", "modeloequipamento", "hodometro",
    "titulomanutencao", "tipomanutencao", "status", "motoristaresponsavel",
    "mecanicoresponsavel", "descricaoos", "fornecedor", "lastupdate"
]

# Campos da API os-details
DETALHES_COLUMNS = ["numeroos", "material", "quantidade", "valorunit", "valortotal", "quantidadeestoque"]


@contextmanager
def get_connection():
    """Context manager para conexão com o PostgreSQL."""
    conn = psycopg2.connect(_get_database_url())
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Cria as tabelas se não existirem (sintaxe PostgreSQL)."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ultimaatualizacao (
                numeroos INTEGER PRIMARY KEY,
                datahoraos TEXT,
                datahorainicio TEXT,
                datahorafim TEXT,
                placaequipamento TEXT,
                marcaequipamento TEXT,
                modeloequipamento TEXT,
                hodometro TEXT,
                titulomanutencao TEXT,
                tipomanutencao TEXT,
                status TEXT,
                motoristaresponsavel TEXT,
                mecanicoresponsavel TEXT,
                descricaoos TEXT,
                fornecedor TEXT,
                lastupdate TEXT
            )
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS detalhesOS (
                id SERIAL PRIMARY KEY,
                numeroos INTEGER NOT NULL REFERENCES ultimaatualizacao(numeroos),
                material TEXT,
                quantidade TEXT,
                valorunit TEXT,
                valortotal TEXT,
                quantidadeestoque TEXT
            )
        """)
        cur.execute("CREATE INDEX IF NOT EXISTS idx_detalhes_numeroos ON detalhesOS(numeroos)")
        conn.commit()


def _row_to_dict(row) -> Dict[str, Any]:
    """Converte linha do cursor (RealDictRow ou tuple) em dict."""
    if row is None:
        return {}
    if hasattr(row, "keys"):
        return dict(row)
    return {}


def os_atende_criterios(item: Dict[str, Any]) -> bool:
    """
    Verifica se a OS deve ser gravada: status FINALIZADA e datahorainicio e datahorafim preenchidos.
    """
    status = (item.get("status") or "").strip().upper()
    di = item.get("datahorainicio")
    df = item.get("datahorafim")
    return (
        status == "FINALIZADA"
        and di is not None
        and str(di).strip() != ""
        and df is not None
        and str(df).strip() != ""
    )


def inserir_os_lote(itens: List[Dict[str, Any]]) -> int:
    """
    Insere ou atualiza OS na tabela ultimaatualizacao.
    Espera apenas itens que já atendam aos critérios (FINALIZADA + datas).
    Retorna quantidade de registros inseridos/atualizados.
    """
    if not itens:
        return 0
    with get_connection() as conn:
        cur = conn.cursor()
        placeholders = ", ".join(["%s"] * len(OS_COLUMNS))
        cols = ", ".join(OS_COLUMNS)
        upsert_sql = f"""
            INSERT INTO ultimaatualizacao ({cols}) VALUES ({placeholders})
            ON CONFLICT(numeroos) DO UPDATE SET
                datahoraos = EXCLUDED.datahoraos,
                datahorainicio = EXCLUDED.datahorainicio,
                datahorafim = EXCLUDED.datahorafim,
                placaequipamento = EXCLUDED.placaequipamento,
                marcaequipamento = EXCLUDED.marcaequipamento,
                modeloequipamento = EXCLUDED.modeloequipamento,
                hodometro = EXCLUDED.hodometro,
                titulomanutencao = EXCLUDED.titulomanutencao,
                tipomanutencao = EXCLUDED.tipomanutencao,
                status = EXCLUDED.status,
                motoristaresponsavel = EXCLUDED.motoristaresponsavel,
                mecanicoresponsavel = EXCLUDED.mecanicoresponsavel,
                descricaoos = EXCLUDED.descricaoos,
                fornecedor = EXCLUDED.fornecedor,
                lastupdate = EXCLUDED.lastupdate
        """
        count = 0
        for item in itens:
            row = [item.get(c) for c in OS_COLUMNS]
            cur.execute(upsert_sql, row)
            count += 1
        return count


def inserir_detalhes_os(numeroos: int, itens: List[Dict[str, Any]]) -> int:
    """
    Insere os detalhes de uma OS na tabela detalhesOS.
    Remove detalhes antigos dessa OS antes de inserir (replace).
    Retorna quantidade de linhas inseridas.
    """
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("DELETE FROM detalhesOS WHERE numeroos = %s", (numeroos,))
        if not itens:
            return 0
        insert_sql = """
            INSERT INTO detalhesOS (numeroos, material, quantidade, valorunit, valortotal, quantidadeestoque)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        count = 0
        for item in itens:
            cur.execute(insert_sql, (
                numeroos,
                item.get("material"),
                item.get("quantidade"),
                item.get("valorunit"),
                item.get("valortotal"),
                item.get("quantidadeestoque"),
            ))
            count += 1
        return count


def listar_numeroos_com_detalhes() -> List[int]:
    """Retorna lista de numeroos que já possuem registros em detalhesOS."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT DISTINCT numeroos FROM detalhesOS")
        return [r[0] for r in cur.fetchall()]


def os_precisa_detalhes(numeroos: int) -> bool:
    """Retorna True se esta OS está em ultimaatualizacao e ainda não tem detalhes."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM ultimaatualizacao WHERE numeroos = %s", (numeroos,))
        if not cur.fetchone():
            return False
        cur.execute("SELECT 1 FROM detalhesOS WHERE numeroos = %s LIMIT 1", (numeroos,))
        return cur.fetchone() is None


def listar_os_sem_detalhes() -> List[int]:
    """Retorna numeroos que estão em ultimaatualizacao e ainda não têm detalhes."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT u.numeroos FROM ultimaatualizacao u
            LEFT JOIN detalhesOS d ON u.numeroos = d.numeroos
            WHERE d.numeroos IS NULL
        """)
        return [r[0] for r in cur.fetchall()]


def listar_todas_os_ultimaatualizacao() -> List[int]:
    """Retorna todos os numeroos da tabela ultimaatualizacao."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("SELECT numeroos FROM ultimaatualizacao ORDER BY numeroos")
        return [r[0] for r in cur.fetchall()]


def buscar_os_para_dashboard() -> List[Dict[str, Any]]:
    """Retorna todos os registros de ultimaatualizacao para uso no dashboard."""
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("SELECT * FROM ultimaatualizacao ORDER BY numeroos")
        return [_row_to_dict(r) for r in cur.fetchall()]


def buscar_detalhes_para_dashboard() -> List[Dict[str, Any]]:
    """Retorna todos os registros de detalhesOS para uso no dashboard."""
    with get_connection() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)
        cur.execute("""
            SELECT numeroos, material, quantidade, valorunit, valortotal, quantidadeestoque
            FROM detalhesOS ORDER BY numeroos, id
        """)
        return [_row_to_dict(r) for r in cur.fetchall()]
