"""
Módulo de banco de dados MySQL (Aiven Cloud) para armazenar OS finalizadas e detalhes.
- ultimaatualizacao: apenas OS com status FINALIZADA e datahorainicio/datahorafim preenchidos.
- detalhesOS: itens de material/valor por OS.
Configuração: no Streamlit Cloud use Secrets (TOML); localmente use variável de ambiente.
"""
import os
from contextlib import contextmanager
from typing import List, Dict, Any

import mysql.connector
from mysql.connector import Error


def _get_database_config() -> Dict[str, Any]:
    """
    Obtém a configuração do banco: Streamlit Cloud usa st.secrets; localmente usa os.environ.
    Retorna um dicionário com host, port, user, password, database, ssl_mode.
    """
    config = {}
    
    try:
        import streamlit as st
        if hasattr(st, "secrets") and "DB_HOST" in st.secrets:
            config = {
                "host": st.secrets["DB_HOST"],
                "port": int(st.secrets.get("DB_PORT", 3306)),
                "user": st.secrets["DB_USER"],
                "password": st.secrets["DB_PASSWORD"],
                "database": st.secrets.get("DB_NAME", "defaultdb"),
                "ssl_disabled": False,  # Aiven exige SSL
                "ssl_verify_cert": True,
                "ssl_verify_identity": True,
            }
    except Exception:
        pass
    
    # Fallback para variáveis de ambiente
    if not config:
        config = {
            "host": os.environ.get("DB_HOST", "mysql-256c83ab-weslei-43d5.i.aivencloud.com"),
            "port": int(os.environ.get("DB_PORT", "12463")),
            "user": os.environ.get("DB_USER", "avnadmin"),
            "password": os.environ.get("DB_PASSWORD", ""),
            "database": os.environ.get("DB_NAME", "defaultdb"),
            "ssl_disabled": False,
            "ssl_verify_cert": True,
            "ssl_verify_identity": True,
        }
    
    return config


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
    """Context manager para conexão com o MySQL."""
    config = _get_database_config()
    conn = None
    try:
        conn = mysql.connector.connect(**config)
        yield conn
        conn.commit()
    except Error as e:
        if conn:
            conn.rollback()
        raise
    finally:
        if conn and conn.is_connected():
            conn.close()


def init_db():
    """Cria as tabelas se não existirem (sintaxe MySQL)."""
    with get_connection() as conn:
        cur = conn.cursor()
        
        # Tabela principal de OS
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ultimaatualizacao (
                numeroos INT PRIMARY KEY,
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
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        # Tabela de detalhes
        cur.execute("""
            CREATE TABLE IF NOT EXISTS detalhesOS (
                id INT AUTO_INCREMENT PRIMARY KEY,
                numeroos INT NOT NULL,
                material TEXT,
                quantidade TEXT,
                valorunit TEXT,
                valortotal TEXT,
                quantidadeestoque TEXT,
                FOREIGN KEY (numeroos) REFERENCES ultimaatualizacao(numeroos) ON DELETE CASCADE,
                INDEX idx_detalhes_numeroos (numeroos)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
        """)
        
        conn.commit()


def _row_to_dict(row, columns) -> Dict[str, Any]:
    """Converte tupla do cursor em dicionário."""
    if row is None:
        return {}
    return dict(zip(columns, row))


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
        
        # MySQL usa INSERT ... ON DUPLICATE KEY UPDATE
        update_parts = [f"{c} = VALUES({c})" for c in OS_COLUMNS if c != "numeroos"]
        update_clause = ", ".join(update_parts)
        
        upsert_sql = f"""
            INSERT INTO ultimaatualizacao ({cols}) 
            VALUES ({placeholders})
            ON DUPLICATE KEY UPDATE {update_clause}
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
        cur = conn.cursor()
        cur.execute("SELECT * FROM ultimaatualizacao ORDER BY numeroos")
        columns = [desc[0] for desc in cur.description]
        return [_row_to_dict(r, columns) for r in cur.fetchall()]


def buscar_detalhes_para_dashboard() -> List[Dict[str, Any]]:
    """Retorna todos os registros de detalhesOS para uso no dashboard."""
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT numeroos, material, quantidade, valorunit, valortotal, quantidadeestoque
            FROM detalhesOS ORDER BY numeroos, id
        """)
        columns = [desc[0] for desc in cur.description]
        return [_row_to_dict(r, columns) for r in cur.fetchall()]
