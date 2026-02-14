import streamlit as st
import pandas as pd
import json
import altair as alt
import requests
import os
import time
import threading
from streamlit.runtime.scriptrunner import add_script_run_ctx

import database

# --- Configuração Inicial da Página e Estado da Sessão ---
st.set_page_config(layout="wide")

CONFIG_FILE = "config.json"
LOGO_URL = "https://github.com/WRSouza93/dashboard-manutencao/blob/main/Translek.png?raw=true"

# MAPEAMENTO DE MESES EM PORTUGUÊS
MONTHS_PT = {
    1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 
    5: "Mai", 6: "Jun", 7: "Jul", 8: "Ago",
    9: "Set", 10: "Out", 11: "Nov", 12: "Dez"
}

# --- Funções de Gerenciamento de Configuração ---
def load_config():
    """Carrega as configurações do arquivo JSON se ele existir."""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
        # Garante que os novos campos existam
        if 'interval_dashboard' not in config:
            config['interval_dashboard'] = config.get('interval', 5)
        if 'interval_andamento' not in config:
            config['interval_andamento'] = 5
        # Remove campo antigo se existir
        if 'interval' in config:
            del config['interval']
        return config
    return {
        'login': '',
        'password': '',
        'interval_dashboard': 5,
        'interval_andamento': 5
    }

def save_config():
    """Salva as configurações atuais no arquivo JSON."""
    with open(CONFIG_FILE, 'w') as f:
        json.dump(st.session_state.config, f, indent=4)
    st.success("Configurações salvas com sucesso!")

# --- Inicialização do Estado da Sessão ---
if 'config' not in st.session_state:
    st.session_state.config = load_config()
if 'last_update' not in st.session_state:
    st.session_state.last_update = "Nenhuma atualização automática ainda."
if 'scheduler_running' not in st.session_state:
    st.session_state.scheduler_running = False
if 'scheduler_thread' not in st.session_state:
    st.session_state.scheduler_thread = None
if 'update_log' not in st.session_state:
    st.session_state.update_log = "Aguardando início do agendador."
if 'next_update_time' not in st.session_state:
    st.session_state.next_update_time = None
if 'api_data' not in st.session_state:
    st.session_state.api_data = None
if 'api_details' not in st.session_state:
    st.session_state.api_details = None
if 'auto_init_done' not in st.session_state:
    st.session_state.auto_init_done = False

# Inicializa o banco de dados (tabelas ultimaatualizacao e detalhesOS)
database.init_db()

# --- Funções de Lógica de Negócio (API e Dados) ---
def _get_token(login, password, log_callback):
    """Obtém o token de autenticação da API."""
    try:
        auth_url = "https://yjlcmonbid.execute-api.us-east-1.amazonaws.com/auth/V1"
        auth_payload = {"login": login, "password": password}
        auth_response = requests.post(auth_url, json=auth_payload, timeout=10)
        auth_response.raise_for_status()
        auth_data = auth_response.json()
        token = auth_data.get("token")
        if not token:
            log_callback("Erro de autenticação: Token não encontrado na resposta.")
            return None
        return token
    except requests.exceptions.RequestException as e:
        log_callback(f"Erro de autenticação: {e}")
        return None

# NOVA FUNÇÃO: Busca apenas histórico (para página OS em Andamento)
def fetch_historico_only(config, log_callback):
    """Busca apenas os dados de histórico da API (sem detalhes)."""
    login, password = config.get('login'), config.get('password')

    if not all([login, password]):
        log_callback("Erro: Login e senha devem estar configurados no config.json.")
        return False

    log_callback("Iniciando atualização do histórico...")
    token = _get_token(login, password, log_callback)
    if not token: 
        return False

    try:
        log_callback("Carregando histórico...")
        data_url = "https://yjlcmonbid.execute-api.us-east-1.amazonaws.com/os/V1/find/last-update/2020-01-01"
        headers = {"Authorization": token}
        data_response = requests.get(data_url, headers=headers, timeout=60)
        data_response.raise_for_status()
        historico_data = data_response.json()
        
        # Armazena dados no session_state
        st.session_state.api_data = historico_data
        log_callback("Histórico carregado com sucesso!")
        return True
    except Exception as e:
        log_callback(f"Erro ao buscar histórico: {e}")
        return False

def fetch_api_data_online(config, log_callback):
    """Busca os dados da API e armazena no session_state (histórico + detalhes)."""
    login, password = config.get('login'), config.get('password')
    st.session_state.next_update_time = None # Reseta o contador no início da atualização

    if not all([login, password]):
        log_callback("Erro: Login e senha devem estar configurados no config.json.")
        return False

    log_callback("Iniciando atualização... Obtendo token...")
    token = _get_token(login, password, log_callback)
    if not token: 
        return False

    try:
        log_callback("Carregando histórico...")
        data_url = "https://yjlcmonbid.execute-api.us-east-1.amazonaws.com/os/V1/find/last-update/2020-01-01"
        headers = {"Authorization": token}
        data_response = requests.get(data_url, headers=headers, timeout=60)
        data_response.raise_for_status()
        historico_data = data_response.json()
        st.session_state.api_data = historico_data
        log_callback("Histórico carregado com sucesso.")
    except Exception as e:
        log_callback(f"Erro ao buscar histórico: {e}")
        return False

    os_list = historico_data.get("data", [])
    # Primeiro loop: separa quem atende ao critério (grava no banco) e quem não atende (guarda na memória)
    os_atendem = []
    os_nao_atendem = []
    for item in os_list:
        if database.os_atende_criterios(item):
            os_atendem.append(item)
        else:
            os_nao_atendem.append(item)

    # Grava em ultimaatualizacao só os que atendem; tabela já fica carregada
    try:
        n_inseridas = database.inserir_os_lote(os_atendem)
        log_callback(f"Banco: {n_inseridas} OS (FINALIZADA com datas) gravadas em ultimaatualizacao. Tabela carregada.")
    except Exception as e:
        log_callback(f"Erro ao gravar OS no banco: {e}")

    # Consulta API os-details somente para os que NÃO atendem aos critérios; contagem só destes
    all_details_nao_atendem = []
    total_nao_atendem = len(os_nao_atendem)
    if total_nao_atendem > 0:
        log_callback(f"Buscando detalhes apenas para {total_nao_atendem} OS que não atendem ao critério...")
        try:
            for i, os_item in enumerate(os_nao_atendem):
                if (i + 1) % 20 == 0 or (i + 1) == total_nao_atendem:
                    log_callback(f"Detalhes (fora do critério): {i+1} de {total_nao_atendem} OS")
                numeroos = os_item.get("numeroos")
                if numeroos:
                    details_url = f"https://yjlcmonbid.execute-api.us-east-1.amazonaws.com/os/V1/find/os-details/{numeroos}"
                    response = requests.get(details_url, headers=headers, timeout=15)
                    if response.status_code == 200:
                        resp_json = response.json()
                        if resp_json.get("status") and resp_json.get("data"):
                            all_details_nao_atendem.append(resp_json)
                    time.sleep(0.05)
        except Exception as e:
            log_callback(f"Erro ao buscar detalhes: {e}")
            return False
    else:
        log_callback("Nenhuma OS fora do critério para consultar detalhes.")

    # Session: detalhes só das OS que não atendem (as que atendem ficam no banco/detalhesOS)
    st.session_state.api_details = all_details_nao_atendem
    log_callback(f"Atualização concluída. {len(all_details_nao_atendem)} detalhes carregados (OS fora do critério).")
    st.session_state.last_update = time.strftime('%d/%m/%Y %H:%M:%S')
    interval_seconds = config.get('interval_dashboard', 5) * 60
    st.session_state.next_update_time = time.time() + interval_seconds
    return True

def scheduler_log_callback(message):
    st.session_state.update_log = message

def scheduler_loop():
    """Loop que executa a atualização de dados em intervalos definidos."""
    while st.session_state.get('scheduler_running', False):
        fetch_api_data_online(st.session_state.config, scheduler_log_callback)
        st.cache_data.clear()
        
        # Usa intervalo do dashboard para o agendador
        interval_seconds = st.session_state.config.get('interval_dashboard', 5) * 60
        for _ in range(interval_seconds):
            if not st.session_state.get('scheduler_running', False): break
            time.sleep(1)

@st.cache_data
def load_data_from_session():
    """Carrega os dados do session_state (ou do banco) e os processa (histórico + detalhes)."""
    # Preferência: session_state (histórico completo); detalhes = sessão (OS que não atendem) + banco (OS que atendem)
    if st.session_state.api_data:
        data_list = st.session_state.api_data["data"]
        detalhes_sessao = [item for entry in (st.session_state.api_details or []) if entry.get("data") for item in entry["data"]]
        try:
            detalhes_banco = database.buscar_detalhes_para_dashboard()
        except Exception:
            detalhes_banco = []
        all_detalhes = detalhes_sessao + (detalhes_banco or [])
    else:
        try:
            data_list = database.buscar_os_para_dashboard()
            all_detalhes = database.buscar_detalhes_para_dashboard() or []
        except Exception:
            return None, None
        if not data_list:
            return None, None

    df_historico = pd.DataFrame(data_list)
    
    # Processa dados dos detalhes (já é lista plana de itens)
    df_detalhes = pd.DataFrame(all_detalhes) if all_detalhes else pd.DataFrame()
    
    # Processamento dos dados
    df_historico['numeroos'] = df_historico['numeroos'].astype(int)
    if not df_detalhes.empty:
        df_detalhes.dropna(subset=['numeroos'], inplace=True)
        df_detalhes['numeroos'] = df_detalhes['numeroos'].astype(int)
        for col in ['quantidade', 'valorunit', 'valortotal']:
            if col in df_detalhes.columns:
                df_detalhes[col] = pd.to_numeric(df_detalhes[col], errors='coerce')
        df_detalhes.fillna(0, inplace=True)
        detalhes_agg = df_detalhes.groupby('numeroos').agg(valortotal=('valortotal', 'sum')).reset_index()
    else:
        detalhes_agg = pd.DataFrame(columns=['numeroos', 'valortotal'])
    df_merged = pd.merge(df_historico, detalhes_agg, on='numeroos', how='left')
    df_merged['valortotal'].fillna(0, inplace=True)
    for col in ['datahoraos', 'datahorainicio', 'datahorafim']:
        df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce')
    return df_merged, df_detalhes

# NOVA FUNÇÃO: Carrega apenas dados do histórico (para página OS em Andamento)
@st.cache_data
def load_historico_only():
    """Carrega os dados do histórico do session_state ou do banco (fallback)."""
    if st.session_state.api_data:
        data_list = st.session_state.api_data["data"]
    else:
        try:
            data_list = database.buscar_os_para_dashboard()
        except Exception:
            return None
        if not data_list:
            return None
    df_historico = pd.DataFrame(data_list)
    df_historico['numeroos'] = df_historico['numeroos'].astype(int)
    for col in ['datahoraos', 'datahorainicio', 'datahorafim']:
        df_historico[col] = pd.to_datetime(df_historico[col], errors='coerce')
    return df_historico

def classify_os_status(row):
    is_valorizado = row.get('valortotal', 0) > 0
    status_str = str(row.get('status', '')).strip().upper()
    is_finalizada = pd.notna(row['datahorafim']) and status_str == 'FINALIZADA'
    if is_valorizado and is_finalizada: return "VALORIZADO E FINALIZADO"
    if pd.notna(row['datahorainicio']) and pd.isna(row['datahorafim']): return "ANDAMENTO"
    if is_valorizado and pd.isna(row['datahorafim']): return "EXECUTADO"
    if is_finalizada: return "FINALIZADA"
    if pd.isna(row['datahorainicio']) and pd.isna(row['datahorafim']): return "EM BRANCO"
    return "OUTRO"

def apply_filters(df, anos_selecionados, meses_selecionados, os_selecionadas, marca_selecionada, 
                 placa_selecionada_filtro, tipo_manutencao_selecionado, situacao_selecionada, 
                 motorista_selecionado):
    """Aplica filtros ao DataFrame."""
    df_filtered = df.copy()
    
    # APLICAR FILTRO DE ANO (multiselect)
    if anos_selecionados and 'Todos' not in anos_selecionados:
        anos_numeros = [int(ano) for ano in anos_selecionados]
        df_filtered = df_filtered[df_filtered['datahoraos'].dt.year.isin(anos_numeros)]
    
    # APLICAR FILTRO DE MÊS (multiselect)
    if meses_selecionados and 'Todos' not in meses_selecionados:
        meses_numeros = [k for k, v in MONTHS_PT.items() if v in meses_selecionados]
        df_filtered = df_filtered[df_filtered['datahoraos'].dt.month.isin(meses_numeros)]
    
    # APLICAR OUTROS FILTROS
    if os_selecionadas: 
        df_filtered = df_filtered[df_filtered['numeroos'].isin(os_selecionadas)]
    if marca_selecionada: 
        df_filtered = df_filtered[df_filtered['marcaequipamento'].isin(marca_selecionada)]
    if placa_selecionada_filtro: 
        df_filtered = df_filtered[df_filtered['placaequipamento'].isin(placa_selecionada_filtro)]
    if tipo_manutencao_selecionado: 
        df_filtered = df_filtered[df_filtered['titulomanutencao'].isin(tipo_manutencao_selecionado)]
    if situacao_selecionada: 
        df_filtered = df_filtered[df_filtered['Situação da OS'].isin(situacao_selecionada)]
    if motorista_selecionado: 
        df_filtered = df_filtered[df_filtered['motoristaresponsavel'].isin(motorista_selecionado)]
    
    return df_filtered

# --- Funções de Renderização de Página ---
def render_dashboard_page():
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title("DASHBOARD DE MANUTENÇÃO TRANSLEK")
    with col2:
        st.image(LOGO_URL, width=200)

    st.sidebar.header("Filtros")
    col1_sidebar, col2_sidebar = st.sidebar.columns(2)
    with col1_sidebar:
        if st.button("Atualizar Dados"):
            if not st.session_state.config.get('login') or not st.session_state.config.get('password'):
                st.error("Configure login e senha no arquivo config.json")
                return
            log_placeholder = st.empty()
            with st.spinner("Atualizando..."):
                success = fetch_api_data_online(st.session_state.config, log_callback=log_placeholder.info)
                if success:
                    st.cache_data.clear()
                    log_placeholder.empty()
                    st.success("Dados atualizados com sucesso!")
                    st.rerun()

    with col2_sidebar:
        if st.button("Limpar Filtros"):
            keys_to_keep = ['config', 'scheduler_running', 'scheduler_thread', 'last_update', 'update_log', 'next_update_time', 'api_data', 'api_details', 'auto_init_done']
            for key in list(st.session_state.keys()):
                if key not in keys_to_keep: del st.session_state[key]
            st.rerun()

    # Carrega dados (session_state ou banco); só exibe aviso se não houver nenhum dado
    try:
        df, df_detalhes = load_data_from_session()
        if df is None:
            st.warning("Nenhum dado carregado. Clique em 'Atualizar Dados' para buscar informações da API ou aguarde o agendador.")
            return
    except Exception:
        st.warning("Nenhum dado carregado. Clique em 'Atualizar Dados' para buscar informações da API.")
        return

    try:
            
        df['Situação da OS'] = df.apply(classify_os_status, axis=1)
        
        # FILTROS NA SIDEBAR (MULTISELECT)
        anos = ['Todos'] + sorted(df['datahoraos'].dt.year.dropna().unique().astype(int), reverse=True)
        anos_selecionados = st.sidebar.multiselect('Período (Ano)', anos, default=['Todos'])
        
        # FILTRO DE MÊS EM PORTUGUÊS (MULTISELECT)
        meses_disponveis = sorted(df['datahoraos'].dt.month.dropna().unique().astype(int))
        meses_opcoes = ['Todos'] + [MONTHS_PT[mes] for mes in meses_disponveis]
        meses_selecionados = st.sidebar.multiselect('Mês', meses_opcoes, default=['Todos'])
        
        os_list = sorted(df['numeroos'].dropna().unique().astype(int))
        os_selecionadas = st.sidebar.multiselect('Pesquisar OS', os_list)
        marcas = sorted(df['marcaequipamento'].dropna().unique())
        marca_selecionada = st.sidebar.multiselect('Marca', marcas)
        placas = sorted(df['placaequipamento'].dropna().unique())
        placa_selecionada_filtro = st.sidebar.multiselect('Placa', placas)
        tipos_manutencao = sorted(df['titulomanutencao'].dropna().unique())
        tipo_manutencao_selecionado = st.sidebar.multiselect('Tipo Manutenção', tipos_manutencao)
        situacoes = sorted(df['Situação da OS'].dropna().unique())
        situacao_selecionada = st.sidebar.multiselect('Situação', situacoes)
        motoristas = sorted(df['motoristaresponsavel'].dropna().unique())
        motorista_selecionado = st.sidebar.multiselect('Motorista', motoristas)

        # APLICAR FILTROS
        df_filtered = apply_filters(df, anos_selecionados, meses_selecionados, os_selecionadas, 
                                  marca_selecionada, placa_selecionada_filtro, tipo_manutencao_selecionado, 
                                  situacao_selecionada, motorista_selecionado)
        
        total_os, os_finalizadas, os_sem_valorizacao, custo_total, custo_medio, veiculos_atendidos, tempo_medio_dias = (
            df_filtered['numeroos'].nunique(),
            df_filtered[(df_filtered['datahorafim'].notna()) & (df_filtered['status'].fillna('').str.strip().str.upper() == 'FINALIZADA')]['numeroos'].nunique(),
            df_filtered[df_filtered['valortotal'] == 0]['numeroos'].nunique(),
            df_filtered['valortotal'].sum(),
            df_filtered[df_filtered['valortotal'] > 0]['valortotal'].mean() if not df_filtered[df_filtered['valortotal'] > 0].empty else 0,
            df_filtered['placaequipamento'].nunique(),
            int(((df_filtered.dropna(subset=['datahorainicio', 'datahorafim'])['datahorafim'] - df_filtered.dropna(subset=['datahorainicio', 'datahorafim'])['datahorainicio']).dt.total_seconds() / (24*3600)).mean()) if not df_filtered.dropna(subset=['datahorainicio', 'datahorafim']).empty else 0
        )
        
        # CSS para ajustar tamanho dos cards
        st.markdown("""
        <style>
        div[data-testid="stMetricValue"] {
            font-size: 18px !important;
        }
        div[data-testid="stMetricLabel"] {
            font-size: 12px !important;
            font-weight: 600 !important;
        }
        </style>
        """, unsafe_allow_html=True)
        
        col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
        col1.metric("ORDENS DE SERVIÇO", f"{total_os}")
        col2.metric("OS FINALIZADAS", f"{os_finalizadas}")
        col3.metric("OS SEM VALORIZAÇÃO", f"{os_sem_valorizacao}")
        col4.metric("CUSTO TOTAL", f"R$ {custo_total:,.2f}")
        col5.metric("CUSTO MÉDIO", f"R$ {custo_medio:,.2f}")
        col6.metric("VEÍCULOS ATENDIDOS", f"{veiculos_atendidos}")
        col7.metric("DIAS DE ATENDIMENTO", f"{tempo_medio_dias}")

        chart_col1, chart_col2 = st.columns(2)
        with chart_col1:
            st.header("REGISTRO DE OS")
            df_charting = df_filtered.dropna(subset=['datahoraos']).copy()
            if not df_charting.empty:
                df_charting['Mês'] = df_charting['datahoraos'].dt.to_period('M').astype(str)
                os_finalizadas_charting = df_charting[(df_charting['datahorafim'].notna()) & (df_charting['status'].fillna('').str.strip().str.upper() == 'FINALIZADA')]
                os_geradas_mes = df_charting.groupby('Mês')['numeroos'].nunique()
                os_finalizadas_mes = os_finalizadas_charting.groupby('Mês')['numeroos'].nunique()
                os_valorizada_mes = df_charting[df_charting['valortotal'] > 0].groupby('Mês')['numeroos'].nunique()
                chart_df = pd.DataFrame({'OS Geradas': os_geradas_mes, 'OS Finalizadas': os_finalizadas_mes, 'OS Valorizada': os_valorizada_mes}).fillna(0).astype(int)
                chart_df['OS Andamento'] = chart_df['OS Geradas'] - chart_df['OS Finalizadas']
                chart_df = chart_df[['OS Geradas', 'OS Andamento', 'OS Finalizadas', 'OS Valorizada']]
                chart_df_long = chart_df.reset_index().melt('Mês', var_name='Status', value_name='Quantidade')
                bars = alt.Chart(chart_df_long).mark_bar().encode(x=alt.X('Mês:N', sort=None, title='Mês'), y=alt.Y('Quantidade:Q', title='Quantidade de OS'), color=alt.Color('Status:N', title='Status da OS'), tooltip=['Mês', 'Status', 'Quantidade'], xOffset='Status:N').properties(width=alt.Step(20))
                text = bars.mark_text(align='center', baseline='bottom', dy=-5).encode(text='Quantidade:Q')
                st.altair_chart((bars + text), use_container_width=True)
            else: 
                st.info("Nenhum dado para exibir no gráfico de Registro de OS com os filtros selecionados.")
                
        with chart_col2:
            st.header("SITUAÇÃO DA OS")
            situacao_counts = df_filtered['Situação da OS'].value_counts().reset_index()
            situacao_counts.columns = ['Situação', 'Quantidade']
            donut_chart = alt.Chart(situacao_counts).mark_arc(innerRadius=100).encode(theta=alt.Theta(field="Quantidade", type="quantitative"), color=alt.Color(field="Situação", type="nominal", title="Situação"), tooltip=['Situação', 'Quantidade']).properties(title='Distribuição das OS por Situação')
            st.altair_chart(donut_chart, use_container_width=True)

        st.divider()
        st.header("Contagem de OS por Categoria")
        cat_col1, cat_col2 = st.columns(2)
        with cat_col1:
            st.subheader("POR TIPO DE MANUTENÇÃO")
            manutencao_counts = df_filtered.fillna({'titulomanutencao': 'Não Informado'}).groupby('titulomanutencao')['numeroos'].nunique().sort_values(ascending=True).reset_index()
            manutencao_counts.columns = ['Tipo de Manutenção', 'Quantidade']
            chart = alt.Chart(manutencao_counts).mark_bar().encode(x=alt.X('Quantidade:Q', title='Quantidade de OS'), y=alt.Y('Tipo de Manutenção:N', sort='-x', title='Tipo de Manutenção'))
            st.altair_chart(chart, use_container_width=True)
            
        with cat_col2:
            st.subheader("POR MARCA DO CAMINHÃO")
            marca_counts = df_filtered.fillna({'marcaequipamento': 'Não Informada'}).groupby('marcaequipamento')['numeroos'].nunique().sort_values(ascending=True).reset_index()
            marca_counts.columns = ['Marca', 'Quantidade']
            chart = alt.Chart(marca_counts).mark_bar().encode(x=alt.X('Quantidade:Q', title='Quantidade de OS'), y=alt.Y('Marca:N', sort='-x', title='Marca'))
            st.altair_chart(chart, use_container_width=True)
            
        st.subheader("CONTAGEM DE OS POR PLACA")
        placa_counts = df_filtered.fillna({'placaequipamento': 'Não Informada'}).groupby('placaequipamento')['numeroos'].nunique().sort_values(ascending=False).reset_index()
        placa_counts.columns = ['Placa', 'Quantidade']
        chart = alt.Chart(placa_counts).mark_bar().encode(x=alt.X('Placa:N', sort='-y', title='Placa do Equipamento'), y=alt.Y('Quantidade:Q', title='Quantidade de OS'))
        st.altair_chart(chart, use_container_width=True)
        
        st.divider()
        st.header("ANÁLISE DETALHADA POR VEÍCULO")
        col_filtro1, col_filtro2 = st.columns([2, 2])
        with col_filtro1:
            placas_com_custo = sorted(df_filtered[df_filtered['valortotal'] > 0]['placaequipamento'].unique())
            if placas_com_custo:
                placa_selecionada = st.selectbox('Selecione uma placa:', placas_com_custo, key="detalhe_placa")
            else:
                st.info("Nenhuma placa com dados de custo disponível.")
                placa_selecionada = None
                
        with col_filtro2:
            valorizacao_filtro = st.radio("Filtrar por valorização:", ('Todas', 'OS Valorizada', 'OS Não Valorizada'), horizontal=True)

        if placas_com_custo and placa_selecionada:
            df_placa = df_filtered[df_filtered['placaequipamento'] == placa_selecionada].copy()
            df_placa_filtrada = df_placa.copy()
            if valorizacao_filtro == 'OS Valorizada':
                df_placa_filtrada = df_placa[df_placa['valortotal'] > 0]
            elif valorizacao_filtro == 'OS Não Valorizada':
                df_placa_filtrada = df_placa[df_placa['valortotal'] == 0]

            os_da_placa = df_placa_filtrada['numeroos'].unique()
            df_detalhes_placa = df_detalhes[df_detalhes['numeroos'].isin(os_da_placa)]
            col_esq, col_dir = st.columns([1, 2])
            with col_esq:
                st.subheader(f"Placa: {placa_selecionada}")
                os_abertas, os_executadas, valor_total_servicos, valor_medio, tempo_medio_dias_placa, qtd_motoristas = (
                    df_placa_filtrada['numeroos'].nunique(),
                    df_placa_filtrada[df_placa_filtrada['valortotal'] > 0]['numeroos'].nunique(),
                    df_placa_filtrada['valortotal'].sum(),
                    df_placa_filtrada[df_placa_filtrada['valortotal'] > 0]['valortotal'].mean() if not df_placa_filtrada[df_placa_filtrada['valortotal'] > 0].empty else 0,
                    int(((df_placa_filtrada.dropna(subset=['datahorainicio', 'datahorafim'])['datahorafim'] - df_placa_filtrada.dropna(subset=['datahorainicio', 'datahorafim'])['datahorainicio']).dt.total_seconds() / (24 * 3600)).mean()) if not df_placa_filtrada.dropna(subset=['datahorainicio', 'datahorafim']).empty else 0,
                    df_placa_filtrada['motoristaresponsavel'].nunique()
                )
                st.metric("Ordens de Serviço Abertas", os_abertas)
                st.metric("Ordens de Serviço Executadas", os_executadas)
                st.metric("Valor Total de Serviços", f"R$ {valor_total_servicos:,.2f}")
                st.metric("Valor Médio de Manutenções", f"R$ {valor_medio:,.2f}")
                st.metric("Tempo Médio por OS (dias)", f"{tempo_medio_dias_placa}")
                st.metric("Quantidade Motoristas", qtd_motoristas)
            
            with col_dir:
                st.subheader("DETALHES DAS ORDENS DE SERVIÇO")
                for numero_os in sorted(os_da_placa, reverse=True):
                    grupo_detalhes = df_detalhes_placa[df_detalhes_placa['numeroos'] == numero_os]
                    info_os = df_placa_filtrada[df_placa_filtrada['numeroos'] == numero_os].iloc[0]
                    descricao = info_os['descricaoos']
                    total_os_valor = info_os['valortotal']
                    
                    with st.expander(f"OS: {numero_os} - Total: R$ {total_os_valor:,.2f}"):
                        if pd.notna(descricao) and descricao.strip():
                            st.markdown(f"**Descrição:** {descricao}")
                        else:
                            st.markdown("**Descrição:** Nenhuma descrição fornecida.")
                        
                        grupo_filtrado_materiais = grupo_detalhes.dropna(subset=['material'])
                        if not grupo_filtrado_materiais.empty:
                            st.dataframe(grupo_filtrado_materiais[['material', 'quantidade', 'valortotal']].rename(columns={'material': 'Material', 'quantidade': 'Quantidade', 'valortotal': 'Valor'}), hide_index=True, use_container_width=True, column_config={"Valor": st.column_config.NumberColumn(format="R$ %.2f")})
                        else:
                            st.write("Nenhum material registrado para esta OS.")

            st.subheader("CUSTOS DE MANUTENÇÃO POR MOTORISTA")
            custo_por_motorista = df_placa_filtrada.groupby('motoristaresponsavel').agg(valor_total=('valortotal', 'sum'), qtd_os=('numeroos', 'nunique')).reset_index().sort_values(by='valor_total', ascending=False)
            custo_por_motorista.columns = ['Motorista', 'Valor Total de Serviços', 'Qtd. OS Abertas']
            st.dataframe(custo_por_motorista, hide_index=True, use_container_width=True, column_config={"Valor Total de Serviços": st.column_config.NumberColumn(format="R$ %.2f")})
        
        st.divider()
        st.header("ORDENS DE SERVIÇO POR MOTORISTA E PLACA")
        
        # Preparar dados para tabela detalhada
        df_motorista_detalhado = df_filtered.fillna({
            'motoristaresponsavel': 'Não Informado', 
            'placaequipamento': 'Não Informada',
            'marcaequipamento': 'Não Informada',
            'titulomanutencao': 'Não Informado',
            'mecanicoresponsavel': 'Não Informado',
            'tipomanutencao': 'Não Informado',
            'descricaoos': 'Sem descrição'
        }).copy()
        
        # Calcular totais por motorista
        driver_total_os = df_motorista_detalhado.groupby('motoristaresponsavel')['numeroos'].nunique().to_dict()
        
        for driver in sorted(driver_total_os.keys()):
            total_os_for_driver = driver_total_os.get(driver, 0)
            
            with st.expander(f"Motorista: {driver} ({total_os_for_driver} OS no total)"):
                # Filtrar dados do motorista
                driver_data = df_motorista_detalhado[df_motorista_detalhado['motoristaresponsavel'] == driver].copy()
                
                # Preparar tabela detalhada igual à página de OS em Andamento
                df_display_motorista = driver_data[[
                    'placaequipamento', 'marcaequipamento', 'datahoraos', 'datahorainicio', 'datahorafim',
                    'titulomanutencao', 'motoristaresponsavel', 'mecanicoresponsavel',
                    'tipomanutencao', 'numeroos', 'descricaoos'
                ]].rename(columns={
                    'placaequipamento': 'PLACA',
                    'marcaequipamento': 'MARCA',
                    'datahoraos': 'DATA ABERTURA',
                    'datahorainicio': 'DATA INÍCIO',
                    'datahorafim': 'DATA FIM',
                    'titulomanutencao': 'TÍTULO MANUTENÇÃO',
                    'motoristaresponsavel': 'MOTORISTA',
                    'mecanicoresponsavel': 'MECÂNICO',
                    'tipomanutencao': 'TIPO MANUT.',
                    'numeroos': 'OS',
                    'descricaoos': 'DESCRIÇÃO'
                }).sort_values(by='DATA ABERTURA', ascending=False)
                
                # Exibir tabela detalhada
                st.dataframe(
                    df_display_motorista, 
                    use_container_width=True, 
                    hide_index=True,
                    column_config={
                        "DATA ABERTURA": st.column_config.DatetimeColumn(
                            "DATA ABERTURA",
                            format="DD/MM/YYYY HH:mm"
                        ),
                        "DESCRIÇÃO": st.column_config.TextColumn(
                            "DESCRIÇÃO",
                            width="large"
                        )
                    }
                )
        
        # NOVA TABELA GERAL NO FINAL
        st.divider()
        st.header("TABELA GERAL DE ORDENS DE SERVIÇO")
        
        # Preparar dados com tempo em dias
        df_tabela_geral = df_filtered.fillna({
            'placaequipamento': 'Não Informada',
            'marcaequipamento': 'Não Informada',
            'titulomanutencao': 'Não Informado',
            'motoristaresponsavel': 'Não Informado',
            'mecanicoresponsavel': 'Não Informado',
            'tipomanutencao': 'Não Informado',
            'descricaoos': 'Sem descrição'
        }).copy()
        
        # Calcular tempo em dias
        today = pd.to_datetime('today').normalize()
        df_tabela_geral['TEMPO (D)'] = (today - df_tabela_geral['datahoraos']).dt.days
        df_tabela_geral['TEMPO (D)'] = df_tabela_geral['TEMPO (D)'].apply(lambda x: max(x, 0))
        
        # Preparar tabela igual à OS em Andamento
        df_display_geral = df_tabela_geral[[
            'placaequipamento', 'marcaequipamento', 'datahoraos',
            'titulomanutencao', 'motoristaresponsavel', 'mecanicoresponsavel',
            'tipomanutencao', 'numeroos', 'TEMPO (D)', 'datahorainicio', 'datahorafim', 'descricaoos'
        ]].rename(columns={
            'placaequipamento': 'PLACA',
            'marcaequipamento': 'MARCA',
            'datahoraos': 'DATA ABERTURA',
            'titulomanutencao': 'TÍTULO MANUTENÇÃO',
            'motoristaresponsavel': 'MOTORISTA',
            'mecanicoresponsavel': 'MECÂNICO',
            'tipomanutencao': 'TIPO MANUT.',
            'numeroos': 'OS',
            'datahorainicio': 'DATA INÍCIO',
            'datahorafim': 'DATA FIM',
            'descricaoos': 'DESCRIÇÃO'
        }).sort_values(by='DATA ABERTURA', ascending=False)
        
        st.info(f"Mostrando {len(df_display_geral)} registros filtrados")
        
        # Exibir tabela geral
        st.dataframe(
            df_display_geral, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "DATA ABERTURA": st.column_config.DatetimeColumn(
                    "DATA ABERTURA",
                    format="DD/MM/YYYY HH:mm"
                ),
                "DATA INÍCIO": st.column_config.DatetimeColumn(
                    "DATA INÍCIO",
                    format="DD/MM/YYYY HH:mm"
                ),
                "DATA FIM": st.column_config.DatetimeColumn(
                    "DATA FIM",
                    format="DD/MM/YYYY HH:mm"
                ),
                "TEMPO (D)": st.column_config.NumberColumn(
                    "TEMPO (D)",
                    format="%d"
                ),
                "DESCRIÇÃO": st.column_config.TextColumn(
                    "DESCRIÇÃO",
                    width=400
                )
            }
        )

    except Exception as e:
        st.error(f"Ocorreu um erro ao processar os dados da API: {e}")

# PÁGINA OS EM ANDAMENTO OTIMIZADA
def render_andamento_page():
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title("ORDENS DE SERVIÇO EM ANDAMENTO")
    with col2:
        st.image(LOGO_URL, width=200)

    # BOTÃO DE ATUALIZAÇÃO OTIMIZADO (SÓ HISTÓRICO)
    col1_top, col2_top = st.columns([1, 4])
    with col1_top:
        if st.button("🔄 Atualizar Dados", key="atualizar_andamento"):
            if not st.session_state.config.get('login') or not st.session_state.config.get('password'):
                st.error("Configure login e senha no arquivo config.json")
                return
            log_placeholder = st.empty()
            with st.spinner("Atualizando histórico..."):
                # USA A NOVA FUNÇÃO QUE SÓ BUSCA HISTÓRICO
                success = fetch_historico_only(st.session_state.config, log_callback=log_placeholder.info)
                if success:
                    st.cache_data.clear()
                    log_placeholder.empty()
                    st.success("Histórico atualizado com sucesso!")
                    st.rerun()

    # Carrega dados do histórico (session_state ou banco); exibe aviso só se não houver nenhum dado
    df = load_historico_only()
    if df is None:
        st.warning("Nenhum dado carregado. Clique em 'Atualizar Dados' para buscar informações da API ou aguarde o agendador.")
        return

    try:
            
        # Adiciona situação das OS (sem usar valortotal dos detalhes)
        df['Situação da OS'] = df.apply(classify_os_status, axis=1)

        # FILTROS NA SIDEBAR (IGUAIS AO DASHBOARD)
        st.sidebar.header("Filtros")
        
        col1_sidebar_and, col2_sidebar_and = st.sidebar.columns(2)
        with col2_sidebar_and:
            if st.button("Limpar Filtros", key="limpar_filtros_andamento"):
                keys_to_keep = ['config', 'scheduler_running', 'scheduler_thread', 'last_update', 'update_log', 'next_update_time', 'api_data', 'api_details', 'auto_init_done']
                for key in list(st.session_state.keys()):
                    if key not in keys_to_keep: del st.session_state[key]
                st.rerun()
        
        anos = ['Todos'] + sorted(df['datahoraos'].dt.year.dropna().unique().astype(int), reverse=True)
        anos_selecionados = st.sidebar.multiselect('Período (Ano)', anos, default=['Todos'], key="anos_andamento")
        
        # FILTRO DE MÊS EM PORTUGUÊS (MULTISELECT)
        meses_disponveis = sorted(df['datahoraos'].dt.month.dropna().unique().astype(int))
        meses_opcoes = ['Todos'] + [MONTHS_PT[mes] for mes in meses_disponveis]
        meses_selecionados = st.sidebar.multiselect('Mês', meses_opcoes, default=['Todos'], key="meses_andamento")
        
        os_list = sorted(df['numeroos'].dropna().unique().astype(int))
        os_selecionadas = st.sidebar.multiselect('Pesquisar OS', os_list, key="os_andamento")
        marcas = sorted(df['marcaequipamento'].dropna().unique())
        marca_selecionada = st.sidebar.multiselect('Marca', marcas, key="marca_andamento")
        placas = sorted(df['placaequipamento'].dropna().unique())
        placa_selecionada_filtro = st.sidebar.multiselect('Placa', placas, key="placa_andamento")
        tipos_manutencao = sorted(df['titulomanutencao'].dropna().unique())
        tipo_manutencao_selecionado = st.sidebar.multiselect('Tipo Manutenção', tipos_manutencao, key="tipo_andamento")
        situacoes = sorted(df['Situação da OS'].dropna().unique())
        situacao_selecionada = st.sidebar.multiselect('Situação', situacoes, key="situacao_andamento")
        motoristas = sorted(df['motoristaresponsavel'].dropna().unique())
        motorista_selecionado = st.sidebar.multiselect('Motorista', motoristas, key="motorista_andamento")

        # APLICAR FILTROS
        df_filtered = apply_filters(df, anos_selecionados, meses_selecionados, os_selecionadas, 
                                  marca_selecionada, placa_selecionada_filtro, tipo_manutencao_selecionado, 
                                  situacao_selecionada, motorista_selecionado)

        # Filtrar apenas OS em andamento
        df_andamento = df_filtered[df_filtered['datahorainicio'].notna() & df_filtered['datahorafim'].isna()].copy()
        today = pd.to_datetime('today').normalize()
        df_andamento['TEMPO (D)'] = (today - df_andamento['datahoraos']).dt.days
        df_andamento['TEMPO (D)'] = df_andamento['TEMPO (D)'].apply(lambda x: max(x, 0))

        st.metric("Total de OS em Andamento", len(df_andamento))

        # TABELA COM DESCRIÇÃO
        df_andamento_fillna = df_andamento.fillna({
            'placaequipamento': 'Não Informada',
            'marcaequipamento': 'Não Informada',
            'titulomanutencao': 'Não Informado',
            'motoristaresponsavel': 'Não Informado',
            'mecanicoresponsavel': 'Não Informado',
            'tipomanutencao': 'Não Informado',
            'descricaoos': 'Sem descrição'
        })

        df_display = df_andamento_fillna[[
            'placaequipamento', 'marcaequipamento', 'datahoraos',
            'titulomanutencao', 'motoristaresponsavel', 'mecanicoresponsavel',
            'tipomanutencao', 'numeroos', 'TEMPO (D)', 'descricaoos'
        ]].rename(columns={
            'placaequipamento': 'PLACA',
            'marcaequipamento': 'MARCA',
            'datahoraos': 'DATA ABERTURA',
            'titulomanutencao': 'TÍTULO MANUTENÇÃO',
            'motoristaresponsavel': 'MOTORISTA',
            'mecanicoresponsavel': 'MECÂNICO',
            'tipomanutencao': 'TIPO MANUT.',
            'numeroos': 'OS',
            'descricaoos': 'DESCRIÇÃO'
        }).sort_values(by='DATA ABERTURA', ascending=False)

        st.dataframe(
            df_display, 
            use_container_width=True, 
            hide_index=True,
            column_config={
                "DATA ABERTURA": st.column_config.DatetimeColumn(
                    "DATA ABERTURA",
                    format="DD/MM/YYYY HH:mm"
                ),
                "TEMPO (D)": st.column_config.NumberColumn(
                    "TEMPO (D)",
                    format="%d"
                ),
                "DESCRIÇÃO": st.column_config.TextColumn(
                    "DESCRIÇÃO",
                    width="large"
                )
            }
        )
    except Exception as e:
        st.error(f"Ocorreu um erro ao processar os dados: {e}")

def render_settings_page():
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title("CONFIGURAÇÕES DA API E AGENDAMENTO")
    with col2:
        st.image(LOGO_URL, width=200)

    # Informação sobre configuração
    st.info("📁 Login e senha devem ser configurados no arquivo config.json")
    
    if not st.session_state.config.get('login') or not st.session_state.config.get('password'):
        st.error("Configure login e senha no arquivo config.json primeiro.")
        st.code("""
{
    "login": "seu_login_aqui",
    "password": "sua_senha_aqui",
    "interval_dashboard": 5,
    "interval_andamento": 5
}
        """)
    else:
        st.success("✅ Login e senha configurados no config.json")

    # NOVO: CONFIGURAÇÕES DE INTERVALO SEPARADAS
    st.subheader("Intervalos de Atualização")
    
    col1_interval, col2_interval = st.columns(2)
    with col1_interval:
        st.session_state.config['interval_dashboard'] = st.number_input(
            "Intervalo Dashboard (minutos)", 
            min_value=1, 
            value=st.session_state.config.get('interval_dashboard', 5),
            help="Intervalo usado para atualizações automáticas do agendador do Dashboard"
        )
    
    with col2_interval:
        st.session_state.config['interval_andamento'] = st.number_input(
            "Intervalo OS em Andamento (minutos)", 
            min_value=1, 
            value=st.session_state.config.get('interval_andamento', 5),
            help="Intervalo de referência para a página de OS em Andamento"
        )
    
    if st.button("Salvar Configurações"):
        save_config()

    st.subheader("Controle do Agendador")
    col1_config, col2_config = st.columns(2)
    with col1_config:
        if st.button("Iniciar Agendador", disabled=st.session_state.scheduler_running):
            if not all([st.session_state.config['login'], st.session_state.config['password']]):
                st.error("Configure login e senha no config.json antes de iniciar.")
            else:
                st.session_state.scheduler_running = True
                thread = threading.Thread(target=scheduler_loop)
                add_script_run_ctx(thread)
                thread.start()
                st.session_state.scheduler_thread = thread
                st.success("Agendador iniciado!")
                st.rerun()
    with col2_config:
        if st.button("Parar Agendador", disabled=not st.session_state.scheduler_running):
            st.session_state.scheduler_running = False
            if st.session_state.scheduler_thread:
                st.session_state.scheduler_thread.join(timeout=2)
            st.warning("Agendador parado.")
            st.rerun()
            
    status_color = "green" if st.session_state.scheduler_running else "red"
    st.markdown(f"**Status do Agendador:** <span style='color:{status_color};'>{'Ativo' if st.session_state.scheduler_running else 'Parado'}</span>", unsafe_allow_html=True)
    st.info("ℹ️ O agendador utiliza o intervalo do Dashboard para atualizações automáticas")
    
    countdown_placeholder = st.empty()
    if st.session_state.scheduler_running:
        if st.session_state.get('next_update_time'):
            remaining_seconds = st.session_state.next_update_time - time.time()
            if remaining_seconds > 0:
                mins, secs = divmod(int(remaining_seconds), 60)
                countdown_placeholder.info(f"Próxima atualização em: {mins:02d}:{secs:02d}")
            else:
                countdown_placeholder.info("Aguardando início da próxima atualização...")
        else:
            countdown_placeholder.info("Iniciando a primeira atualização...")
        
        time.sleep(1)
        st.rerun()
        
    st.info(f"Última atualização automática: {st.session_state.last_update}")
    st.code(st.session_state.update_log, language=None)

# --- Ponto de Entrada Principal ---
def main():
    # Inicialização automática: uma vez por sessão, roda "Atualizar Dados" e depois "Iniciar Agendador"
    if not st.session_state.auto_init_done:
        if st.session_state.config.get("login") and st.session_state.config.get("password"):
            place = st.empty()
            with place.container():
                st.info("Inicializando: atualizando dados...")
                def _log_init(msg):
                    st.session_state["update_log"] = msg
                success = fetch_api_data_online(st.session_state.config, log_callback=_log_init)
            if success:
                st.session_state.scheduler_running = True
                thread = threading.Thread(target=scheduler_loop)
                add_script_run_ctx(thread)
                thread.start()
                st.session_state.scheduler_thread = thread
                st.cache_data.clear()
            st.session_state.auto_init_done = True
            place.empty()
            st.rerun()
        else:
            st.session_state.auto_init_done = True  # evita ficar tentando sem login/senha

    # Logo na sidebar
    st.sidebar.image(LOGO_URL)

    # Usando st.Page corretamente
    dashboard_page = st.Page(render_dashboard_page, title="Dashboard", icon="📊")
    andamento_page = st.Page(render_andamento_page, title="OS em Andamento", icon="⏳")
    settings_page = st.Page(render_settings_page, title="Configurações", icon="⚙️")
    
    # Lista de páginas para st.navigation
    pages = [dashboard_page, andamento_page, settings_page]
    
    pg = st.navigation(pages)
    pg.run()

if __name__ == "__main__":
    main()
