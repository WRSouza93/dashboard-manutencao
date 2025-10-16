import streamlit as st
import pandas as pd
import json
import altair as alt
import requests
import os
import time
import threading
from streamlit.runtime.scriptrunner import add_script_run_ctx

# --- Configuração Inicial da Página e Estado da Sessão ---
st.set_page_config(layout="wide")

CONFIG_FILE = "config.json"

# --- Funções de Gerenciamento de Configuração ---

def load_config():
    """Carrega as configurações do arquivo JSON se ele existir."""
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, 'r') as f:
            config = json.load(f)
            return config
    return {
        'login': '',
        'password': '',
        'interval': 5,
    }

def save_config():
    """Salva as configurações atuais no arquivo JSON."""
    # Garante que campos sensíveis como a senha não sejam salvos se estiverem vazios
    config_to_save = {k: v for k, v in st.session_state.config.items() if k != 'password' or v}
    with open(CONFIG_FILE, 'w') as f:
        json.dump(config_to_save, f, indent=4)
    st.success("Configurações salvas com sucesso!")

# --- Inicialização do Estado da Sessão ---
if 'config' not in st.session_state:
    st.session_state.config = load_config()
if 'last_update' not in st.session_state:
    st.session_state.last_update = "Nenhuma atualização ainda."
if 'scheduler_running' not in st.session_state:
    st.session_state.scheduler_running = False
if 'scheduler_thread' not in st.session_state:
    st.session_state.scheduler_thread = None
if 'update_log' not in st.session_state:
    st.session_state.update_log = "Aguardando início do agendador."
if 'next_update_time' not in st.session_state:
    st.session_state.next_update_time = None
if 'editing_config' not in st.session_state:
    st.session_state.editing_config = not st.session_state.config.get('login')
if 'df_merged' not in st.session_state:
    st.session_state.df_merged = None
if 'df_detalhes' not in st.session_state:
    st.session_state.df_detalhes = None


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

def fetch_api_data(config, log_callback):
    """Busca os dados da API e os processa, salvando em session_state."""
    login, password = config.get('login'), config.get('password')
    st.session_state.next_update_time = None

    if not all([login, password]):
        log_callback("Erro: Login e senha devem ser configurados.")
        return

    log_callback("Iniciando atualização... Obtendo token...")
    token = _get_token(login, password, log_callback)
    if not token: return

    try:
        log_callback("Carregando histórico...")
        data_url = "https://yjlcmonbid.execute-api.us-east-1.amazonaws.com/os/V1/find/last-update/2020-01-01"
        headers = {"Authorization": token}
        data_response = requests.get(data_url, headers=headers, timeout=60)
        data_response.raise_for_status()
        historico_data = data_response.json()
        log_callback("Histórico carregado.")
    except Exception as e:
        log_callback(f"Erro ao buscar histórico: {e}")
        return

    all_details = []
    try:
        os_list = historico_data.get("data", [])
        total = len(os_list)
        log_callback(f"Encontradas {total} OS. Buscando detalhes...")
        
        for i, os_item in enumerate(os_list):
            if (i + 1) % 50 == 0:
                log_callback(f"Carregando detalhes... {i+1} de {total} OS")
            
            numeroos = os_item.get("numeroos")
            if numeroos:
                details_url = f"https://yjlcmonbid.execute-api.us-east-1.amazonaws.com/os/V1/find/os-details/{numeroos}"
                response = requests.get(details_url, headers=headers, timeout=15)
                if response.status_code == 200 and response.json().get("status"):
                    all_details.append(response.json())
                time.sleep(0.02) # Pequeno delay para não sobrecarregar a API
        
        log_callback(f"Processando {len(all_details)} detalhes...")

        # Processamento dos dados (lógica do antigo load_data)
        df_historico = pd.DataFrame(historico_data['data'])
        detalhes_list = [item for entry in all_details if entry.get('data') and entry['data'][0] is not None for item in entry['data']]
        df_detalhes = pd.DataFrame(detalhes_list)
        
        df_historico['numeroos'] = df_historico['numeroos'].astype(int)
        if not df_detalhes.empty:
            df_detalhes.dropna(subset=['numeroos'], inplace=True)
            df_detalhes['numeroos'] = df_detalhes['numeroos'].astype(int)
            for col in ['quantidade', 'valorunit', 'valortotal']:
                df_detalhes[col] = pd.to_numeric(df_detalhes[col], errors='coerce')
            df_detalhes.fillna(0, inplace=True)
            detalhes_agg = df_detalhes.groupby('numeroos').agg(valortotal=('valortotal', 'sum')).reset_index()
            df_merged = pd.merge(df_historico, detalhes_agg, on='numeroos', how='left')
        else: # Caso não venha nenhum detalhe
            df_merged = df_historico.copy()
            df_merged['valortotal'] = 0

        df_merged['valortotal'].fillna(0, inplace=True)
        for col in ['datahoraos', 'datahorainicio', 'datahorafim']:
            df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce')

        # Salva os dataframes processados no estado da sessão
        st.session_state.df_merged = df_merged
        st.session_state.df_detalhes = df_detalhes

        log_callback("Atualização completa!")
        st.session_state.last_update = time.strftime('%d/%m/%Y %H:%M:%S')
        interval_seconds = config.get('interval', 5) * 60
        st.session_state.next_update_time = time.time() + interval_seconds
    except Exception as e:
        log_callback(f"Erro ao processar detalhes: {e}")

def scheduler_log_callback(message):
    st.session_state.update_log = message

def scheduler_loop():
    """Loop que executa a atualização de dados em intervalos definidos."""
    while st.session_state.get('scheduler_running', False):
        fetch_api_data(st.session_state.config, scheduler_log_callback)
        
        interval_seconds = st.session_state.config.get('interval', 5) * 60
        for _ in range(interval_seconds):
            if not st.session_state.get('scheduler_running', False): break
            time.sleep(1)

def classify_os_status(row):
    is_valorizado = row['valortotal'] > 0
    status_str = str(row.get('status', '')).strip().upper()
    is_finalizada = pd.notna(row['datahorafim']) and status_str == 'FINALIZADA'
    if is_valorizado and is_finalizada: return "VALORIZADO E FINALIZADO"
    if pd.notna(row['datahorainicio']) and pd.isna(row['datahorafim']): return "ANDAMENTO"
    if is_valorizado and pd.isna(row['datahorafim']): return "EXECUTADO"
    if is_finalizada: return "FINALIZADA"
    if pd.isna(row['datahorainicio']) and pd.isna(row['datahorafim']): return "EM BRANCO"
    return "OUTRO"

# --- Funções de Renderização de Página ---
def render_dashboard_page():
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title("DASHBOARD DE MANUTENÇÃO TRANSLEK")
    with col2:
        st.image("https://github.com/WRSouza93/dashboard-manutencao/blob/main/Translek.png?raw=true", width=200)

    st.sidebar.header("Filtros")
    col1_sidebar, col2_sidebar = st.sidebar.columns(2)
    with col1_sidebar:
        if st.button("Atualizar Dados"):
            log_placeholder = st.empty()
            with st.spinner("Atualizando..."):
                fetch_api_data(st.session_state.config, log_callback=log_placeholder.info)
            log_placeholder.empty()
            st.rerun()
    with col2_sidebar:
        if st.button("Limpar Filtros"):
            keys_to_keep = ['config', 'scheduler_running', 'scheduler_thread', 'last_update', 'update_log', 'next_update_time', 'df_merged', 'df_detalhes']
            for key in list(st.session_state.keys()):
                if key not in keys_to_keep: del st.session_state[key]
            st.rerun()

    if st.session_state.df_merged is None:
        st.warning("Os dados ainda não foram carregados. Vá para a página de Configurações para buscar os dados da API.")
    else:
        df = st.session_state.df_merged
        df_detalhes = st.session_state.df_detalhes
        df['Situação da OS'] = df.apply(classify_os_status, axis=1)
        
        # Filtros
        anos = ['Todos'] + sorted(df['datahoraos'].dt.year.dropna().unique().astype(int), reverse=True)
        ano_selecionado = st.sidebar.selectbox('Período (Ano)', anos)
        # ... (restante dos filtros)

        df_filtered = df.copy()
        # ... (lógica de filtragem)

        # O restante do seu código de dashboard continua aqui, usando df_filtered
        st.markdown("""<style>div[data-testid="stMetricValue"] {font-size: 28px;}</style>""", unsafe_allow_html=True)
        # ... (Métricas, Gráficos, etc.)

def render_andamento_page():
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title("ORDENS DE SERVIÇO EM ANDAMENTO")
    with col2:
        st.image("https://github.com/WRSouza93/dashboard-manutencao/blob/main/Translek.png?raw=true", width=200)

    if st.session_state.df_merged is None:
        st.warning("Os dados ainda não foram carregados. Vá para a página de Configurações para buscar os dados da API.")
        return

    try:
        df = st.session_state.df_merged.copy()
        df_andamento = df[df['datahorainicio'].notna() & df['datahorafim'].isna()].copy()

        today = pd.to_datetime('today').normalize()
        df_andamento['TEMPO (D)'] = (today - df_andamento['datahoraos']).dt.days
        df_andamento['TEMPO (D)'] = df_andamento['TEMPO (D)'].apply(lambda x: max(x, 0))

        st.metric("Total de OS em Andamento", len(df_andamento))
        
        df_display = df_andamento[[
            'placaequipamento', 'marcaequipamento', 'datahoraos', 
            'titulomanutencao', 'motoristaresponsavel', 'mecanicoresponsavel',
            'tipomanutencao', 'numeroos', 'TEMPO (D)'
        ]].rename(columns={
            'placaequipamento': 'PLACA',
            'marcaequipamento': 'MARCA',
            'datahoraos': 'DATA ABERTURA',
            'titulomanutencao': 'TÍTULO MANUTENÇÃO',
            'motoristaresponsavel': 'MOTORISTA',
            'mecanicoresponsavel': 'MECÂNICO',
            'tipomanutencao': 'TIPO MANUT.',
            'numeroos': 'OS'
        }).sort_values(by='DATA ABERTURA', ascending=False)
        
        st.dataframe(df_display, use_container_width=True, hide_index=True)

    except Exception as e:
        st.error(f"Ocorreu um erro ao processar os dados: {e}")

def render_settings_page():
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title("CONFIGURAÇÕES DA API E AGENDAMENTO")
    with col2:
        st.image("https://github.com/WRSouza93/dashboard-manutencao/blob/main/Translek.png?raw=true", width=200)

    if not st.session_state.config.get('login') or st.session_state.editing_config:
        st.session_state.config['login'] = st.text_input("Login", value=st.session_state.config.get('login', ''))
        st.session_state.config['password'] = st.text_input("Senha", type="password", value=st.session_state.config.get('password', ''))
        st.session_state.config['interval'] = st.number_input("Intervalo de atualização (minutos)", min_value=1, value=st.session_state.config.get('interval', 5))
        
        if st.button("Salvar Configurações"):
            save_config()
            st.session_state.editing_config = False
            st.rerun()
    else:
        st.success("Dados de configuração já foram preenchidos.")
        if st.button("Alterar Configurações"):
            st.session_state.editing_config = True
            st.rerun()

    st.subheader("Controle do Agendador")
    col1_config, col2_config = st.columns(2)
    with col1_config:
        if st.button("Iniciar Agendador", disabled=st.session_state.scheduler_running):
            if not all([st.session_state.config['login'], st.session_state.config['password']]):
                st.error("Preencha Login e Senha para salvar antes de iniciar.")
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
    dashboard_page = st.Page(render_dashboard_page, title="Dashboard", default=True)
    andamento_page = st.Page(render_andamento_page, title="OS em Andamento")
    settings_page = st.Page(render_settings_page, title="Configurações")
    
    pg = st.navigation([dashboard_page, andamento_page, settings_page])
    pg.run()

if __name__ == "__main__":
    main()

