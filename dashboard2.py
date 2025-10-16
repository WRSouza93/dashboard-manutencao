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
            # Garante que a nova chave exista
            if 'logo_url' not in config:
                config['logo_url'] = 'https://github.com/WRSouza93/dashboard-manutencao/blob/main/Translek.png?raw=true'
            return config
    return {
        'login': '',
        'password': '',
        'save_path': r'C:\Users\wesle\OneDrive\Documentos\Documents\Dados Translek',
        'interval': 5,
        'logo_url': 'https://github.com/WRSouza93/dashboard-manutencao/blob/main/Translek.png?raw=true'
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
if 'editing_config' not in st.session_state:
    # Inicia no modo de edição se o login não estiver configurado
    st.session_state.editing_config = not st.session_state.config.get('login')


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
    """Busca e salva os dados de historico.json e detalhes.json da API."""
    login, password, save_path = config.get('login'), config.get('password'), config.get('save_path')
    st.session_state.next_update_time = None # Reseta o contador no início da atualização

    if not all([login, password, save_path]):
        log_callback("Erro: Login, senha e caminho do arquivo devem ser configurados.")
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
        historico_json_str = json.dumps(data_response.json(), indent=4, ensure_ascii=False)
        
        historico_path = os.path.join(save_path, "historico.json")
        with open(historico_path, 'w', encoding='utf-8') as f: f.write(historico_json_str)
        log_callback(f"Histórico salvo em {os.path.basename(historico_path)}")
    except Exception as e:
        log_callback(f"Erro ao buscar histórico: {e}")
        return

    all_details = []
    try:
        historico_data = json.loads(historico_json_str)
        os_list = historico_data.get("data", [])
        total = len(os_list)
        log_callback(f"Encontradas {total} OS. Buscando detalhes...")
        
        for i, os_item in enumerate(os_list):
            if (i + 1) % 20 == 0:
                log_callback(f"Carregando detalhes... {i+1} de {total} OS")
            
            numeroos = os_item.get("numeroos")
            if numeroos:
                details_url = f"https://yjlcmonbid.execute-api.us-east-1.amazonaws.com/os/V1/find/os-details/{numeroos}"
                response = requests.get(details_url, headers=headers, timeout=15)
                if response.status_code == 200 and response.json().get("status"):
                    all_details.append(response.json())
                time.sleep(0.05)
        
        detalhes_path = os.path.join(save_path, "detalhes.json")
        with open(detalhes_path, 'w', encoding='utf-8') as f: json.dump(all_details, f, indent=4, ensure_ascii=False)
        
        log_callback(f"Atualização completa! {len(all_details)} detalhes salvos.")
        st.session_state.last_update = time.strftime('%d/%m/%Y %H:%M:%S')
        interval_seconds = config.get('interval', 5) * 60
        st.session_state.next_update_time = time.time() + interval_seconds
    except Exception as e:
        log_callback(f"Erro ao buscar detalhes: {e}")

def scheduler_log_callback(message):
    st.session_state.update_log = message

def scheduler_loop():
    """Loop que executa a atualização de dados em intervalos definidos."""
    while st.session_state.get('scheduler_running', False):
        fetch_api_data(st.session_state.config, scheduler_log_callback)
        st.cache_data.clear()
        
        interval_seconds = st.session_state.config.get('interval', 5) * 60
        for _ in range(interval_seconds):
            if not st.session_state.get('scheduler_running', False): break
            time.sleep(1)

@st.cache_data
def load_data(historico_path, detalhes_path):
    with open(historico_path, 'r', encoding='utf-8') as f:
        historico_data = json.load(f)
    df_historico = pd.DataFrame(historico_data['data'])

    with open(detalhes_path, 'r', encoding='utf-8') as f:
        detalhes_data = json.load(f)
    
    all_detalhes = [item for entry in detalhes_data if entry.get('data') and entry['data'][0] is not None for item in entry['data']]
    df_detalhes = pd.DataFrame(all_detalhes)
    df_historico['numeroos'] = df_historico['numeroos'].astype(int)
    df_detalhes.dropna(subset=['numeroos'], inplace=True)
    df_detalhes['numeroos'] = df_detalhes['numeroos'].astype(int)
    for col in ['quantidade', 'valorunit', 'valortotal']:
        df_detalhes[col] = pd.to_numeric(df_detalhes[col], errors='coerce')
    df_detalhes.fillna(0, inplace=True)
    detalhes_agg = df_detalhes.groupby('numeroos').agg(valortotal=('valortotal', 'sum')).reset_index()
    df_merged = pd.merge(df_historico, detalhes_agg, on='numeroos', how='left')
    df_merged['valortotal'].fillna(0, inplace=True)
    for col in ['datahoraos', 'datahorainicio', 'datahorafim']:
        df_merged[col] = pd.to_datetime(df_merged[col], errors='coerce')
    return df_merged, df_detalhes

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
        if st.session_state.config.get('logo_url'):
            st.image(st.session_state.config['logo_url'], width=200)

    st.sidebar.header("Filtros")
    col1_sidebar, col2_sidebar = st.sidebar.columns(2)
    with col1_sidebar:
        if st.button("Atualizar Dados"):
            log_placeholder = st.empty()
            with st.spinner("Atualizando..."):
                fetch_api_data(st.session_state.config, log_callback=log_placeholder.info)
            st.cache_data.clear()
            log_placeholder.empty()
            st.rerun()
    with col2_sidebar:
        if st.button("Limpar Filtros"):
            keys_to_keep = ['config', 'scheduler_running', 'scheduler_thread', 'last_update', 'update_log', 'next_update_time']
            for key in list(st.session_state.keys()):
                if key not in keys_to_keep: del st.session_state[key]
            st.rerun()

    historico_file = os.path.join(st.session_state.config['save_path'], "historico.json")
    detalhes_file = os.path.join(st.session_state.config['save_path'], "detalhes.json")
    
    if not os.path.exists(historico_file) or not os.path.exists(detalhes_file):
        st.warning("Arquivos de dados não encontrados. Vá para a página de Configurações para buscar os dados da API.")
    else:
        try:
            df, df_detalhes = load_data(historico_file, detalhes_file)
            df['Situação da OS'] = df.apply(classify_os_status, axis=1)
            
            anos = ['Todos'] + sorted(df['datahoraos'].dt.year.dropna().unique().astype(int), reverse=True)
            ano_selecionado = st.sidebar.selectbox('Período (Ano)', anos)
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

            df_filtered = df.copy()
            if ano_selecionado != 'Todos': df_filtered = df_filtered[df_filtered['datahoraos'].dt.year == ano_selecionado]
            if os_selecionadas: df_filtered = df_filtered[df_filtered['numeroos'].isin(os_selecionadas)]
            if marca_selecionada: df_filtered = df_filtered[df_filtered['marcaequipamento'].isin(marca_selecionada)]
            if placa_selecionada_filtro: df_filtered = df_filtered[df_filtered['placaequipamento'].isin(placa_selecionada_filtro)]
            if tipo_manutencao_selecionado: df_filtered = df_filtered[df_filtered['titulomanutencao'].isin(tipo_manutencao_selecionado)]
            if situacao_selecionada: df_filtered = df_filtered[df_filtered['Situação da OS'].isin(situacao_selecionada)]
            if motorista_selecionado: df_filtered = df_filtered[df_filtered['motoristaresponsavel'].isin(motorista_selecionado)]
            
            st.markdown("""<style>div[data-testid="stMetricValue"] {font-size: 28px;}</style>""", unsafe_allow_html=True)
            total_os, os_finalizadas, os_sem_valorizacao, custo_total, custo_medio, veiculos_atendidos, tempo_medio_dias = (
                df_filtered['numeroos'].nunique(),
                df_filtered[(df_filtered['datahorafim'].notna()) & (df_filtered['status'].fillna('').str.strip().str.upper() == 'FINALIZADA')]['numeroos'].nunique(),
                df_filtered[df_filtered['valortotal'] == 0]['numeroos'].nunique(),
                df_filtered['valortotal'].sum(),
                df_filtered[df_filtered['valortotal'] > 0]['valortotal'].mean() if not df_filtered[df_filtered['valortotal'] > 0].empty else 0,
                df_filtered['placaequipamento'].nunique(),
                int(((df_filtered.dropna(subset=['datahorainicio', 'datahorafim'])['datahorafim'] - df_filtered.dropna(subset=['datahorainicio', 'datahorafim'])['datahorainicio']).dt.total_seconds() / (24*3600)).mean()) if not df_filtered.dropna(subset=['datahorainicio', 'datahorafim']).empty else 0
            )
            
            col1, col2, col3, col4, col5, col6, col7 = st.columns(7)
            col1.metric("ORDENS DE SERVIÇO", f"{total_os}"); col2.metric("OS FINALIZADAS", f"{os_finalizadas}"); col3.metric("OS SEM VALORIZAÇÃO", f"{os_sem_valorizacao}"); col4.metric("CUSTO TOTAL", f"R$ {custo_total:,.2f}"); col5.metric("CUSTO MÉDIO", f"R$ {custo_medio:,.2f}"); col6.metric("VEÍCULOS ATENDIDOS", f"{veiculos_atendidos}"); col7.metric("DIAS DE ATENDIMENTO", f"{tempo_medio_dias}")

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
                else: st.info("Nenhum dado para exibir no gráfico de Registro de OS com os filtros selecionados.")
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
                placa_selecionada = st.selectbox('Selecione uma placa:', placas_com_custo, key="detalhe_placa")
            with col_filtro2:
                valorizacao_filtro = st.radio("Filtrar por valorização:", ('Todas', 'OS Valorizada', 'OS Não Valorizada'), horizontal=True)

            if placa_selecionada:
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
                    st.metric("Ordens de Serviço Abertas", os_abertas); st.metric("Ordens de Serviço Executadas", os_executadas); st.metric("Valor Total de Serviços", f"R$ {valor_total_servicos:,.2f}"); st.metric("Valor Médio de Manutenções", f"R$ {valor_medio:,.2f}"); st.metric("Tempo Médio por OS (dias)", f"{tempo_medio_dias_placa}"); st.metric("Quantidade Motoristas", qtd_motoristas)
                
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
            else: st.info("Nenhuma placa com dados para exibir na análise detalhada com os filtros selecionados.")
            
            st.divider()
            st.header("ORDENS DE SERVIÇO POR MOTORISTA E PLACA")
            pivot_data = df_filtered.fillna({'motoristaresponsavel': 'Não Informado', 'placaequipamento': 'Não Informada'}).groupby(['motoristaresponsavel', 'placaequipamento'])['numeroos'].nunique().reset_index()
            pivot_data.columns = ['Motorista', 'Placa', 'Quantidade de OS']
            driver_total_os = df_filtered.fillna({'motoristaresponsavel': 'Não Informado'}).groupby('motoristaresponsavel')['numeroos'].nunique().to_dict()
            for driver in sorted(pivot_data['Motorista'].unique()):
                total_os_for_driver = driver_total_os.get(driver, 0)
                with st.expander(f"Motorista: {driver} ({total_os_for_driver} OS no total)"):
                    driver_data = pivot_data[pivot_data['Motorista'] == driver].sort_values(by='Quantidade de OS', ascending=False)
                    st.dataframe(driver_data[['Placa', 'Quantidade de OS']], use_container_width=True, hide_index=True)
        except FileNotFoundError:
            st.error(f"Erro: Arquivo não encontrado. Verifique se os caminhos para `historico.json` e `detalhes.json` estão corretos no script.")
        except Exception as e:
            st.error(f"Ocorreu um erro ao processar os arquivos: {e}")

def render_andamento_page():
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title("ORDENS DE SERVIÇO EM ANDAMENTO")
    with col2:
        if st.session_state.config.get('logo_url'):
            st.image(st.session_state.config['logo_url'], width=200)

    historico_file = os.path.join(st.session_state.config['save_path'], "historico.json")
    if not os.path.exists(historico_file):
        st.warning("Arquivo de histórico não encontrado. Vá para a página de Configurações para buscar os dados.")
        return

    try:
        with open(historico_file, 'r', encoding='utf-8') as f:
            historico_data = json.load(f)
        df = pd.DataFrame(historico_data['data'])

        for col in ['datahoraos', 'datahorainicio', 'datahorafim']:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
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
        st.error(f"Ocorreu um erro ao processar o arquivo de histórico: {e}")

def render_settings_page():
    col1, col2 = st.columns([4, 1])
    with col1:
        st.title("CONFIGURAÇÕES DA API E AGENDAMENTO")
    with col2:
        if st.session_state.config.get('logo_url'):
            st.image(st.session_state.config['logo_url'], width=200)

    if not st.session_state.config.get('login') or st.session_state.editing_config:
        st.session_state.config['login'] = st.text_input("Login", value=st.session_state.config.get('login', ''))
        st.session_state.config['password'] = st.text_input("Senha", type="password", value=st.session_state.config.get('password', ''))
        st.session_state.config['save_path'] = st.text_input("Pasta para salvar os arquivos", value=st.session_state.config.get('save_path', ''))
        st.session_state.config['logo_url'] = st.text_input("Endereço do Logo", value=st.session_state.config.get('logo_url', ''))
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
            if not all([st.session_state.config['login'], st.session_state.config['password'], st.session_state.config['save_path']]):
                st.error("Preencha Login, Senha e Pasta para salvar antes de iniciar.")
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
    if 'page' not in st.session_state:
        st.session_state.page = "Dashboard"

    pages = {
        "Dashboard": render_dashboard_page,
        "OS em Andamento": render_andamento_page,
        "Configurações": render_settings_page
    }
    
    with st.sidebar:
        if st.session_state.config.get('logo_url'):
            st.image(st.session_state.config['logo_url'])
        
        selection = st.radio("Navegação", list(pages.keys()))

    pages[selection]()

if __name__ == "__main__":
    main()

