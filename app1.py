# --- BLOCO 1: IMPORTS E CONFIGURAÇÃO ---
import os
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
import seaborn as sns
import streamlit as st
from fpdf import FPDF
from datetime import datetime
import unicodedata

# Configuração da Página (War Room Theme - Adaptado para Saúde B2B)
st.set_page_config(
    page_title="Market Mapping - Saúde B2B",
    layout="wide",
    initial_sidebar_state="expanded",
    page_icon="🩺"
)

# Paleta Semântica (Corporate Teal/Blue - Estilo do Relatório)
WAR_PALETTE = ["#003f5c", "#2f4b7c", "#665191", "#a05195", "#d45087"]
ORDER_TIER = ['Hospital/Alta Complexidade', 'Medicina Diagnóstica', 'Clínica Premium', 'Consultório/Pequeno']
COLOR_MAP = {
    'Hospital/Alta Complexidade': '#003f5c', 
    'Clínica Premium': '#2f4b7c', 
    'Medicina Diagnóstica': '#a05195', 
    'Consultório/Pequeno': '#bdc3c7'
}

# --- BLOCO 2: CARGA DE DADOS ---
@st.cache_data(ttl=3600)
def load_data() -> pd.DataFrame:
    base_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(base_dir, 'leads_saude_processed.parquet')

    if not os.path.exists(file_path):
        st.error(f"Base de dados não encontrada em: {file_path}. Rode o script de ETL primeiro.")
        st.stop()

    df = pd.read_parquet(file_path)
    
    # Tratamentos de segurança (Bairro e Identificador Key Account)
    if 'is_key_account' not in df.columns:
        df['is_key_account'] = df['segmento_saude'].isin(['Hospital/Alta Complexidade', 'Clínica Premium']).astype(int)
    
    if 'bairro_norm' not in df.columns and 'bairro' in df.columns:
        df['bairro_norm'] = df['bairro'].astype(str).str.lower().str.strip()
    elif 'bairro_norm' not in df.columns:
        df['bairro_norm'] = 'nao_informado'
        
    return df

# --- BLOCO 3: SIDEBAR (FILTROS) ---
def sidebar_filters(df: pd.DataFrame):
    st.sidebar.markdown("## 🎯 Radar de Prospecção")
    st.sidebar.markdown("Filtre o território de atuação:")
    
    # Filtro de Estado
    lista_ufs = [str(x) for x in df['uf_norm'].dropna().unique().tolist()]
    opts_uf = ["Todos"] + sorted(lista_ufs)
    sel_uf = st.sidebar.selectbox("Estado (UF)", opts_uf, index=0)
    
    df_filtered = df.copy()
    sel_cidade = "Todas"
    
    if sel_uf != "Todos":
        df_filtered = df_filtered[df_filtered['uf_norm'] == sel_uf]
        
        # Filtro de Cidade (Usando municipio_visual para ficar bonito)
        lista_cidades = [str(x) for x in df_filtered['municipio_visual'].dropna().unique().tolist()]
        opts_cidade = ["Todas"] + sorted(lista_cidades)
        sel_cidade = st.sidebar.selectbox("Cidade", opts_cidade, index=0)
        
        if sel_cidade != "Todas":
            df_filtered = df_filtered[df_filtered['municipio_visual'] == sel_cidade]

    # Filtro de Segmento de Saúde
    opts_tier = ORDER_TIER
    sel_tier = st.sidebar.multiselect("Segmento Alvo", opts_tier, default=opts_tier)
    
    if sel_tier:
        df_filtered = df_filtered[df_filtered['segmento_saude'].isin(sel_tier)]
        
    return df_filtered, sel_uf, sel_cidade

# --- BLOCO 4: GERAÇÃO DE PDF (REPORTE EXECUTIVO) ---
def generate_pdf(df_city: pd.DataFrame, cidade: str, estado: str):
    pdf = FPDF()
    pdf.add_page()
    pdf.set_auto_page_break(auto=True, margin=15)
    
    def fix_text(text):
        if not isinstance(text, str): return str(text)
        try:
            return text.encode('latin-1', 'replace').decode('latin-1')
        except:
            return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8')

    # Cabeçalho
    pdf.set_font("Arial", "B", 16)
    pdf.set_text_color(0, 63, 92) # Azul Corporate
    pdf.cell(0, 10, fix_text(f"Dossiê de Prospecção B2B: {cidade.upper()} - {estado.upper()}"), align="C", ln=1)
    
    pdf.set_font("Arial", "", 10)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 10, fix_text(f"Gerado em: {datetime.now().strftime('%d/%m/%Y')} | Leads mapeados: {len(df_city)}"), align="C", ln=1)
    pdf.ln(5)
    
    # Resumo
    pdf.set_font("Arial", "B", 12)
    pdf.set_text_color(40, 40, 40)
    pdf.cell(0, 10, fix_text("1. GOLDEN LEADS LOCAIS (Top Clínicas e Hospitais)"), ln=1)
    pdf.set_font("Arial", "", 9)
    pdf.multi_cell(0, 5, fix_text("Listagem filtrada pelas empresas com maior estrutura de capital e maturidade na região. Ideal para visitas de Field Sales."))
    pdf.ln(5)
    
    # Tabela TOP Leads
    col_w = [70, 40, 40, 30]
    headers = ["Estabelecimento (Nome)", "Segmento", "Capital (R$)", "Idade (Anos)"]
    
    pdf.set_font("Arial", "B", 8)
    pdf.set_fill_color(220, 220, 220)
    for i, h in enumerate(headers):
        pdf.cell(col_w[i], 8, fix_text(h), 1, align='C', fill=True, ln=(1 if i == 3 else 0))
    
    pdf.set_font("Arial", "", 7)
    
    # Filtra Top Leads (Prioriza Hospitais e Clínicas Premium)
    df_top = df_city.sort_values(by=['capital_social', 'idade'], ascending=[False, False]).head(20)
    
    if df_top.empty:
        pdf.cell(0, 10, "Nenhum lead relevante encontrado neste filtro.", 1, align='C', ln=1)
    else:
        for _, r in df_top.iterrows():
            nome = str(r.get('razao_social', 'N/D'))[:35]
            seg = str(r.get('segmento_saude', 'N/D'))[:20]
            cap = f"{r.get('capital_social', 0):,.0f}"
            idade = f"{r.get('idade', 0):.1f}"
            
            pdf.cell(col_w[0], 7, fix_text(nome), 1)
            pdf.cell(col_w[1], 7, fix_text(seg), 1, align='C')
            pdf.cell(col_w[2], 7, fix_text(cap), 1, align='R')
            pdf.cell(col_w[3], 7, fix_text(idade), 1, align='C', ln=1)
            
    # Geração do PDF à prova de falhas de versão
    pdf_out = pdf.output(dest='S')
    
    # Se a biblioteca retornar texto, converte para bytes
    if isinstance(pdf_out, str):
        return pdf_out.encode('latin-1')
    # Se já retornar em bytes (bytearray), apenas garante o formato correto
    else:
        return bytes(pdf_out)

# --- BLOCO 5: APP MAIN ---
def main():
    st.markdown("<h1 style='text-align: center; color: #003f5c;'>🩺 Bússola dos Dados: Inteligência Saúde Privada</h1>", unsafe_allow_html=True)
    
    st.markdown("""
    <div style='background-color: #1E1E1E; padding: 15px; border-radius: 5px; margin-bottom: 20px; border-left: 5px solid #003f5c;'>
        <p style='color: #E0E0E0; font-size: 16px; margin: 0;'>
            <b>Contexto Estratégico:</b> Mapeamento de Oportunidades no Setor de Saúde B2B. 
            Direcione sua força de vendas avaliando Densidade Geográfica, Risco (Maturidade) e Porte de Clínicas/Hospitais.
        </p>
    </div>
    """, unsafe_allow_html=True)

    df = load_data()
    df_filtered, sel_uf, sel_cidade = sidebar_filters(df)
    
    if df_filtered.empty:
        st.warning("Sem dados para os filtros aplicados. Tente ampliar a busca.")
        return

    # --- KPIs RÁPIDOS ---
    total = len(df_filtered)
    sharks = df_filtered['is_key_account'].sum()
    mediana_cap = df_filtered['capital_social'].median()
    
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Volume de Estabelecimentos", f"{total:,}")
    col2.metric("Key Accounts (Premium/Hospitais)", f"{sharks:,}")
    col3.metric("Capital Social Mediano", f"R$ {mediana_cap:,.0f}")
    col4.metric("Idade Média (Risco)", f"{df_filtered['idade'].mean():.1f} anos")
    
    st.markdown("---")
    
    # --- ORGANIZAÇÃO EM ABAS ---
    tab1, tab2, tab3, tab4 = st.tabs([
        "🌍 Matriz de Expansão (Geográfica)", 
        "🏙️ Micro-Targeting (Bairros)", 
        "📊 Perfil de Risco e Carteira", 
        "📋 Leads & Exportação"
    ])

    # ==========================================
    # ABA 1: VISÃO MACRO / MATRIZ GEOGRÁFICA
    # ==========================================
    with tab1:
        # LÓGICA 1: VISÃO NACIONAL TOTAL (MAPA DE CALOR)
        if sel_uf == "Todos" and sel_cidade == "Todas":
            st.markdown("### 🔥 Densidade Nacional de Saúde Privada")
            st.markdown("*Matriz cruzando Estados e Segmentação (Consultórios vs. Alta Complexidade).*")
            
            df_heat = df_filtered.groupby(['uf_norm', 'segmento_saude']).size().unstack(fill_value=0)
            cols_avail = [c for c in ORDER_TIER if c in df_heat.columns]
            df_heat = df_heat[cols_avail]
            
            df_heat['Total_Volume'] = df_heat.sum(axis=1)
            df_heat = df_heat.sort_values('Total_Volume', ascending=True).tail(15).drop(columns=['Total_Volume'])
            
            fig_heat = px.imshow(
                df_heat,
                labels=dict(x="Segmento", y="Estado (UF)", color="Volume Absoluto"),
                x=df_heat.columns, y=df_heat.index,
                text_auto=True, color_continuous_scale="Blues", aspect="auto",
                title="Heatmap de Concentração: Top 15 Estados"
            )
            fig_heat.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=500)
            st.plotly_chart(fig_heat, use_container_width=True)

        # LÓGICA 2: VISÃO ESTADUAL (MATRIZ TÁTICA DE CIDADES)
        elif sel_uf != "Todos" and sel_cidade == "Todas":
            st.markdown(f"### 📍 Matriz Tática Geográfica: {sel_uf}")
            st.markdown("*Cruzamento de Maturidade (Eixo X) vs Volume de Leads (Eixo Y). Foco no quadrante superior direito.*")
            
            city_matrix = df_filtered.groupby('municipio_visual').agg(
                total=('cnpj_completo', 'count'),
                idade_med=('idade', 'mean'),
                key_accounts=('is_key_account', 'sum')
            ).reset_index()
            
            city_matrix = city_matrix[city_matrix['total'] > 5].sort_values('total', ascending=False).head(20)
            
            if not city_matrix.empty:
                fig_scatter = px.scatter(
                    city_matrix, x='idade_med', y='total', size='total', color='key_accounts',
                    hover_name='municipio_visual', size_max=45, text='municipio_visual',
                    title=f'Matriz de Oportunidades: {sel_uf}',
                    labels={'total': 'Volume Total de Estabelecimentos', 'idade_med': 'Maturidade Média (Anos)', 'key_accounts': 'Qtd Key Accounts'},
                    color_continuous_scale='Teal'
                )
                fig_scatter.update_traces(textposition='top center')
                fig_scatter.add_hline(y=city_matrix['total'].mean(), line_dash="dot", annotation_text="Volume Médio")
                fig_scatter.add_vline(x=city_matrix['idade_med'].mean(), line_dash="dot", annotation_text="Maturidade Média")
                fig_scatter.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=600)
                st.plotly_chart(fig_scatter, use_container_width=True)
            else:
                st.info("Municípios insuficientes no filtro.")

        # LÓGICA 3: VISÃO CIDADE ESPECÍFICA (BENCHMARKING)
        elif sel_cidade != "Todas":
            st.markdown(f"### 🧬 Benchmarking Dinâmico de Mercado: {sel_cidade}")
            st.markdown(f"*Comparação da estrutura de {sel_cidade} com cidades similares no Estado ({sel_uf}).*")
            
            df_state_baseline = df[df['uf_norm'] == sel_uf]
            city_matrix_full = df_state_baseline.groupby('municipio_visual').agg(
                total=('cnpj_completo', 'count'),
                key_accounts=('is_key_account', 'sum'),
                ticket=('capital_social', 'median')
            ).reset_index()
            
            sel_stats = city_matrix_full[city_matrix_full['municipio_visual'] == sel_cidade]
            
            if not sel_stats.empty:
                val_total = sel_stats['total'].values[0]
                val_ka = sel_stats['key_accounts'].values[0]
                
                city_matrix_full['dist_euclidiana'] = np.sqrt(((city_matrix_full['total'] - val_total) ** 2) + (((city_matrix_full['key_accounts'] - val_ka) * 5) ** 2))
                
                peer_cluster = city_matrix_full.sort_values('dist_euclidiana').head(6).copy()
                peer_cluster['Classificação'] = peer_cluster['municipio_visual'].apply(lambda x: 'Alvo Estratégico' if x == sel_cidade else 'Peer Regional')
                
                fig_peers = px.scatter(
                    peer_cluster, x='total', y='key_accounts', size='ticket', color='Classificação',
                    hover_name='municipio_visual', size_max=45, text='municipio_visual',
                    title=f'Ecossistema: {sel_cidade} vs Cidades Semelhantes',
                    color_discrete_map={'Alvo Estratégico': '#003f5c', 'Peer Regional': '#a05195'}
                )
                fig_peers.update_traces(textposition='top center', textfont=dict(size=14, color='white'))
                fig_peers.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=500)
                st.plotly_chart(fig_peers, use_container_width=True)

    # ==========================================
    # ABA 2: VISÃO MICRO (Bairros)
    # ==========================================
    with tab2:
        st.markdown("### 🏘️ Micro-Targeting: Análise de Bairros")
        st.markdown("Identifique as 'Medical Zones' (clusters de saúde) para otimizar roteiros de visita presencial.")
        
        if sel_cidade != "Todas":
            bairros_data = df_filtered[df_filtered['bairro_norm'] != 'nao_informado']
            top_bairros = bairros_data['bairro_norm'].value_counts().head(15).index.tolist()
            bairros_top = bairros_data[bairros_data['bairro_norm'].isin(top_bairros)]
            
            if not bairros_top.empty:
                bairros_tier = bairros_top.groupby(['bairro_norm', 'segmento_saude']).size().reset_index(name='count')
                
                fig_bairros = px.bar(
                    bairros_tier, x='count', y='bairro_norm', color='segmento_saude',
                    title=f'Concentração Clínica por Bairro em {sel_cidade}',
                    orientation='h', color_discrete_map=COLOR_MAP,
                    category_orders={"bairro_norm": top_bairros}
                )
                fig_bairros.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", height=600)
                st.plotly_chart(fig_bairros, use_container_width=True)
            else:
                st.info("Resolução geográfica de bairros insuficiente para esta cidade.")
        else:
            st.warning("⚠️ Selecione uma cidade específica no menu lateral para visualizar os dados de bairro.")

    # ==========================================
    # ABA 3: PERFIL DE MERCADO (Pizza e Risco)
    # ==========================================
    with tab3:
        c1, c2 = st.columns(2)
        
        with c1:
            st.markdown("### 🍩 Distribuição da Carteira (Porte)")
            st.markdown("*A base da pirâmide (Consultórios) exige venda Digital. O topo exige Visita Presencial.*")
            df_tier = df_filtered['segmento_saude'].value_counts().reset_index()
            df_tier.columns = ['segmento', 'count']
            
            fig_donut = px.pie(
                df_tier, values='count', names='segmento',
                hole=0.5, color='segmento', color_discrete_map=COLOR_MAP
            )
            fig_donut.update_traces(textposition='inside', textinfo='percent+label')
            fig_donut.update_layout(template="plotly_dark", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", showlegend=False)
            st.plotly_chart(fig_donut, use_container_width=True)

        with c2:
            st.markdown("### ⏳ Curva de Sobrevivência (Gestão de Risco)")
            st.markdown("*Avalie a mortalidade para aplicar carências ou pagamento antecipado.*")
            data_plot = df_filtered[df_filtered['idade'] <= 40]
            
            if not data_plot.empty:
                fig_hist, ax = plt.subplots(figsize=(8, 5))
                fig_hist.patch.set_facecolor('#1E1E1E')
                ax.set_facecolor('#1E1E1E')
                
                sns.histplot(data=data_plot, x='idade', bins=30, kde=True, color='#2f4b7c', ax=ax)
                ax.axvline(2, color='red', linestyle='--', linewidth=2, label="Zona Vermelha (Risco <2 anos)")
                ax.axvline(10, color='green', linestyle='--', linewidth=2, label="Zona Verde (Premium >10 anos)")
                
                ax.tick_params(colors='white')
                ax.xaxis.label.set_color('white')
                ax.yaxis.label.set_color('white')
                ax.title.set_color('white')
                ax.legend()
                
                st.pyplot(fig_hist)
            else:
                st.info("Dispersão temporal insuficiente para compilação do histograma.")

    # ==========================================
    # ABA 4: EXPORTAÇÃO E RIVAIS DIRETOS
    # ==========================================
    with tab4:
        st.markdown("### 🎯 Lista de Prospecção Qualificada (CRM)")
        st.markdown("Listagem ordenada por estrutura (Capital Social) e estabilidade (Idade).")
        
        # Filtro de qualidade: remove os muito pequenos se a base for muito grande
        df_leads = df_filtered.sort_values(by=['capital_social', 'idade'], ascending=[False, False])
        
        cols_to_show = ['razao_social', 'municipio_visual', 'bairro_norm', 'segmento_saude', 'idade', 'capital_social']
        cols_available = [c for c in cols_to_show if c in df_leads.columns]
        
        st.dataframe(df_leads[cols_available].head(100), use_container_width=True)
        
        st.markdown("---")
        st.markdown("### 📥 Datalake & Output Executivo")
        
        c_dl1, c_dl2 = st.columns(2)
        
        with c_dl1:
            st.download_button(
                "💾 Exportar Excel para CRM", 
                df_filtered.to_csv(index=False).encode('utf-8-sig'), 
                f"leads_saude_{sel_uf}.csv", 
                "text/csv", 
                use_container_width=True
            )
            
        with c_dl2:
            if sel_cidade != "Todas":
                try:
                    pdf_bytes = generate_pdf(df_filtered, sel_cidade, sel_uf)
                    st.download_button(
                        "📄 Emissão de Dossiê Local (PDF)", 
                        data=pdf_bytes, 
                        file_name=f"dossie_saude_{sel_cidade}.pdf", 
                        mime="application/pdf", 
                        use_container_width=True
                    )
                except Exception as e:
                    st.error(f"Erro ao compilar o engine de renderização PDF: {e}")
            else:
                st.button("🔒 Fixe uma Cidade no filtro lateral para liberar o Dossiê PDF", disabled=True, use_container_width=True)

if __name__ == "__main__":
    main()