# 🧭 Plataforma de Inteligência de Mercado B2B

Este repositório contém o ecossistema proprietário de Business Intelligence e 
Mapeamento de Mercado desenvolvido para a **Rocha Sales**.

A plataforma cruza dados públicos da Receita Federal do Brasil (RFB) com algoritmos 
customizados de *scoring*, geolocalização e regras de negócio para gerar listas 
de prospecção de altíssima conversão (Golden Leads) e dossiês executivos.

## 🚀 Arquitetura da Solução

O projeto opera em um modelo de **Fábrica de Dashboards**, onde cada setor da 
economia possui um pipeline de dados (ETL) dedicado e uma aplicação analítica focada 
nas dores específicas daquele mercado.

* **Datalake (Armazenamento):** Apache Parquet (`.parquet`) para altíssima performance.
* **Engine Visual:** Streamlit, Plotly e Seaborn.
* **Engine de Relatórios:** FPDF2 (Geração dinâmica de Dossiês PDF).

## 📊 Módulos Setoriais Desenvolvidos

1. **Market Mapping - Concorrência (War Room):** Análise bivariada de saturação local, identificação de "Tubarões" e algoritmo K-NN para identificação de cidades com comportamento mercadológico semelhante (Peers Regionais).
2. **Setor de Tecnologia (Corporate Tech):** Segmentação de mercado de TI cruzando Volume de Leads vs Maturidade (Idade Média), focando em clientes de baixo risco (Oceano Azul).
3. **Setor de Educação:** Mapeamento de Colégios e Universidades. Avalia a vocação do bairro (Ensino Básico vs Superior) e Tier de Riqueza para venda de Seguro Saúde focado em retenção de professores.
4. **Engenharia e Construção Civil:** Inteligência geográfica para separar Sedes Corporativas (Compradores de High Ticket) de Canteiros de Obras (Seguro de Acidentes). Remoção inteligente de ruído (MEIs).
5. **Mercado de Carbono (ESG):** Análise geopolítica do fluxo de capital ambiental. O modelo conecta Hubs de Originação (Projetos REDD+ e Agro) aos Hubs de Demanda (Enterprise), provando visualmente o valor do Broker através de Diagramas Sankey.

## ⚙️ Como executar localmente

1. Crie o ambiente virtual e instale as dependências:
   ```bash
   pip install -r requirements.txt


2. Execute o pipeline de dados do setor desejado:
   ```bash
   python exampleX/etl_to_parquet.py

3. Inicie o servidor do Streamlit:
   ```bash
   streamlit run exampleX/app.py

🔒 Confidencialidade e Licença
PROPRIEDADE EXCLUSIVA - ROCHA SALES
Todos os direitos reservados. O código, os algoritmos e a engenharia de dados contidos neste repositório são estritamente confidenciais. É proibida a cópia, reprodução ou distribuição sem autorização explícita. Consulte o arquivo LICENSE para mais detalhes.
