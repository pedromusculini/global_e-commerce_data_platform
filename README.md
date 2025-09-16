# Global E-commerce Analytics Dashboard

![Python](https://img.shields.io/badge/python-3.8+-blue.svg)
![License](https://img.shields.io/badge/license-MIT-green.svg)
![Stars](https://img.shields.io/github/stars/yourusername/global-ecommerce-analytics-dashboard.svg)

## Overview

Este é um projeto end-to-end em Python para previsão de churn em e-commerce e dashboard interativo. O projeto inclui análise exploratória de dados (EDA), modelagem preditiva e uma interface de dashboard para visualização de insights.

## How to Run

1. Clone o repositório:
   ```bash
   git clone https://github.com/yourusername/global-ecommerce-analytics-dashboard.git
   cd global-ecommerce-analytics-dashboard
   ```

2. Instale as dependências:
   ```bash
   pip install -r requirements.txt
   ```

3. Execute o dashboard:
   ```bash
   make run-dashboard
   ```

## Dependencies

As principais dependências estão listadas em `requirements.txt`. Inclui:
- pandas: Para manipulação de dados
- plotly: Para visualizações interativas
- scikit-learn: Para modelagem de machine learning
- streamlit: Para o dashboard web

## Roadmap

- [ ] Implementar scraping de dados
- [ ] Desenvolver modelos de churn prediction
- [ ] Criar dashboard interativo
- [ ] Adicionar testes automatizados
- [ ] Documentar o projeto completo

## Estrutura do Projeto

- `/data/`: Dados brutos e limpos, banco de dados SQLite
- `/notebooks/`: Notebooks para EDA e modelagem
- `/scripts/`: Scripts Python para limpeza, scraping e dashboard
- `/tests/`: Testes com pytest
- `/docs/`: Documentação, README, screenshots