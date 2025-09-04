import pandas as pd
import random
from faker import Faker
from datetime import datetime
import sqlite3 

fake = Faker('pt_BR')

# --- Primeira etapa: Geração dos arquivos csv ---

# --- Geração de dados de produtos ---
num_produtos = 50
categorias_produtos = ['Eletrônicos', 'Livros', 'Roupas', 'Alimentos', 'Ferramentas']
lista_produtos = []

print("Primeira etapa: Iniciando a geração de dados de produtos...")
for i in range(1, num_produtos + 1):
    produto = {
        'id_produto': i,
        'nome_produto': fake.word().capitalize() + ' ' + fake.word(),
        'categoria': random.choice(categorias_produtos),
        'custo_produto': round(random.uniform(10.0, 500.0), 2)
    }
    lista_produtos.append(produto)

df_produtos = pd.DataFrame(lista_produtos)
df_produtos.to_csv('data/raw/produtos.csv', index=False, encoding='utf-8')
print("Arquivo 'produtos.csv' gerado em data/raw/")
print("-" * 50)

# --- Geração de dados de clientes ---
num_clientes = 200
segmentos_clientes = ['Pessoa Física', 'Pessoa Jurídica']
lista_clientes = []

print("Iniciando a geração de dados de clientes...")
for i in range(1, num_clientes + 1):
    cliente = {
        'id_cliente': i,
        'nome_cliente': fake.name(),
        'cidade': fake.city(),
        'estado': fake.state_abbr(),
        'segmento': random.choice(segmentos_clientes)
    }
    lista_clientes.append(cliente)

df_clientes = pd.DataFrame(lista_clientes)
df_clientes.to_csv('data/raw/clientes.csv', index=False, encoding='utf-8')
print("Arquivo 'clientes.csv' gerado com em data/raw/")
print("-" * 50)

# --- Geração de dados de vendas ---
num_vendas = 5000
lista_vendas = []

print("Iniciando a geração de dados de vendas...")
for i in range(1, num_vendas + 1):
    id_produto_selecionado = random.randint(1, num_produtos)
    id_cliente_selecionado = random.randint(1, num_clientes)
    custo = df_produtos.loc[df_produtos['id_produto'] == id_produto_selecionado, 'custo_produto'].iloc[0]
    margem_lucro = random.uniform(1.2, 1.8)
    preco_venda = round(custo * margem_lucro, 2)
    venda = {
        'id_venda': i,
        'id_produto': id_produto_selecionado,
        'id_cliente': id_cliente_selecionado,
        'data_venda': fake.date_between(start_date='-2y', end_date='today'),
        'quantidade': random.randint(1, 5),
        'preco_unitario': preco_venda
    }
    lista_vendas.append(venda)

df_vendas = pd.DataFrame(lista_vendas)
df_vendas.to_csv('data/raw/vendas.csv', index=False, encoding='utf-8')
print("Arquivo 'vendas.csv' gerado com sucesso em data/raw/")
print("-" * 50)


# --- Segunda Etapa: Salvando dados no banco de dados SQLite ---

print("\nETAPA 2: Iniciando a criação do banco de dados SQLite...")

# Caminho para o banco de dados na pasta de dados processados
path_db = 'data/processed/varejo.db'

# Criando a conexão com o banco de dados
conn = sqlite3.connect(path_db)
df_produtos.to_sql('produtos', conn, if_exists='replace', index=False)
df_clientes.to_sql('clientes', conn, if_exists='replace', index=False)
df_vendas.to_sql('vendas', conn, if_exists='replace', index=False)

conn.close()

print(f"Banco de dados '{path_db}' criado com as tabelas: produtos, clientes, vendas.")