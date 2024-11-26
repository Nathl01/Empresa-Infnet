import os
import sqlite3
import pandas as pd

#Criar banco de dados
conn = sqlite3.connect('empresa.db')
cursor = conn.cursor()

#Função para a criação das tabelas
def criar_tabela(nome_tabela, colunas):
    cursor.execute(f"DROP TABLE IF EXISTS {nome_tabela}")
    cursor.execute(f"CREATE TABLE {nome_tabela} ({colunas})")

#Função para inserir os dados do arquivo CSV nas tabelas
def inserir_dados_csv(nome_arquivo, nome_tabela):
    df = pd.read_csv(nome_arquivo)
    df.to_sql(nome_tabela, conn, if_exists='append', index=False)

#Criar as tabelas
criar_tabela('Funcionarios', '''
    id_funcionario INTEGER PRIMARY KEY,
    nome_funcionario TEXT NOT NULL,
    id_cargo INTEGER,
    id_departamento INTEGER,
    salario_real REAL NOT NULL,
    data_admissao DATE NOT NULL
''')

criar_tabela('Cargos', '''
    id_cargo INTEGER PRIMARY KEY,
    descricao_cargo TEXT NOT NULL,
    salario_base REAL NOT NULL,
    nivel_cargo TEXT NOT NULL,
    carga_horaria INTEGER NOT NULL
''')

criar_tabela('Departamentos', '''
    id_departamento INTEGER PRIMARY KEY,
    nome_departamento TEXT NOT NULL,
    id_gerente INTEGER,
    andar_localizacao INTEGER NOT NULL,
    orcamento_anual REAL NOT NULL
''')

criar_tabela('Historico_salarios', '''
    id_historico INTEGER PRIMARY KEY,
    id_funcionario INTEGER,
    mes TEXT,
    ano INTEGER,
    salario REAL
''')

criar_tabela('Dependentes', '''
    id_dependente INTEGER PRIMARY KEY,
    id_funcionario INTEGER,
    nome_dependente TEXT,
    idade INTEGER,
    parentesco TEXT
''')

criar_tabela('Projetos_desenvolvidos', '''
    id_projeto INTEGER PRIMARY KEY,
    nome_projeto TEXT NOT NULL,
    descricao TEXT,
    data_inicio DATE,
    data_conclusao DATE,
    id_funcionario INTEGER,
    custo_projeto REAL,
    status TEXT CHECK(status IN ('Em Planejamento', 'Em Execução', 'Concluído', 'Cancelado')),
    FOREIGN KEY (id_funcionario) REFERENCES Funcionarios(id_funcionario)
''')

criar_tabela('Recursos_do_projeto', '''
    id_recurso INTEGER PRIMARY KEY,
    id_projeto INTEGER,
    descricao_recurso TEXT,
    tipo_recurso TEXT CHECK(tipo_recurso IN ('Financeiro', 'Material', 'Humano')),
    quantidade_utilizada INTEGER,
    data_utilizacao DATE,
    FOREIGN KEY (id_projeto) REFERENCES Projetos_desenvolvidos(id_projeto)
''')

#Inserir os dados dos arquivos CSV's nas tabelas
inserir_dados_csv('funcionarios.csv', 'Funcionarios')
inserir_dados_csv('cargos.csv', 'Cargos')
inserir_dados_csv('departamentos.csv', 'Departamentos')
inserir_dados_csv('historico_salarios.csv', 'Historico_salarios')
inserir_dados_csv('dependentes.csv', 'Dependentes')
inserir_dados_csv('projetos_desenvolvidos.csv', 'Projetos_desenvolvidos')
inserir_dados_csv('recursos_do_projeto.csv', 'Recursos_do_projeto')

#Função para executar as consultas SQL
def executar_consulta(query, descricao):
    print(f"\n{descricao}:")
    resultado = pd.read_sql_query(query, conn)
    print(resultado)
    return resultado

# Criar diretório para armazenar arquivos JSON
output_dir = os.path.join(os.path.abspath(os.getcwd()), 'json_outputs')
os.makedirs(output_dir, exist_ok=True)

#Consultas

#1.Consulta para obter a média dos salários dos funcionários responsáveis por projetos concluídos, agrupados por departamento.
questao1 = executar_consulta("""
SELECT d.nome_departamento, AVG(f.salario_real) AS media_salario
FROM Projetos_desenvolvidos p
JOIN Funcionarios f ON p.id_funcionario = f.id_funcionario
JOIN Departamentos d ON f.id_departamento = d.id_departamento
WHERE p.status = 'Concluído'
GROUP BY d.nome_departamento
""", "Média dos salários dos funcionários responsáveis por projetos concluídos, agrupados por departamento")

#2.Consulta para identificar os três recursos materiais mais usados nos projetos.
questao2 = executar_consulta("""
SELECT descricao_recurso, SUM(quantidade_utilizada) AS total_utilizado
FROM Recursos_do_projeto
WHERE tipo_recurso = 'Material'
GROUP BY descricao_recurso
ORDER BY total_utilizado DESC
LIMIT 3
""", "Três recursos materiais mais usados nos projetos")

try:
    questao2.to_json(os.path.join(output_dir, 'recursos_materiais_mais_utilizados.json'), orient='records', lines=True)
    print("Arquivo 'recursos_materiais_mais_utilizados.json' criado com sucesso.")
except Exception as e:
    print(f"Erro ao criar 'recursos_materiais_mais_utilizados.json': {e}")

#3.Executar a consulta para calcular o custo total dos projetos concluídos por departamento.
questao3 = executar_consulta("""
SELECT d.nome_departamento, SUM(p.custo_projeto) AS custo_total
FROM Projetos_desenvolvidos p
JOIN Funcionarios f ON p.id_funcionario = f.id_funcionario
JOIN Departamentos d ON f.id_departamento = d.id_departamento
WHERE p.status = 'Concluído'
GROUP BY d.nome_departamento
""", "Custo total dos projetos concluídos por departamento")

try:
    questao3.to_json(os.path.join(output_dir,'custo_total_projetos_por_departamento.json'), orient='records', lines=True)
    print("Arquivo 'custo_total_projetos_por_departamento.json' criado com sucesso.")
except Exception as e:
    print(f"Erro ao criar 'custo_total_projetos_por_departamento.json': {e}")

#4.Executar a consulta para listar projetos 'Em execução'.
questao4 = executar_consulta("""
SELECT 
    p.nome_projeto, 
    p.custo_projeto, 
    p.data_inicio, 
    p.data_conclusao, 
    f.nome_funcionario
FROM 
    Projetos_desenvolvidos p
JOIN 
    Funcionarios f ON p.id_funcionario = f.id_funcionario
WHERE 
    p.status = 'Em Execução'
""", "Projetos em Execução com Detalhes")

try:
    questao4.to_json(os.path.join(output_dir, 'projetos_em_execucao.json'), orient='records', lines=True)
    print("Arquivo 'projetos_em_execucao.json' criado com sucesso.")
except Exception as e:
    print(f"Erro ao criar 'projetos_em_execucao.json': {e}")

#5.Executar a consulta para identificar o projeto com o maior número de dependentes.
questao5 = executar_consulta("""
SELECT 
    p.nome_projeto, 
    COUNT(dep.id_dependente) AS num_dependentes
FROM 
    Projetos_desenvolvidos p
JOIN 
    Funcionarios f ON p.id_funcionario = f.id_funcionario
JOIN 
    Dependentes dep ON f.id_funcionario = dep.id_funcionario
GROUP BY 
    p.id_projeto
ORDER BY 
    num_dependentes DESC
LIMIT 1
""", "Projeto com o Maior Número de Dependentes")

#Fechar a conexão com o banco de dados
conn.close()


