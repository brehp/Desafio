#bibliotecas
import pandas as pd
from sqlalchemy import create_engine
import logging


''' 

#Criando tabelas apartir dos dados do Retail.xlsx #############################################################

# Carregar os dados
df = pd.read_excel('Online Retail.xlsx')

#A tabela de dimensão do produto vai armazenar informações sobre os produtos
dimensoes_produto = df[['StockCode', 'Description']].drop_duplicates().reset_index(drop=True)
dimensoes_produto.rename(columns={'StockCode': 'ID', 'Description': 'nome'}, inplace=True)

#A tabela do cliente vai armazenar informações sobre os clientes
dimensoes_cliente = df[['CustomerID', 'Country']].drop_duplicates().reset_index(drop=True)
dimensoes_cliente.rename(columns={'CustomerID': 'ID', 'Country': 'País'}, inplace=True)

#A tabela de dimensão de data vai armazenas as datas de vendas
df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])  # Comando que irá realizar a conversão para formato de data
dimensoes_data = df[['InvoiceDate']].drop_duplicates().reset_index(drop=True) #evita duplicata
dimensoes_data['ano'] = dimensoes_data['InvoiceDate'].dt.year
dimensoes_data['mes'] = dimensoes_data['InvoiceDate'].dt.month
dimensoes_data['dia'] = dimensoes_data['InvoiceDate'].dt.day
dimensoes_data.rename(columns={'InvoiceDate': 'data_venda'}, inplace=True)

# Criar a tabela fato de vendas
fato_vendas = df[['InvoiceNo', 'CustomerID', 'StockCode', 'Quantity', 'InvoiceDate', 'UnitPrice']]
fato_vendas['valor_total'] = fato_vendas['Quantity'] * fato_vendas['UnitPrice'] #acrescenta valor total

fato_vendas.rename(columns={'InvoiceNo': 'ID_Venda', 'CustomerID': 'ID_cliente', 'StockCode': 'ID_produto', 'Quantity': 'Quantidade', 'InvoiceDate': 'Data','UnitPrice': 'Preço_unitario'}, inplace=True)




#Conexão com banco de dados############################################################################

# Conexão com o banco de dados
engine = create_engine('postgresql://postgres:brenda@localhost:5432/Desafio_Kyros')

# Carregar as tabelas no banco de dados
dimensoes_produto.to_sql('dimensoes_produto', engine, if_exists='replace', index=False)
dimensoes_cliente.to_sql('dimensoes_cliente', engine, if_exists='replace', index=False)
dimensoes_data.to_sql('dimensoes_data', engine, if_exists='replace', index=False)
fato_vendas.to_sql('fato_vendas', engine, if_exists='replace', index=False)

print("Dados carregados no banco de dados!")

'''



#Processo ETL############################################################################
# Configuração básica do logger
logging.basicConfig(filename='Informaçoes.log', 
                    level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')


#função que extrai os dados do arquivo excel
def extrair_dados():
    logging.info('Realizando a extração dos dados do arquivo Excel.')
    df = pd.read_excel('Online Retail.xlsx')
    logging.info('Concluida a extração de dados.')
    return df

#Função que realizará a limpeza dos dados nesse caso de valores nulo
def limpeza_inicial(df):
    logging.info('Inicio da limpeza dos dados a fim de evitar valores invalidos.')

    registros_iniciais = len(df)
    # Remover registros em que não tenha CustomerID e StockCode 
    df = df.dropna(subset=['CustomerID', 'StockCode'])

    #Coloca 0 quando o valor é invalido 
    df['Quantity'].fillna(0, inplace=True)
    df['UnitPrice'].fillna(0, inplace=True)  

    # Remover os registros onde Quantity ou UnitPrice são 0 ou negativos, somente mantem quando ambas as condiçoes são satisfeitas
    df = df[(df['Quantity'] > 0.00) & (df['UnitPrice'] > 0.00)]

    registros_finais = len(df)

    logging.info(f'Registros que foram removidos durante a limpeza: {registros_iniciais - registros_finais}')
    logging.info('Limpeza dos dados.')
    return df


#Função que irá transformar os dados na modelagem anteriormente realizada
def Modelagem(df):
    logging.info('Realizando a modelagem')

    # Dimensão de Produto
    dimensoes_produto = df[['StockCode', 'Description']].drop_duplicates().reset_index(drop=True)
    dimensoes_produto.rename(columns={'StockCode': 'ID', 'Description': 'nome'}, inplace=True)
    
    # Dimensão de Cliente
    dimensoes_cliente = df[['CustomerID', 'Country']].drop_duplicates().reset_index(drop=True)
    dimensoes_cliente.rename(columns={'CustomerID': 'ID', 'Country': 'País'}, inplace=True)
    
    # Dimensão de Data
    df['InvoiceDate'] = pd.to_datetime(df['InvoiceDate'])  # Comando que irá realizar a conversão para formato de data
    dimensoes_data = df[['InvoiceDate']].drop_duplicates().reset_index(drop=True) #evita duplicata
    dimensoes_data['ano'] = dimensoes_data['InvoiceDate'].dt.year
    dimensoes_data['mes'] = dimensoes_data['InvoiceDate'].dt.month
    dimensoes_data['dia'] = dimensoes_data['InvoiceDate'].dt.day
    dimensoes_data['hora'] = dimensoes_data['InvoiceDate'].dt.time
    dimensoes_data.rename(columns={'InvoiceDate': 'data_venda'}, inplace=True)
    
    # Tabela Fato de Vendas
    fato_vendas = df[['InvoiceNo', 'CustomerID', 'StockCode', 'Quantity', 'InvoiceDate', 'UnitPrice']]
    fato_vendas['InvoiceDate'] = pd.to_datetime(fato_vendas['InvoiceDate'], errors='coerce')
    fato_vendas['valor_total'] = fato_vendas['Quantity'] * fato_vendas['UnitPrice']
    fato_vendas.rename(columns={'InvoiceNo': 'id_venda', 'CustomerID': 'id_cliente', 'StockCode': 'id_produto', 'Quantity': 'Quantidade', 'InvoiceDate': 'data_venda','UnitPrice': 'Preco_Unitario'}, inplace=True)
    
    logging.info('Finalizando a modelagem')
    return dimensoes_produto, dimensoes_cliente, dimensoes_data, fato_vendas

#Função que carrega os dataframe para o banco de dados
def carregar_BD(dimensoes_produto, dimensoes_cliente, dimensoes_data, fato_vendas):

    try:
        logging.info('Carregando as informações para o banco de dados.')
        # Conexão com o banco de dados
        BD = create_engine('postgresql://postgres:brenda@localhost:5432/Desafio_Kyros')

        # Carregar as tabelas no banco de dados
        dimensoes_produto.to_sql('dimensoes_produto', BD, if_exists='replace', index=False)
        dimensoes_cliente.to_sql('dimensoes_cliente', BD, if_exists='replace', index=False)
        dimensoes_data.to_sql('dimensoes_data', BD, if_exists='replace', index=False)
        fato_vendas.to_sql('fato_vendas', BD, if_exists='replace', index=False)
        logging.info('Os dados foram todos carregados.')
    except Exception as e:
        logging.error(f'Erro durante a carga dos dados: {e}')


if __name__ == "__main__":
    # Executa a extração
    df_inicial = extrair_dados()
    
    #Realiza a limpeza inicial dos dados 
    df_limpo = limpeza_inicial(df_inicial)

    # Executa a transformação
    df_produto, df_cliente, df_data, df_vendas = Modelagem(df_limpo)
    
    # Executa a carga no Data Warehouse
    carregar_BD(df_produto, df_cliente, df_data, df_vendas)
