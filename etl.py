# Script para etl
import pandas as pd
import requests
from bs4 import BeautifulSoup
import mysql.connector
import glob
from datetime import date

DB_USER = 'root'
DB_PASS = 'root'
DB_HOST = '127.0.0.1'
DB_DATABASE = 'inss'
URL_INSS = 'https://dados.gov.br/dataset/inss-beneficios-concedidos'
URL_SALARIO_MINIMO = 'http://www.ipeadata.gov.br/exibeserie.aspx?stub=1&serid1739471028=1739471028'
ANO_CORTE_SALARIO = '2018.12'



INSERT_DIM_GENERIC = "INSERT INTO TABELA (COLUNA) VALUES (%s)"
SELECT_DIM_GENERIC = "SELECT * FROM TABELA WHERE COLUNA = 'VAL'"
INSERT_FATO = "INSERT INTO fato_qtd_rendamensalinicial (data_concessao, quantidade, id_especie, id_despacho, data_nascimento, idade, id_sexo, id_clientela, municipio, vinculo_dependentes, forma_filiacao, id_uf, id_faixaetaria, valor_concedido) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"





def factory_conexao_db():
    cnx = mysql.connector.connect(user=DB_USER, password=DB_PASS,
                                  host=DB_HOST,
                                  database=DB_DATABASE)
    return cnx


def executar_select(query):
    print(f""" executando {query} """)
    cnx = factory_conexao_db()
    cursor = cnx.cursor()
    cursor.execute(query)
    result = []
    for a in cursor:
        print(a)
        result.append(a)
    cursor.close()
    cnx.close()
    return result


def executar_insert(query, val):
    print(f""" executando {query} """)
    cnx = factory_conexao_db()
    cursor = cnx.cursor()
    cursor.execute(query, val)

    idgerado = cursor.lastrowid

    cnx.commit()

    print(f""" {cursor.rowcount} registro inseridos""")

    cursor.close()
    cnx.close()
    return idgerado


def manter_generico(consulta, insert, val, tabela, coluna):
    existe = []
    if consulta:
        existe = executar_select(consulta.replace('TABELA', tabela).replace('COLUNA', coluna).replace('VAL', val[0]))
    if len(existe) > 0:
        for e in existe:
            return e[0]
    else:
        return executar_insert(insert.replace('TABELA', tabela).replace('COLUNA', coluna), val)


def baixar_arquivo(url):
    local_filename = url.split('/')[-1]
    r = requests.get(url, verify=False)
    f = open(local_filename, 'wb')
    for chunk in r.iter_content(chunk_size=512 * 1024):
        if chunk:  # filter out keep-alive new chunks
            f.write(chunk)
    f.close()
    return


def baixarcsvinss():
    page = requests.get(URL_INSS)
    soup = BeautifulSoup(page.text, 'html.parser')
    links = soup.find_all('a', attrs={"class": "resource-url-analytics"})

    for l in links:
        csv = l.attrs['href']
        csv = csv.split('url=')[1]
        print(f""" Baixando {csv} """)
        baixar_arquivo(csv)
        print(f""" Sucesso ao baixar {csv} """)


def baixarsalariominimo():
    print('baixando salario p1')
    page = requests.get(URL_SALARIO_MINIMO)
    print('baixando salario p2')
    soup = BeautifulSoup(page.text, 'html.parser')
    trs = soup.find_all('tr', attrs={"class": "dxgvDataRow"})
    print('baixando salario p3')
    salarios = []
    for tr in trs:
        tds = tr.find_all('td', attrs={"class": "dxgv"})
        if len(tds) == 2:
            td_ano = tds[0].text
            td_valor = tds[1].text
            if td_ano >= ANO_CORTE_SALARIO:
                td_valor_float = float(td_valor.replace(".", "").replace(",", "."))
                print(f""" ano {td_ano} valor {td_valor_float}""")
                salarios.append({"ano": td_ano, "valor": td_valor_float})
    return salarios


def ler_linha_csv(row, nmcoluna, idx):
    if nmcoluna in row:
        return row[nmcoluna]
    if len(row) >= idx and nmcoluna == "Competência concessão":
        return row[idx]
    return 'Não informado'


def converter_mesano(mesano):
    # converter 202101 ou 2021-01-13 ou janeiro/2019 para o padrao 2005.02
    # 2018-12-01
    mesano = str(mesano)
    if '-' in mesano:
        arr = mesano.split('-')
        return arr[0] + "." + arr[1]
    elif '/' in mesano:
        # janeiro/2019
        mes = ['janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho', 'julho', 'agosto', 'setembro',
               'outubro', 'novembro', 'dezembro']
        arr = mesano.split('/')
        idx = str(mes.index(arr[0]) + 1)
        idx = '0' + idx if len(idx) == 1 else idx
        return arr[1] + "." + idx
    else:
        return mesano[0:4] + "." + mesano[4:]


def get_no_array(arr, prop, valor):
    for a in arr:
        if a[prop] == valor:
            return a


# Criança 0-12, Adolescente 13-17, Adulto 18-50, Idoso 51-infitito

def calcular_idade(dtnascimento):
    # padrao data nascimento 30/10/1995
    current_date = date.today()
    ano = dtnascimento.split('/')[2]
    data_actual = current_date.year
    idade = data_actual - int(ano)
    return idade


def regra_faixa_etaria(idade):
    if idade <= 12:
        return 'Criança'
    elif 13 <= idade <= 17:
        return 'Adolescente'
    elif 18 <= idade <= 59:
        return 'Adulto'
    else:
        return 'Idoso'


def etl_beneficios(salarios):
    # csv = 'concedidos-10-2019.csv'
    csv = 'beneficios-concedidos-01-2020.csv'
    print(f""" carregando para o banco de dados o csv de beneficios {csv}""")
    try:
        data = pd.read_csv(csv, sep=';', low_memory=False, encoding='utf-8')
    except UnicodeDecodeError as e:
        data = pd.read_csv(csv, sep=';', low_memory=False, encoding='ISO-8859-1')
    print(data.columns)
    c_compe = 'Competência concessão'
    c_especie = 'Espécie'
    c_despacho = 'Despacho'
    c_dtnasc = 'Dt Nascimento'
    c_sex = 'Sexo.'
    c_cli = 'Clientela'
    c_mun = 'Mun Resid'
    c_vin = 'Vínculo dependentes'
    c_forma = 'Forma Filiação'
    c_uf = 'UF'
    c_qt = 'Qt SM RMI'
    for i, row in data.iterrows():
        data_concessao = ler_linha_csv(row, c_compe, 0)
        especie = ler_linha_csv(row, c_especie, 1)
        id_especie = manter_generico(SELECT_DIM_GENERIC, INSERT_DIM_GENERIC, (especie,), 'dim_especie', 'especie')
        despacho = ler_linha_csv(row, c_despacho, 4)
        id_despacho = manter_generico(SELECT_DIM_GENERIC, INSERT_DIM_GENERIC, (despacho,), 'dim_despacho', 'despacho')
        data_nascimento = ler_linha_csv(row, c_dtnasc, 5)
        sexo = ler_linha_csv(row, c_sex, 6)
        id_sexo = manter_generico(SELECT_DIM_GENERIC, INSERT_DIM_GENERIC, (sexo,), 'dim_sexo', 'sexo')
        clientela = ler_linha_csv(row, c_cli, 7)
        id_clientela = manter_generico(SELECT_DIM_GENERIC, INSERT_DIM_GENERIC, (clientela,), 'dim_clientela', 'clientela')
        municipio = ler_linha_csv(row, c_mun, 8)
        vinculo_dependentes = ler_linha_csv(row, c_vin, 9)
        forma_filiacao = ler_linha_csv(row, c_forma, 10)
        uf = ler_linha_csv(row, c_uf, 11)
        id_uf= manter_generico(SELECT_DIM_GENERIC, INSERT_DIM_GENERIC, (uf,), 'dim_uf', 'uf')
        qtd = ler_linha_csv(row, c_qt, 12)
        quantidade = float(qtd.replace(".", "").replace(",", "."))
        vlsalariominimodomes = get_no_array(salarios, "ano", converter_mesano(data_concessao))["valor"]
        valor_concedido = quantidade * vlsalariominimodomes
        idade = calcular_idade(data_nascimento)
        faixaetaria = regra_faixa_etaria(idade)
        id_faixaetaria = manter_generico(SELECT_DIM_GENERIC, INSERT_DIM_GENERIC, (faixaetaria,), 'dim_faixaetaria', 'faixaetaria')

        print(
            f""" DATA_CONCESSAO {data_concessao} ESPECIE {especie} DESPACHO {despacho} DATA_NASCIMENTO 
            {data_nascimento} SEXO {sexo} CLIENTELA {clientela} MUNICIPIO {municipio} 
            VINCULO_DEPE {vinculo_dependentes} FORMA_FI {forma_filiacao} UF {uf} QTD {quantidade}
            IDADE {idade}
            FAIXA_ETARIA {faixaetaria}
            VALOR_CONCEDIDO {valor_concedido}""")

        manter_generico(None, INSERT_FATO, (data_concessao, quantidade, id_especie, id_despacho, data_nascimento, idade, id_sexo, id_clientela, municipio, vinculo_dependentes, forma_filiacao, id_uf, id_faixaetaria, valor_concedido), 'dim_faixaetaria', 'faixaetaria')


    for key, value in data.iteritems():
        print(key, value)
        print()


if __name__ == '__main__':
    print('Iniciando')

    csvs_concedidos = glob.glob("./*concedidos*.csv")
    if len(csvs_concedidos) < 5:
        baixarcsvinss()
    else:
        print(' CSVs inss ja foram baixados anteriormente')

    salarios = baixarsalariominimo()

    etl_beneficios(salarios)


    data = ("Fernando3",)
    id = manter_generico(SELECT_DIM_GENERIC, INSERT_DIM_GENERIC, data, tabela='teste', coluna='teste')
    print(id)
