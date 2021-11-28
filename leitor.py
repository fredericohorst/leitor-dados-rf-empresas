import utils
from pyspark.sql.types import StringType, StructField, StructType

# spark configs:
spark_session, sqlContext, sc = utils.spark_configs(memory='4G')

# dicionario de faixas de idade para socios
faixa_etaria = sqlContext.read.csv(
        'dict_faixa_etaria.txt',
        sep=',',
        schema=StructType([StructField('cod_faixa',StringType(),True),StructField('desc_faixa_etaria',StringType(),True)])
        )
faixa_etaria.registerTempTable('faixa_etaria')

# importador
def importador_spark(arquivo, separador, schema, sql_alias, previsualizacao):
        """
        Método para construir o descompactador. O método constrói um objeto
        spark para a leitura dos arquivos e registra uma tabela temporária 
        em memória para permitir a utilização do SQL na consulta dos dados.
        """
        leitor = sqlContext.read.csv(
                arquivo,
                sep=separador,
                inferSchema="false",
                header="false",
                schema=schema,
                encoding="ISO-8859-1"
        )
        leitor.registerTempTable(sql_alias)
        if previsualizacao == True: leitor.show(10) # preview da tabela
        return leitor

def importador_cnaes(arquivo, separador, previsualizacao):
        """
        Método para a importação dos arquivos contendo
        as informações das tabelas do CNAE.
        """
        schema_cnaes = StructType([
                StructField('codigo',StringType(), True),
                StructField('descricao',StringType(), True)
                ])
        importador_spark(arquivo,separador=separador,schema=schema_cnaes,sql_alias='cnaes', previsualizacao=previsualizacao)
        return 'tabela de cnaes carregada com sucesso'

def importador_socios(arquivo, separador, previsualizacao):
        """
        Método para a importação dos arquivos contendo
        as informações das tabelas dos Sócios.
        """
        schema_socios = StructType([
                StructField('cnpj_basico',StringType(),True),
                StructField('identificador_de_socio',StringType(),True),
                StructField('nome_do_socio',StringType(),True),
                StructField('cnpj_cpf_socio',StringType(),True),
                StructField('qualificacao_do_socio',StringType(),True),
                StructField('data_de_entrada_sociedade',StringType(),True),
                StructField('pais',StringType(),True),
                StructField('representante_legal',StringType(),True),
                StructField('nome_do_representante',StringType(),True),
                StructField('qualificacao_do_representante_legal',StringType(),True),
                StructField('faixa_etaria',StringType(),True)
                ])
        importador_spark(arquivo,separador=separador,schema=schema_socios,sql_alias='socios', previsualizacao=False)
        query_fx = """
                SELECT socios.* , faixa_etaria.*
                FROM socios
                LEFT JOIN faixa_etaria ON socios.faixa_etaria = faixa_etaria.cod_faixa
        """
        socios_fx = sqlContext.sql(query_fx)
        socios_fx.registerTempTable('socios')
        if previsualizacao == True: socios_fx.show(10)
        return 'tabela de socios carregada com sucesso'

def importador_empresas(arquivo, separador, previsualizacao):
        """
        Método para a importação dos arquivos contendo
        as informações das tabelas das Empresas.
        """
        schema_empresas = StructType([
                StructField('cnpj_basico',StringType(),True),
                StructField('razao_social',StringType(),True),
                StructField('natureza_juridica',StringType(),True),
                StructField('qualificacao_do_responsavel',StringType(),True),
                StructField('capital_social_da_empresa',StringType(),True),
                StructField('porte_da_empresa',StringType(),True),
                StructField('ente_federativo_responsavel',StringType(),True)
                ])
        importador_spark(arquivo,separador=separador,schema=schema_empresas,sql_alias='empresas', previsualizacao=previsualizacao)
        return 'tabela de empresas carregada com sucesso'

def importador_estabelecimentos(arquivo, separador, previsualizacao):
        """
        Método para a importação dos arquivos contendo
        as informações das tabelas dos Estabelecimentos.
        """
        schema_estabelecimentos = StructType([
                StructField('cnpj_basico',StringType(),True),
                StructField('cnpj_ordem',StringType(),True),
                StructField('cnpj_dv',StringType(),True),
                StructField('identificador_matriz_filial',StringType(),True),
                StructField('nome_fantasia',StringType(),True),
                StructField('situacao_cadastral',StringType(),True),
                StructField('data_situacao_cadastral',StringType(),True),
                StructField('motivo_situacao_cadastral',StringType(),True),
                StructField('nome_da_cidade_no_exterior',StringType(),True),
                StructField('pais',StringType(),True),
                StructField('data_inicio_atividade',StringType(),True),
                StructField('cnae_fiscal_principal',StringType(),True),
                StructField('cnae_fiscal_secundaria',StringType(),True),
                StructField('tipo_logradouro',StringType(),True),
                StructField('logradouro',StringType(),True),
                StructField('numero',StringType(),True),
                StructField('complemento',StringType(),True),
                StructField('bairro',StringType(),True),
                StructField('cep',StringType(),True),
                StructField('uf',StringType(),True),
                StructField('municipio',StringType(),True),
                StructField('ddd_1',StringType(),True),
                StructField('telefone_1',StringType(),True),
                StructField('ddd_2',StringType(),True),
                StructField('telefone_2',StringType(),True),
                StructField('ddd_fax',StringType(),True),
                StructField('fax',StringType(),True),
                StructField('correio_eletronico',StringType(),True),
                StructField('situacao_especial',StringType(),True),
                StructField('data_situacao_especial',StringType(),True)
                ])
        importador_spark(arquivo,separador=separador,schema=schema_estabelecimentos,sql_alias='estabelecimentos', previsualizacao=previsualizacao)
        return 'tabela de estabelecimentos carregada com sucesso'

def importador_motivo_situacao_cadastral(arquivo, separador, previsualizacao):
        """
        Método para a importação dos arquivos contendo
        as informações da tabela do Motivo Situação Cadastral.
        """
        schema_sit_cadastral = StructType([
                StructField('codigo',StringType(),True),
                StructField('descricao',StringType(),True),
                ])
        importador_spark(arquivo,separador=separador,schema=schema_sit_cadastral,sql_alias='situacao_cadastral', previsualizacao=previsualizacao)
        return 'tabela de situacao cadastral carregada com sucesso'

def importador_municipios(arquivo, separador, previsualizacao):
        """
        Método para a importação dos arquivos contendo
        as informações da tabela dos Municípios.
        """
        schema_municipios = StructType([
                StructField('codigo',StringType(),True),
                StructField('nome_municipio',StringType(),True),
                ])
        importador_spark(arquivo,separador=separador,schema=schema_municipios,sql_alias='municipios', previsualizacao=previsualizacao)
        return 'tabela de municipios carregada com sucesso'

def importador_natureza_juridica(arquivo, separador, previsualizacao):
        """
        Método para a importação dos arquivos contendo
        as informações da tabela da Natureza Jurídica.
        """
        schema_natureza_juridica = StructType([
                StructField('codigo',StringType(),True),
                StructField('descricao',StringType(),True),
                ])
        importador_spark(arquivo,separador=separador,schema=schema_natureza_juridica,sql_alias='natureza_juridica', previsualizacao=previsualizacao)
        return 'tabela de natureza juridica carregada com sucesso'

def importador_paises(arquivo, separador, previsualizacao):
        """
        Método para a importação dos arquivos contendo
        as informações da tabela dos Países.
        """
        schema_paises = StructType([
                StructField('codigo',StringType(),True),
                StructField('nome_do_pais',StringType(),True),
                ])
        importador_spark(arquivo,separador=separador,schema=schema_paises,sql_alias='paises', previsualizacao=previsualizacao)
        return 'tabela de países carregada com sucesso'

def importador_qualificacao_socios(arquivo, separador, previsualizacao):
        """
        Método para a importação dos arquivos contendo
        as informações da tabela de Qualificação dos Sócios.
        A tabela de qualificação dos sócios também pode ser usada
        para identificar a qualificação dos responsáveis pelo CNPJ.
        """
        schema_qualificacao_socios = StructType([
                StructField('codigo',StringType(),True),
                StructField('descricao',StringType(),True),
                ])
        importador_spark(arquivo,separador=separador,schema=schema_qualificacao_socios,sql_alias='qualificacao_socios', previsualizacao=previsualizacao)
        return 'tabela de qualificacao socios carregada com sucesso'

def importador_simples(arquivo, separador, previsualizacao):
        """
        Método para a importação dos arquivos contendo
        as informações da tabela do Simples Nacional.
        """
        schema_simples = StructType([
                StructField('cnpj_basico',StringType(),True),
                StructField('opcao_simples',StringType(),True),
                StructField('data_opcao_simples',StringType(),True),
                StructField('data_exclusao_simples',StringType(),True),
                StructField('opcao_simei',StringType(),True),
                StructField('data_opcao_simei',StringType(),True),
                StructField('data_exclusao_simei',StringType(),True)
                ])
        importador_spark(arquivo,separador=separador,schema=schema_simples,sql_alias='simples', previsualizacao=previsualizacao)
        return 'tabela do simples nacional carregada com sucesso'
