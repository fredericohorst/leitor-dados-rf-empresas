# leitor-dados-rf-empresas

## Contexto

A receita federal disponibiliza mensalmente as bases de dados abertas para os dados cadastrais das empresas brasileiras [aqui](https://www.gov.br/receitafederal/pt-br/assuntos/orientacao-tributaria/cadastros/consultas/dados-publicos-cnpj).

Os arquivos são bastante grandes, exigindo um maior processamento e capacidade de disco. Por esse motivo desenvolvi uma solução bastante simples composta de:

1. Uma instrução bash que faz o download do arquivo. A instrução bash pode ser executada dentro do próprio notebook ou apenas no seu terminal de preferência aberto já na pasta do projeto `~/leitor-dados-rf-empresas/`.
2. Biblioteca `utils` contendo método para inicialização do spark e outro método para descompactar os arquivos.
3. Biblioteca `leitor` contendo métodos para a leitura e limpeza básica dos dados já descompactados da Receita Federal. 

Todos os métodos de como usar as bibliotecas acima estão dentro do notebook `exemplo_perfil_empresarios_br.ipynb`. Os métodos aqui listados seguem o seguinte fluxo:



## Download e unzip dos arquivos:

Para download dos arquivos, basta executar o comando `bash downloader.sh` dentro do terminal aberto na pasta deste projeto. Você pode executar esse comando dentro do notebook também, conforme o exemplo.

O download demora bastante, por serem arquivos pesados, é normal demorar um dia inteiro para fazer o download completo.

Para descompactar os arquivos, você precisará executar:
```
import utils
utils.descompactando(caminho_do_arquivo='caminho_dos_arquivos_zipados', tipo_arquivo='.zip',destino='destino_desejado')
```
E pronto :)

## Leitura dos arquivos:

Para carregar os dados para memória, basta executar as bibliotecas do leitor conforme o código abaixo.
```
import leitor
leitor.importador_socios(arquivo='caminho_dos_arquivos',separador=';',previsualizacao=True)
```

Os métodos do leitor são:
- `importador_socios`: designado às informações de sócios. SQL alias: `socios`.
- `importador_empresas`: designado às informações das empresas. SQL alias: `empresas`.
- `importador_estabelecimentos`: designado às informações dos estabelecimentos de cada empresa. SQL alias: `estabelecimentos`.
- `importador_motivo_situacao_cadastral`: dicionário contendo o motivo da situação cadastral. SQL alias: `situacao_cadastral`.
- `importador_municipios`: dicionário contendo os municípios. SQL alias: `municipios`.
- `importador_natureza_juridica`: dicionário contendo a natureza jurídica. SQL alias: `natureza_juridica`.
- `importador_paises`: dicionário contendo os países. SQL alias: `paises`.
- `importador_qualificacao_socios`: dicionário contendo a qualificação dos sócios e responsáveis. SQL alias: `qualificacao_socios`.
- `importador_simples`: informações complementares sobre o simples nacional. SQL alias: `simples`.

Cada método irá ler os dados, aplicar o schema correto e carregar os dados para a memória contendo o *alias* previamente citado.

O método conta também com um argumento de `previsualizacao` que, quando `True` ele irá mostrar 10 linhas da base e, quando falso, não irá mostrar nada.

Por fim, para consultar os dados, você só precisará usar comandos SQL, como o exemplo abaixo:

```
spark_session, sqlContext, sc = utils.spark_configs(memory='4G')
query = """
    SELECT *
    FROM empresas
    """
sqlContext.sql(query)
```
Assim você pode executar consultas para cada uma das bases de maneira fácil e sem necessariamente saber python ou spark avançado, basta usar consultas simples em SQL.