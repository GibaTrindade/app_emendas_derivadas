# Especificacao do Algoritmo `gerar_ro.py`

## Objetivo

O script `gerar_ro.py` processa um arquivo CSV de remanejamentos e separa os registros em duas saidas:

- `df_RO.csv`: movimentos classificados como remanejamento orcamentario (`RO`)
- `df_CA.csv`: movimentos classificados como credito adicional (`CA`)

A regra de negocio considerada por este documento e:

- `CODACAO` identifica a acao orcamentaria
- `RO`: a movimentacao e tratada como remanejamento quando origem e destino pertencem a mesma acao, ou seja, ao mesmo `CODACAO`
- `CA`: a movimentacao e tratada como credito adicional quando nao consegue ser conciliada dentro do mesmo `CODACAO`

## Resumo Executivo

O script classifica as movimentacoes com base no `CODACAO`, que representa a acao orcamentaria. A logica geral e:

1. separa linhas de origem (`OD = O`) e destino (`OD = D`)
2. agrupa logicamente por `CODACAO`
3. considera como candidatas a `RO` apenas as linhas cujo `CODACAO` aparece tanto em origens quanto em destinos
4. dentro de cada `CODACAO`, aloca para `RO` o volume financeiro que consegue ser compensado entre origens e destinos
5. toda sobra financeira, alem dos `CODACAO` sem par, vai para `CA`

Portanto:

- a regra "mesma acao" e representada por `mesmo CODACAO`
- a regra de classificacao nao depende de `SUBACAO`
- `CA` corresponde ao que nao fecha dentro do mesmo `CODACAO`

## Entradas

O script le o arquivo:

- `Remanejamentos 2025-08-utf8.csv`

Separador:

- `;`

Colunas relevantes para o algoritmo:

- `OD`: identifica se a linha e origem (`O`) ou destino (`D`)
- `CODACAO`: codigo da acao orcamentaria
- `VALOR`: valor financeiro da movimentacao
- `SUBACAO`: coluna informativa, preservada nas saidas, mas nao usada como criterio de classificacao

Outras colunas sao preservadas e copiadas para as saidas.

## Saidas

O script gera:

- `df_RO.csv`
- `df_CA.csv`

Formato de exportacao:

- separador `;`
- decimal `,`
- encoding `utf-8-sig`

Antes da exportacao, o script ainda aplica dois pos-processamentos:

- consolidacao de linhas iguais no mesmo lado
- compensacao entre linhas iguais de origem e destino, mantendo apenas a diferenca liquida

## Fluxo Geral do Algoritmo

## 1. Leitura e limpeza minima

O script carrega o CSV com `pandas.read_csv`.

Depois faz duas normalizacoes:

- `OD` e convertido para texto, tem espacos removidos e fica em maiusculas
- `CODACAO` e convertido para texto e tem espacos removidos

Tambem cria a coluna auxiliar `__idx`, contendo a ordem original das linhas no arquivo.

Objetivo dessa coluna:

- preservar a ordem de entrada como criterio secundario de processamento

## 2. Separacao entre origens e destinos

Os dados sao divididos em dois subconjuntos:

- `df_origens`: linhas com `OD == "O"`
- `df_destinos`: linhas com `OD == "D"`

Em seguida, cada subconjunto e ordenado por:

1. `CODACAO`
2. `__idx`

Depois disso, `__idx` e removida.

E importante notar que o script nao agrega valores por chave. Ele nao faz `group by` com soma. Ele apenas:

- separa as linhas
- ordena
- processa sequencialmente

## 3. Identificacao de acoes com par e sem par

O script tira uma copia base de origens e destinos:

- `df_origens_base`
- `df_destinos_base`

Depois monta dois conjuntos:

- `cod_o`: todos os `CODACAO` presentes nas origens
- `cod_d`: todos os `CODACAO` presentes nos destinos

A partir disso, calcula:

- `cod_somente_origem = cod_o - cod_d`
- `cod_somente_destino = cod_d - cod_o`

Esses casos representam acoes que aparecem apenas de um lado.

As linhas correspondentes sao separadas em:

- `df_only_o`: acoes so de origem
- `df_only_d`: acoes so de destino

Interpretacao funcional:

- se uma acao nao possui origem e destino dentro do mesmo `CODACAO`, ela nao pode ser conciliada como `RO` pela logica atual
- por isso, essas linhas vao integralmente para `CA` no final

## 4. Restricao do universo de remanejamento

O script calcula a intersecao:

- `codacoes_comuns = CODACAO presentes em origens e destinos`

Depois filtra:

- `df_origens`: fica apenas com origens de `CODACAO` comum
- `df_destinos`: fica apenas com destinos de `CODACAO` comum

Esse passo define o universo potencial de `RO`.

Na pratica, a regra implementada e:

- somente pode haver `RO` quando existe entrada de origem e destino no mesmo `CODACAO`

## 5. Inicializacao dos acumuladores

Sao criados dois dataframes vazios:

- `df_RO`: acumula o que fecha como remanejamento
- `df_CA`: acumula o que sobra como credito adicional

Eles usam a estrutura de colunas de `df_origens`.

## 6. Processamento por `CODACAO`

O loop principal percorre todos os `CODACAO` unicos do arquivo original.

Para cada `acao`:

- `df_o`: linhas de origem dessa acao
- `df_d`: linhas de destino dessa acao

Depois calcula:

- `soma_origens`
- `soma_destinos`

Ambos sao obtidos convertendo `VALOR` para numerico, tratando erros como zero.

### Interpretacao central

Para cada `CODACAO`, o script tenta descobrir qual parte do fluxo financeiro pode ser considerada interna a essa mesma acao.

Essa parcela interna vai para `RO`.

Qualquer excedente, de origem ou de destino, vai para `CA`.

## 7. Caso A: origem menor ou igual ao destino

Condicao:

- `soma_origens <= soma_destinos`

Leitura funcional:

- tudo que saiu dessa acao pode ser absorvido por destinos da mesma acao
- portanto, todas as origens entram em `RO`
- apenas uma parte dos destinos entra em `RO`
- o excedente dos destinos vai para `CA`

### Passo a passo

1. todas as linhas de `df_o` sao adicionadas em `df_RO`
2. a variavel `diff_origem` recebe `soma_origens`
3. o script percorre os destinos em ordem

Para cada linha de destino:

- se `VALOR destino > diff_origem`:
  - divide a linha de destino em duas partes
  - uma parte, no valor de `diff_origem`, vai para `RO`
  - a sobra da mesma linha vai para `CA`
  - todas as linhas seguintes de destino tambem vao para `CA`
  - encerra o processamento dessa acao

- se `VALOR destino == diff_origem`:
  - a linha fecha exatamente o total de origem
  - essa linha vai para `RO`
  - todas as linhas seguintes de destino vao para `CA`
  - encerra o processamento dessa acao

- se `VALOR destino < diff_origem`:
  - a linha inteira vai para `RO`
  - `diff_origem` e reduzido pelo valor desse destino
  - o loop continua

### Resultado desse caso

- `RO` recebe:
  - todas as origens da acao
  - apenas a parcela de destinos necessaria para compensar essas origens

- `CA` recebe:
  - o excedente de destinos que nao foi necessario para fechar as origens

## 8. Caso B: origem maior que destino

Condicao:

- `soma_origens > soma_destinos`

Leitura funcional:

- todos os destinos dessa acao podem ser absorvidos por origens da mesma acao
- portanto, todos os destinos entram em `RO`
- apenas uma parte das origens entra em `RO`
- o excedente das origens vai para `CA`

### Passo a passo

1. a variavel `diff_destino` recebe `soma_destinos`
2. o script percorre as origens em ordem

Para cada linha de origem:

- se `VALOR origem < diff_destino`:
  - a linha inteira vai para `RO`
  - `diff_destino` e reduzido

- se `VALOR origem > diff_destino`:
  - a linha e dividida em duas partes
  - uma parte, no valor de `diff_destino`, vai para `RO`
  - a sobra da mesma linha vai para `CA`
  - todas as linhas seguintes de origem vao para `CA`
  - encerra o processamento dessa acao

- se `VALOR origem == diff_destino`:
  - a linha fecha exatamente o total de destino
  - essa linha vai para `RO`
  - todas as linhas seguintes de origem vao para `CA`
  - encerra o processamento dessa acao

3. ao final, todos os destinos de `df_d` sao adicionados em `df_RO`

### Resultado desse caso

- `RO` recebe:
  - todas as linhas de destino da acao
  - apenas a parcela de origens necessaria para compensar os destinos

- `CA` recebe:
  - o excedente de origem que nao encontrou compensacao dentro do mesmo `CODACAO`

## 9. Inclusao dos casos sem par

Depois do loop principal, o script concatena em `df_CA`:

- `df_only_o`
- `df_only_d`

Ou seja, qualquer `CODACAO` presente apenas em um dos lados vai integralmente para credito adicional.

## 10. Gravacao dos arquivos finais

Antes de gravar os arquivos finais, o script aplica dois ajustes sobre `df_RO` e `df_CA`.

### 10.1 Consolidacao de linhas iguais no mesmo lado

O script agrupa linhas com os mesmos campos:

- `OD`
- `CODACAO`
- `SUBACAO`
- `GD`
- `MA`
- `MUNICIPIO`
- `OBJETO`

Quando a chave e igual:

- `VALOR` e somado
- `AUTOR` e `NUMERO` sao concatenados quando houver mais de um valor distinto

Essa etapa evita que varias linhas equivalentes do mesmo lado aparecam separadas no resultado final.

### 10.2 Compensacao entre origem e destino iguais

Depois da consolidacao, o script verifica se existe uma linha de origem e uma linha de destino com a mesma chave:

- `CODACAO`
- `SUBACAO`
- `GD`
- `MA`
- `MUNICIPIO`
- `OBJETO`

Observe que, nessa etapa, `OD` nao faz parte da chave, porque o objetivo e justamente comparar origem com destino.

Regra:

- se existir apenas origem, a linha permanece
- se existir apenas destino, a linha permanece
- se existirem origem e destino para a mesma chave, o script calcula a diferenca entre os valores

Resultado:

- se `destino > origem`, a linha final permanece apenas como destino, com `VALOR = destino - origem`
- se `origem > destino`, a linha final permanece apenas como origem, com `VALOR = origem - destino`
- se `origem == destino`, as duas linhas se anulam e desaparecem do resultado

Essa etapa remove pares redundantes em que a mesma configuracao aparece nos dois lados.

### Exemplo conceitual da compensacao

Considere estas duas linhas ja consolidadas:

- origem `EN91` = `621.800`
- destino `EN91` = `680.000`

Como a chave tecnica da linha e a mesma e o destino e maior:

- a origem e anulada
- o destino permanece com `58.200`

Esse e exatamente o comportamento esperado quando a mesma linha aparece nos dois lados e interessa apenas a diferenca liquida.

### 10.3 Gravacao dos arquivos finais

Por fim, o script grava:

- `df_RO.csv`
- `df_CA.csv`

## Regra de Classificacao Implementada

Em termos objetivos, a classificacao implementada pode ser descrita assim:

- `RO` = parcela das movimentacoes que consegue ser compensada entre origem e destino dentro do mesmo `CODACAO`
- `CA` = toda movimentacao que sobra apos essa compensacao, incluindo acoes sem par

Essa regra e coerente com a definicao operacional adotada neste processo:

- `CODACAO` e o identificador da acao
- mesmo `CODACAO` implica mesma acao
- o que fecha dentro da mesma acao e `RO`
- o que nao fecha dentro da mesma acao e `CA`
- depois da classificacao, linhas tecnicamente identicas em lados opostos sao liquidadas pela diferenca

## Exemplo Conceitual

Suponha um `CODACAO = 1000`.

Origens:

- O1 = 100
- O2 = 50

Destinos:

- D1 = 80
- D2 = 90

Totais:

- origem = 150
- destino = 170

Como a origem e menor que o destino:

- todas as origens vao para `RO` = 150
- destinos entram em ordem ate completar 150
- `D1 = 80` vai inteiro para `RO`
- `D2` e dividido:
  - `70` vai para `RO`
  - `20` vai para `CA`

Resultado:

- `RO` total = 150 em origens + 150 em destinos conciliados
- `CA` total = 20 de destino excedente

## Premissas do Script

- `CODACAO` representa a acao orcamentaria
- linhas de mesma acao podem ser conciliadas independentemente da `SUBACAO`
- a ordem das linhas influencia o resultado quando existe necessidade de fracionar registros
- o fracionamento acontece sempre na primeira linha que ultrapassa o saldo a compensar

## Caracteristicas da Implementacao

## 1. Nao existe pareamento individual entre movimentos durante a classificacao principal

O algoritmo trabalha por massa financeira agregada dentro de cada `CODACAO`, e nao por relacionamento direto entre uma origem e um destino especificos.

## 2. Existe compensacao final entre linhas tecnicamente identicas

Depois que `RO` e `CA` sao montados, o script aplica uma liquidacao por diferenca quando a mesma chave aparece em origem e destino.

## 3. A classificacao de `CA` e residual

`CA` e identificado como o conjunto de valores que nao conseguiram ser conciliados dentro do mesmo `CODACAO`.

## 4. O resultado depende da ordem das linhas

Quando uma linha precisa ser dividida, a escolha de qual linha sera parcialmente alocada para `RO` depende da ordem do arquivo original.

## 5. Possivel sensibilidade ao formato de `VALOR`

Durante o processamento, o script usa `float(row["VALOR"])` em pontos especificos. Isso assume que os valores ja estejam em formato numerico compativel naquele momento.

## Relacao Entre Regra de Negocio e Script Atual

O script esta alinhado com a regra adotada neste processo:

- `CODACAO` representa a acao
- movimentacoes conciliadas dentro do mesmo `CODACAO` sao classificadas como `RO`
- valores que nao encontram compensacao dentro do mesmo `CODACAO` sao classificados como `CA`
- ao final, linhas equivalentes de origem e destino sao liquidadas pela diferenca, evitando duplicidade tecnica no resultado

Assim, a coluna `SUBACAO` nao precisa participar da decisao para que a classificacao ocorra corretamente segundo essa regra.
