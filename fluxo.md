# Fluxo sudoeste - inicial

## Entrada obrigatoria

O fluxo `sudoeste - inicial` aceita somente 3 arquivos:

1. `base`
2. `recebimento`
3. `denodo`

O fluxo antigo de `sudoeste` foi substituido por esse modelo.
Nao existe mais dependencia do arquivo `relatorio de acionamentos`.
Tambem nao existe mais separacao entre `DIRETOS` e `INDIRETOS`.

## Regra geral

- A planilha principal e a de `recebimento`.
- A planilha `base` serve apenas para complementar informacoes e localizar com mais seguranca o registro correspondente.
- A planilha `denodo` serve apenas para identificar pagamentos via boleto e preencher `Protocolo` quando houver correspondencia segura.
- A falta de correspondencia na `base` ou na `denodo` nao deve interromper o processamento.

## Colunas usadas

### Base

Colunas obrigatorias:

- `ASSOCIADO`
- `CPF`
- `N do Contrato`
- `N Parcela`

Colunas opcionais aproveitadas na saida quando existirem:

- `AG`
- `Conta`

Classificacao do titulo/contrato na base:

- `Cartoes Master`
- `Cartao Visa Empresarial`
- `Atraso Cartao Visa`

Esses 3 valores sao tratados como `cartao`.

- `Inadimplencia Cheque Especial`
- `Inadimplencia Juros Adiantamento`

Esses 2 valores sao tratados como `chi`.

Contratos alfanumericos sao tratados como contrato de negociacao e comparados com normalizacao alfanumerica.
Exemplos:

- `C57020042-0` -> `C570200420`
- `C46220228-0` -> `C462202280`

### Recebimento

Colunas obrigatorias:

- `Associado`
- `Titulo`
- `Parcela`
- `Valor Titulo`
- `Historico`
- `Data`

Colunas opcionais aproveitadas na saida quando existirem:

- `AG`
- `Conta`
- `CPF/CNPJ`

Filtro obrigatorio:

- manter somente linhas com `Historico = 1, 2, 3, 4` ou vazio

Interpretacao do campo `Titulo`:

- `CHI` -> familia `chi`
- `MAS` e `CAR` -> familia `cartao`
- contrato como `C33822210-0` -> contrato normalizado

### Denodo

Colunas obrigatorias:

- `protocolo`
- `cpf_cnpj_formatado`
- `solucao_associada`

Uso:

- identificar boleto por `cpf_cnpj_formatado` + classificacao segura de `solucao_associada`
- preencher `Protocolo` somente quando houver correspondencia segura
- se houver mais de um protocolo possivel para a mesma chave, o protocolo fica vazio para evitar falso positivo

## Regras de match

### Recebimento x Base

O match nao pode usar apenas CPF.

Regras implementadas:

- contratos: exigem mesmo contrato normalizado e apoio de CPF ou associado; quando a parcela existe nos dois lados, divergencia de parcela bloqueia o match
- cartao: `MAS` e `CAR` do recebimento batem com a familia de cartao da base; o match exige CPF ou associado, e usa parcela como reforco quando disponivel
- chi: `CHI` do recebimento bate com a familia de cheque especial/juros adiantamento da base; o match exige CPF ou associado, e usa parcela como reforco quando disponivel
- outros textos: exigem mesma chave de titulo e pelo menos 2 fatores de apoio entre CPF, associado e parcela
- quando houver empate entre multiplas linhas da base com a mesma pontuacao, a linha do recebimento segue sem match para evitar falso positivo

### Recebimento/Base x Denodo

O protocolo de boleto e identificado por:

- CPF/CNPJ normalizado
- tipo classificado do titulo/contrato
- chave normalizada do titulo/contrato

Regras:

- contratos usam contrato normalizado
- cartao usa a familia `cartao`
- chi usa a familia `chi`
- somente CPF nao basta
- ausencia de match na denodo e valida e gera `Protocolo` vazio

## Saida final

O arquivo final sai em uma unica aba `Sudoeste Inicial` com estas colunas, nesta ordem:

1. `AG`
2. `Conta`
3. `Associado`
4. `CPF/CNPJ`
5. `Titulo`
6. `Parcela`
7. `Valor Título`
8. `Histórico`
9. `Data`
10. `Atraso`
11. `%receita`
12. `receita`
13. `Dt Ultimo Acionamento`
14. `Situação`
15. `Venc. Parcela`
16. `Protocolo`

Preenchimento atual:

- `AG`, `Conta`, `Associado`, `CPF/CNPJ`, `Titulo`, `Parcela`, `Valor Título`, `Histórico` e `Data` saem do recebimento com complemento da base quando necessario
- `Protocolo` so e preenchido quando a denodo confirma boleto com correspondencia segura
- `Atraso`, `%receita`, `receita`, `Dt Ultimo Acionamento`, `Situação` e `Venc. Parcela` existem no layout, mas permanecem vazios nesta fase
