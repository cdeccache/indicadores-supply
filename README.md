## O id da view_oc vai ligar no processocompra_id da view_item_oc

O id da OC vai ligar no processocompra_id da ITEM_OC

O id da RM vai ligar com o ProcessoCompra_Id do Item da RM

O id do Mapa de Coleta liga com o campo MapaColeta_Id na RM

O id do Mapa de Coleta liga com o campo mapacoleta_Id na OC

O ProcessoCompra_Id da tabela Aprovador liga com o campo id da OC

O id da RC vai ligar com o ProcessoCompra_Id do Item da RC


## UI http://localhost:4040/


## SAVING

-- Backup do dia 27/05
Quando não tem o item o valor fica zero, por conseguinte, o valor da cotação ficar menor que a do outro fornecedor que tem todos os itens


## Instruções do Saving

Vou descrever aqui as ações que fiz:
- Fiz um filtro na coluna NumeroMC para pegar apenas um caso, o mapa escolhido foi 2075 (NumeroMC=2075)
- Fiz um PROCV da tabela de OC (coluna numMpa) e a tabela do mapa (coluna NumeroMC) para trazer a informação do vaalor total OC (coluna TotalOC)
- Filtro na coluna Sequencia Negociação = 1
- Somar o valor da coluna TotalItem para cada fornecedor diferente
- Selecionar o menor valor entre as somas do passo anterior
- Como nesse caso o mapa gerou mais de uma OC eu tive que somar os valores do campo TotalOC (informacao que veio da tabela de OC)
- Logo basta agora apenas calcular o saving = soma TotalOC - menor valor da negociação 1
- E a porcentagem do saving = o valor calculado no passo anterior/ menor valor da negociação 1


https://docs.google.com/spreadsheets/d/1EhXiejABFkrx1IxB2IGe3w87UYHUE11vznn686oUEmo/edit#gid=1437848408


## Observação

- Pode ter mapa de fornecedor sem cadastro - Agrupar por nome

- Desconsiderar SEEL1, SEEL2 na cotação - são valores fictícios

- Verificar as quantidades dos itens - 


root
 |-- Id: long (nullable = true)
 |-- DataAberturaNegociacao: timestamp (nullable = true)
 |-- NumeroMC: integer (nullable = true)
 |-- NegociacaoId: long (nullable = true)
 |-- CodigoFornecedor: string (nullable = false)
 |-- NomeFornecedor: string (nullable = true)
 |-- sum(TotalItem): decimal(38,9) (nullable = true)