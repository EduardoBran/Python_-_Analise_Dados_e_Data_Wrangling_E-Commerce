####  Big Data Real-Time Analytics com Python e Spark  ####

# Configurando o diretório de trabalho
setwd("~/Desktop/DataScience/CienciaDeDados/2.Big-Data-Real-Time-Analytics-com-Python-e-Spark/4.Analise_Exploratoria_de_Dados")
getwd()



## Importando Pacotes
library(readxl)         # carregar arquivos
library(dplyr)          # manipula dados
library(tidyr)          # manipula dados (funcao pivot_longer)
library(ggplot2)        # gera gráficos
library(patchwork)      # unir gráficos
library(corrplot)       # mapa de Correlação




#############################             Estudo de Caso             #############################


###  Análise Exploratória e Data Wrangling Para E-Commerce Analytics



## Objetivo:

# - Este Estudo de Caso traz uma série de atividades em uma das tarefas mais importantes em Ciência de Dados, a Análise Exploratória.

# - Na Parte 1 do Estudo de Caso,a Análise Exploratória traz os detalhes técnicos com análise estatística, visualização de dados, interpretação de gráficos
#   e tabelas, análise univariada e bivariada e relatório de conclusão.

# - Na Parte 2 o foco é na Análise Exploratória para responder perguntas de negócio, onde os dados são manipulados através de Data Wrangling com Pandas e
#   analisados por diferentes perspectivas. Customização de gráficos é outro tema abordado durante as aulas.

# Todo Estudo de Caso é no contexto de um problema de negócio em E-Commerce Analytics.


### Definição do Problema

# - Uma empresa internacional de comércio eletrônico (E-commerce)que vende produtos eletrônicos deseja descobrir informações importantes de seu banco de dados
#   de clientes.

# - Os produtos ficam armazenados em um armazém na sede da empresa. Após concluir a compra no web site da empresa, o cliente recebe o produto em casa, em qualquer 
#   parte do mundo. Os produtos são enviados de Navio, Avião ou Caminhão, dependendo da região de entrega.

# - Em cada compra o cliente pode receber um desconto dependendo do peso do produto comprado. Cada cliente pode fazer chamadas ao suporte da empresa no caso de 
#   dúvidas ou problemas e após receber o produto o cliente pode deixar uma avaliação sobre a experiência de compra. O único dado pessoal sobre o cliente que está
#   disponível é o gênero.

# - Nosso trabalho neste Estudo de Caso é explorar os dados, compreender como estão organizados, detectar eventuais problemas e analisar os dados por diferentes
#   perspectivas.

# Trabalharemos com dados fictícios que representam dados reais de uma empresa de E-Commerce. Os dados estão disponíveis na pasta "dados".




####   PARTE 1   ####


## Carregando os Dados
df <- data.frame(read.csv2("dados/dataset.csv", sep = ","))
head(df)


## Realizando Análise Inicial (Sumário Estatístico, Veriricação de Valores NA, '' e especiais)

analise_inicial <- function(dataframe_recebido) {  # para encotrar linhas com caracter especial, vá para o fim do script
  # Sumário
  print(dim(dataframe_recebido))
  print(str(dataframe_recebido))
  print(summary(dataframe_recebido))
  cat("\n\n\n####  VERIFICANDO VALORES NA  ####\n\n")
  valores_na <- colSums(is.na(dataframe_recebido))
  if(any(valores_na > 0)) {
    cat("\n-> Colunas com valores NA:\n\n")
    print(valores_na[valores_na > 0])
  } else {
    cat("\n-> Não foram encontrados valores NA.\n")
  }
  cat("\n\n\n####  VERIFICANDO VALORES VAZIOS ''  ####\n\n")
  valores_vazios <- sapply(dataframe_recebido, function(x) sum(x == ""))
  if(any(valores_vazios > 0)) {
    cat("\n-> Colunas com valores vazios \"\":\n\n")
    print(valores_vazios[valores_vazios > 0])
  } else {
    cat("\n-> Não foram encontrados valores vazios \"\".\n")
  }
  cat("\n\n\n####  VERIFICANDO VALORES COM CARACTERES ESPECIAIS  ####\n\n")
  caracteres_especiais <- sapply(dataframe_recebido, function(x) {
    sum(sapply(x, function(y) {
      if(is.character(y) && length(y) == 1) {
        any(charToRaw(y) > 0x7E | charToRaw(y) < 0x20)
      } else {
        FALSE
      }
    }))
  })
  if(any(caracteres_especiais > 0)) {
    cat("\n-> Colunas com caracteres especiais:\n\n")
    print(caracteres_especiais[caracteres_especiais > 0])
  } else {
    cat("\n-> Não foram encontrados caracteres especiais.\n")
  }
}

analise_inicial(df)


## Modificando todas as variáveis do tipo chr para factor
df <- dplyr::mutate_if(df, is.character, as.factor)
str(df)


## Dividindo o Dataframe em Variáveis Categoricas, Variáveis Numéricas e Variável Alvo
names(df)

# Dataframe somente com as variáveis categóricas
df_categoricas <- df %>% 
  select(where(is.factor))

# Dataframe somente com as variáveis numéricas
df_numericas <- df %>% 
  select(where(is.numeric)) %>% 
  select(-ID, -entregue_no_prazo)

# Dataframe somente com a variável alvo/target "entregue_no_prazo"
df_target <- df %>%
  select(entregue_no_prazo) %>% 
  mutate(entregue_no_prazo = factor(entregue_no_prazo))



#### Explorando as Variáveis Numéricas

## Resumo Estatístico
summary(df_numericas)

# As colunas numero_chamadas_cliente, avaliacao_cliente e custo_produto parecem ter uma distribuição bastante simétrica (média e mediana não são tão diferentes).
# As colunas compras_anteriores e desconto parecem estar inclinadas para a direita (Média maior do que a Mediana).
# A coluna peso_gramas parece estar mais inclinada para a esquerda (Média menor do que a Mediana).


## Visualizando Através de Gráficos

# Transformar o dataframe para o formato longo apropriado para o ggplot
df_long_num <- df_numericas %>% 
  pivot_longer(
    cols = everything(), 
    names_to = "variable", 
    values_to = "value"
  )


# Box Plot
ggplot(data = df_long_num, aes(x = variable, y = value, fill = variable)) + 
  geom_boxplot(width = 0.25) +                                                       # Ajusta a largura dos boxes
  scale_fill_manual(values = rep("magenta", length(unique(df_long_num$variable)))) + 
  theme_minimal() + 
  theme(legend.position = "none", 
        axis.text.x = element_text(angle = 45, vjust = 0.5)) + 
  labs(x = NULL, y = "Valores") + 
  facet_wrap(~variable, scales = "free")

# Histograma  com linha de densidade para cada variável
ggplot(df_long_num, aes(x = value)) +
  geom_histogram(aes(y = ..density..), binwidth = 1, colour = "black", fill = "gray") +
  geom_density(alpha = .2, fill = "#FF6666") +
  facet_wrap(~ variable, scales = "free") +
  theme_minimal() +
  labs(title = "Histograma com Linha de Densidade para Cada Variável",
       x = "Valor",
       y = "Densidade")

rm(df_long_num)


## Mapa de Correlação (somente variáveis numéricas)

cor(df_numericas)
corrplot(cor(df_numericas),
         method = "color",
         type = "upper",
         addCoef.col = 'springgreen2',
         tl.col = "black",
         tl.srt = 45)




#### Explorando as Variáveis Categóricas

## Resumo estatístico
summary(df_categoricas)


## Visualizando Através de Gráficos
names(df_categoricas)

# Gráfico de Barras
grafico_corredor <- ggplot(df_categoricas, aes(x = corredor_armazem)) +
  geom_bar() +
  labs(title = "Distribuição de corredor_armazem")

# Gráfico para modo_envio
grafico_modo_envio <- ggplot(df_categoricas, aes(x = modo_envio)) +
  geom_bar() +
  labs(title = "Distribuição de modo_envio")

# Gráfico para prioridade_produto
grafico_prioridade <- ggplot(df_categoricas, aes(x = prioridade_produto)) +
  geom_bar() +
  labs(title = "Distribuição de prioridade_produto")

# Gráfico para genero
grafico_genero <- ggplot(df_categoricas, aes(x = genero)) +
  geom_bar() +
  labs(title = "Distribuição de genero")

# Colocando os gráficos em um único plot
grafico_completo <- (grafico_corredor | grafico_modo_envio) / (grafico_prioridade | grafico_genero)
grafico_completo

rm(grafico_corredor, grafico_modo_envio, grafico_prioridade, grafico_genero, grafico_completo)



head(df_categoricas)
head(df_target)
str(df_categoricas)
str(df_target)


## Explorando a Variável Alvo

# Resumo Estatístico
summary(df_target)

# Visualizando Através de Gráficos
ggplot(df_target, aes(x = entregue_no_prazo)) +
  geom_bar() +
  labs(title = "Distribuição de entregue_no_prazo")



#### Criando Gráficos para Análise das Variávels Categóricas x Variável Target

## Visualizando Através de Gráficos
names(df_categoricas)

# Gráfico de barras empilhadas para corredor_armazem
grafico_corredor <- ggplot(cbind(df_categoricas, df_target), aes(x = corredor_armazem, fill = entregue_no_prazo)) +
  geom_bar(position = "dodge") +
  scale_fill_manual(values = c("0" = "blue", "1" = "orange")) +
  labs(x = "Corredor do Armazém", y = "Contagem") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Gráfico de barras empilhadas para modo_envio
grafico_modo_envio <- ggplot(cbind(df_categoricas, df_target), aes(x = modo_envio, fill = entregue_no_prazo)) +
  geom_bar(position = "dodge") +
  scale_fill_manual(values = c("0" = "blue", "1" = "orange")) +
  labs(x = "Modo de Envio", y = "Contagem") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Gráfico de barras empilhadas para prioridade_produto
grafico_prioridade <- ggplot(cbind(df_categoricas, df_target), aes(x = prioridade_produto, fill = entregue_no_prazo)) +
  geom_bar(position = "dodge") +
  scale_fill_manual(values = c("0" = "blue", "1" = "orange")) +
  labs(x = "Prioridade do Produto", y = "Contagem") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Gráfico de barras empilhadas para genero
grafico_genero <- ggplot(cbind(df_categoricas, df_target), aes(x = genero, fill = entregue_no_prazo)) +
  geom_bar(position = "dodge") +
  scale_fill_manual(values = c("0" = "blue", "1" = "orange")) +
  labs(x = "Gênero", y = "Contagem") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

# Combinando os gráficos em um único plot
grafico_completo <- (grafico_corredor | grafico_modo_envio) / (grafico_prioridade | grafico_genero)
grafico_completo

rm(grafico_corredor, grafico_modo_envio, grafico_prioridade, grafico_genero, grafico_completo)



#### Conclusão da Parte 1:

## Algumas das coisas que encontramos neste conjunto de dados são:
  
# - Os dados parecem válidos e não há defeitos maiores/significativos.
# - Existem algumas distribuições que são um pouco *assimétricas*, isso deve ser lembrado se usarmos modelos que exijam a suposição de uma
#   distribuição normal.
# - Não detectamos problemas de multicolinearidade.
# - Alguns *recursos* parecem completamente não correlacionados.
# - Dos recursos categóricos, `modo_envio` , `corredor_armazem` e `importancia_produto` parecem úteis para prever a variável target.




####   PARTE 2   ####

# - Esta parte 2 terá como foco perguntas de negócio.


#### Pergunta 1:
#### Os atrasos nas entregas estão igualmente distribuídos pelos modos de envio? Há diferenças discrepantes?


## Criando Tabelas de Agragação

# Tabela de agregação 1 (agrupamento através de 'entregue_no_prazo' e 'modo_envio' e retornando a quantidade de valores únicos)
df_group1 <- df %>% 
  group_by(entregue_no_prazo, modo_envio) %>% 
  summarise(total = n(), .groups = "drop")
df_group1

# Tabela de agregação 2 (agrupamento através de 'entregue_no_prazo' e retornando a quantidade de valores únicos)
df_group2 <- df %>% 
  group_by(entregue_no_prazo) %>% 
  summarise(total = n(), .groups = "drop")
df_group2

# Tabela de agregação 3 (concatenando Tabela de Agregação 1 e Tabela de Agregação 2 com base na coluna "entregue_no_prazo")
df_group3 <- merge(df_group1, df_group2, by = 'entregue_no_prazo')
df_group3

# Tabela de agregação 4 (agrupamento através de 'modo_envio'  e retornando a quantidade de valores únicos)
df_group4 <- df %>% 
  group_by(modo_envio) %>% 
  summarise(total = n(), .groups = "drop")
df_group4

# Tabela de agregação 5 (concatenando Tabela de Agregação 1 e Tabela de Agregação 4 com base na coluna "modo_envio")
df_group5 <- merge(df_group1, df_group4, by = 'modo_envio')
df_group5


## Usando Tabela de Agregação 5

# Criando nova coluna Percentual
df_group5$Percentual <- (df_group5$total.x / df_group5$total.y) * 100
df_group5

# Renomear as colunas em df_group5
df_group5 <- df_group5 %>%
  rename(Status_Entrega_Prazo = entregue_no_prazo,
         Modo_De_Envio = modo_envio,
         Total_Por_Categoria = total.x,
         Total_Geral = total.y,
         Percentual = Percentual)
df_group5

# Transformando para factor a coluna Status_Entrega_Prazo
df_group5$Status_Entrega_Prazo <- as.factor(df_group5$Status_Entrega_Prazo)
str(df_group5)


rm(df_group1, df_group2, df_group3, df_group4)


## Gerando Gráficos Para Responder a Pergunta de Negócio

# Gráfico 1 - Análise em Valores Absolutos (utilizando o dataframe df)

# Gráfico de barras empilhadas para modo_envio
grafico_modo_envio_abs <- ggplot(cbind(df_categoricas, df_target), aes(x = modo_envio, fill = entregue_no_prazo)) +
  geom_bar(position = "dodge") +
  scale_fill_manual(values = c("0" = "blue", "1" = "orange")) +
  labs(title = "Entregas com Base no Modo de Envio (Absoluto)",
       x = "Modo de Envio", y = "Contagem", fill = "Status de Entrega no Prazo") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        plot.title = element_text(hjust = 0.5))
#grafico_modo_envio_abs


# Gráfico 2 - Análise em Valores Percentuais (utilizando o dataframe df_group5)

grafico_modo_envio_per <- ggplot(df_group5, aes(x = Modo_De_Envio, y = Percentual, fill = Status_Entrega_Prazo)) +
  geom_bar(stat = "identity", position = "dodge") +
  scale_fill_manual(values = c("0" = "red", "1" = "green")) +
  labs(title = "Entregas com Base no Modo de Envio (Percentual)",
       x = "Modo de Envio", y = "Percentual", fill = "Status de Entrega no Prazo") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1),
        plot.title = element_text(hjust = 0.5))
#grafico_modo_envio_per


# Combinando os gráficos em um único plot
grafico_completo <- grafico_modo_envio_abs | grafico_modo_envio_per
grafico_completo

rm(grafico_modo_envio_abs, grafico_modo_envio_per)


# -> Resposta: as respostas estão nos gráficos.



#### Pergunta 2:
#### Há diferença significativa no atraso das entregas quando o produto tem prioridade baixa ou média?



































# Alternativa para encontrar a linha com caractere especial (somente 1 linha)
for (i in 1:nrow(df2)) {
  if (any(charToRaw(df2$installment[i]) > 0x7E) | any(charToRaw(df2$installment[i]) < 0x20)) {
    cat("A linha com o caractere especial na coluna 'installment' é:", i, "\n")
    break
  }
}
# Encontrar linhas QUE NÃO TEM caracteres especiais na coluna ESPECÍFICA (para mais de 1 linha com caracteres especiais)
linhas_com_caracteres_especiais <- which(sapply(df2$int_rate, function(x) grepl("[^\\x20-\\x7E]", x)))

# Exibir as linhas que contêm caracteres especiais, se houver
if (length(linhas_com_caracteres_especiais) > 0) {
  cat("Foram encontrados valores com caracteres especiais nas linhas:", paste(linhas_com_caracteres_especiais, collapse=", "), "\n")
} else {
  cat("Não foram encontrados valores com caracteres especiais.\n")
}
