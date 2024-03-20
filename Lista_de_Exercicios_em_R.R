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




####  Lista de Exercícios  ####

# -> Respondas as questões.
# -> # O dataset contendo registros de corridas de táxi.



## Carregando os dados
df <- data.frame(read.csv2("dados/dataframe.csv", sep = ","))
df$Data <- as.Date(df$Data)
df$Distancia <- as.numeric(df$Distancia)
df$Segundos <- as.numeric(df$Segundos)
df$Minutos <- as.numeric(df$Minutos)
df$Min_Por_Km <- as.numeric(df$Min_Por_Km)
head(df)
str(df)


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


# Substituindo valores NA por 0 em todas as colunas numéricas e vazias ''
df <- df %>%
  mutate_if(is.numeric, ~if_else(is.na(.), 0, .))




#### Exercício 1 - Qual o valor máximo da coluna Minutos?
max(df$Minutos)




#### Exercício 2 - Qual o valor mínimo de distância acima de 2.0?
df %>% 
  filter(Distancia > 2.0) %>% 
  summarise(Valor_Minimo = min(Distancia))





#### Exercício 3 - Crie um plot com a frequência acumulada da coluna Distancia.

df$Distancia
str(df$Distancia)


# Calculando a frequência para cada valor único de Distancia
freq <- table(df$Distancia)
freq

# Calculando a frequência acumulada
freq_acumulada <- as.data.frame(cumsum(freq))
freq_acumulada

# Calculando a frequência acumulada normalizado
#freq_acumulada_nor <- prop.table(as.matrix(freq_acumulada))
freq_acumulada_nor <- as.data.frame(cumsum(freq) / sum(freq))
freq_acumulada_nor


# Gráfico de Linha
plot(as.numeric(rownames(freq_acumulada_nor)), freq_acumulada_nor$`cumsum(freq)/sum(freq)`,
     type = "l", col = "blue", xlab = "Distância", ylab = "Frequência Acumulada Normalizada")

ggplot(freq_acumulada_nor, aes(x = as.numeric(rownames(freq_acumulada_nor)), y = `cumsum(freq)/sum(freq)`)) +
  geom_line(color = "blue") +
  labs(x = "Distância", y = "Frequência Acumulada Normalizada") +
  theme_minimal()

rm(freq, freq_acumulada, freq_acumulada_nor)




#### Exercício 4 - Qual o dia da semana no índice de posição zero?

# Extrair a data da primeira linha do DataFrame
data_indice_zero = as.Date(df$Data[1])

# Determinar o dia da semana
dia_da_semana <- weekdays(data_indice_zero)
dia_da_semana

rm(data_indice_zero, dia_da_semana)

# Respota -> Domingo






#### Exercício 5 - Qual o dia da semana nos índices nas 5 primeiras posições?

for(i in 1:5) {
  data_indice <- as.Date(df$Data[i])
  dia_da_semana <- weekdays(data_indice)
  print(dia_da_semana)
}

rm(i, data_indice, dia_da_semana)

# Respota -> Domingo, Segunda, Terça, Quinta e Sexta




#### Exercício 6 - Extraia todos os dias da semana (em formato texto) e insira em uma nova coluna no dataframe df.

for(i in 1:nrow(df)) {
  data_indice <- as.Date(df$Data[i])
  dia_da_semana <- weekdays(data_indice)
  df$Dias_da_Semana[i] <- dia_da_semana
}
head(df)

rm(i, data_indice, dia_da_semana)





#### Exercício 7 - Crie um gráfico de barras com o total da distância percorrida em cada dia da semana.

# Calcular Distância Percorrida Por Cada Dia da Semana

df_group <- df %>% 
  group_by(Dias_da_Semana) %>% 
  summarise(Total_Percorrido = sum(Distancia))
df_group

# Definir a ordem desejada dos dias da semana
ordem_dias <- c("domingo", "segunda", "terça", "quarta", "quinta", "sexta", "sábado")

# Reordenar o dataframe df_group de acordo com a ordem desejada
df_group <- df_group[match(ordem_dias, df_group$Dias_da_Semana), ]
df_group


# Gráfico Cru
barplot(df_group$Total_Percorrido, names.arg = df_group$Dias_da_Semana)

# Gráfico ggplot
ggplot(df_group, aes(x = Dias_da_Semana, y = Total_Percorrido)) +
  geom_bar(stat = "identity", fill = "blue") +
  labs(title = "Total de Distância Percorrida por Dia da Semana",
       x = "Dia da Semana",
       y = "Total de Distância Percorrida") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))

rm(df_group, ordem_dias)



#### Exercício 8 - Delete a coluna Tempo do dataframe df.

df$Tempo <- NULL




#### Exercício 9 - Qual o total de corridas de taxi por dia da semana?

total_corridas <- summary(as.factor(df$Dias_da_Semana))
total_corridas





#### Exercício 10 - Qual a média para cada uma das colunas por dia da semana?

## Calculando Tudo Através do Loop

# Total de Corridas
total_corridas <- summary(as.factor(df$Dias_da_Semana))

# Calculando a Média de Corridas por Dia da Semana
media_por_dia <- as.data.frame(total_corridas / nrow(df))
media_por_dia

# Exbindo Em um Gráfico de Barras
barplot(media_por_dia$`total_corridas/nrow(df)`, names.arg = rownames(media_por_dia))


rm(media_por_dia, total_corridas)


