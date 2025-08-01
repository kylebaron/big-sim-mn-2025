library(glue)
library(dplyr)
library(duckplyr)

methods_restore()

system("du -sh output")

ds <- read_parquet_duckdb("output/**")

ds

nrow(ds)

class(ds)

output <- 
  summarise(
    ds, 
    Med = median(IPRED), 
    .by = c(time, dose)
  ) %>% arrange(dose, time)

output
explain(output)


nrow(ds) # prudence


library(arrow)

ds <- open_dataset("output") 
dd <- to_duckdb(ds)

foo <- 
  ds %>% 
  group_by(time) %>% 
  summarise( 
    Med = median(IPRED)
  )

foo2 <- 
  dd %>% 
  group_by(time) %>% 
  summarise( 
    Med = median(IPRED)
  )


ds # load when needed

pipe <- filter(ds, time < 4, CENT > 100) %>% select(dose, ECL, rep)
explain(pipe)
as_tibble(pipe, prudence="stingy")


glimpse(ds)
nrow(ds)
ds2 <- as_duckdb_tibble(ds, prudence = "thrifty")

ds2

nrow(ds2)
filter(ds2, time < 1, ID < 10) %>% nrow()

