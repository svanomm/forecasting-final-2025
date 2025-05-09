library(tidyverse)
library(fpp3)
library(tsibble)
library(here)

# Load the tables
load(here("../data/data_final.RData"))

# Declare tsibble
data <- data_final |> as.data.frame() |> 
	as_tsibble(key = c(shop_id, item_id, item_category_id),
						 index = ym)

train <- data[data$ym <  550, ]
test  <- data[data$ym == 550, ]

# Declare aggregated structure
train <- train |> aggregate_key(
	shop_id * (item_category_id / item_id), qty = sum(qty)
)



my_models <- train |>
	model(
		naive = NAIVE(qty),
	) |>
	reconcile(
		BottomUp =  bottom_up(naive),
		TopDown  =   top_down(naive),
		OLS      =  min_trace(naive, method = "ols"),
		MinT     =  min_trace(naive, method = "mint_shrink")
	)





# Use the last month of data as a testing set
test <- reg_data |>
	filter(ym == yearmonth("2015 Oct"))
reg_data <- reg_data |>
	filter(ym < yearmonth("2015 Oct")) # saving RAM by removing test period from main data


# Fit models
my_models <- reg_data |>
	model(
	  naive = NAIVE(qty),
		ets   = ETS(qty ~ trend("A")  + season("A")),
		ets   = ETS(qty ~ trend("A")  + season("M")),
		ets   = ETS(qty ~ trend("Ad") + season("A")),
		ets   = ETS(qty ~ trend("Ad") + season("M")),
	)

my_forecasts <- my_models |>
	forecast(new_data = test)
