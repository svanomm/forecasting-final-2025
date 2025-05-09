library(dplyr)
library(here)
library(tsibble)
library(zoo)
library(data.table) 

# Import each csv file
raw_item_categories   <- read.csv(here("../data/item_categories.csv"))
raw_items             <- read.csv(here("../data/items.csv"))
raw_sales_train       <- read.csv(here("../data/sales_train.csv"))
raw_sample_submission <- read.csv(here("../data/sample_submission.csv"))
raw_shops             <- read.csv(here("../data/shops.csv"))
raw_test              <- read.csv(here("../data/test.csv"))

# Join data from the files
raw_data <- raw_sales_train |>
  left_join(raw_items, by = "item_id", keep = FALSE) |>
  left_join(raw_item_categories, by = "item_category_id", keep = FALSE) |>
  left_join(raw_shops, by = "shop_id", keep = FALSE) |>
  left_join(raw_test, by = c("shop_id", "item_id"), keep = FALSE)

# Create fake sales data for the brand new products
new_products <- raw_test |>
	anti_join(raw_data, by=c("item_id", "shop_id")) |>
	left_join(raw_items, by = "item_id", keep = FALSE) |>
	mutate(
		item_cnt_day=0,
		revenue=0,
		ym=yearmonth("2015 Oct")
		) |>
	select(c(item_id, shop_id, item_cnt_day, revenue, item_category_id, ym, ID))

# Convert date to date format
data <- raw_data |>
  mutate(
    date = as.Date(date, format = "%d.%m.%Y"),
    ym = yearmonth(date)
    )

data <- data |>
  arrange(shop_id, item_id, date) |>
  mutate(
    revenue = item_price * item_cnt_day
  ) |>
	select(c(item_id, shop_id, item_cnt_day, revenue, item_category_id, ym, ID, item_price))

data <- data |>
	bind_rows(new_products)

# Fix certain shops that are split into multiple shop_id values
# https://www.kaggle.com/code/abubakar624/first-place-solution-kaggle-predict-future-sales/comments
data <- data |> mutate(
	shop_id = case_when(
		shop_id == 0 ~ 57,
		shop_id == 1 ~ 58,
		shop_id == 11 ~ 10,
		shop_id == 40 ~ 39,
		.default = shop_id
	)
)

# Product-Monthly data
data_monthly <- data |>
  group_by(shop_id, item_id, ym, item_category_id) |>
  summarise(
    qty = sum(item_cnt_day),
    revenue = sum(revenue)
  ) |> as.data.frame() |>
  as_tsibble(key = c(shop_id, item_id)) |>
  arrange(shop_id, item_id, ym)

# See how many IDs have 0 sales in last month of data
t1 <- data_monthly[data_monthly$ym == yearmonth("2015 Oct"),]

t <- raw_test |> 
  left_join(t1, by = c("shop_id", "item_id"), keep=FALSE) |>
  mutate(
    qty = ifelse(is.na(qty), 0, qty),
    revenue = ifelse(is.na(revenue), 0, revenue),
  )

table(t$qty)
# 185566 zeroes out of 214200, or 86.6%

# Export a naive model prediction
naive <- t |> select(
  id = ID,
  item_cnt_month = qty
) |> mutate(
	item_cnt_month = ifelse(item_cnt_month>20, 20, item_cnt_month)
)
write.csv(naive, here("../output/naive.csv"), row.names = FALSE)

# Save a dataset with only the IDs that appear in test
data_nozero <- data |> filter(!is.na(ID))

ID_lookup <- data_nozero |>
	select(ID, item_id, shop_id, item_category_id) |>
	distinct() |>
	arrange(ID)

# Create balanced Monthly data
data_monthly <- data_nozero |>
  group_by(ID, ym) |>
  summarise(
    qty = sum(item_cnt_day),
    revenue = sum(revenue),
    price_mean = sum(revenue) / sum(item_cnt_day)
  ) |> as.data.frame() |>
  as_tsibble(key = ID) |>
  fill_gaps(.full = TRUE)

# Carry forward values
data_monthly <- data_monthly |>	as.data.table() |>
  arrange(ID, ym) |>
  group_by(ID) |>
  mutate(
    qty = ifelse(is.na(qty), 0, qty),
    revenue = ifelse(is.na(revenue), 0, revenue),
    price_mean = na.locf(price_mean, na.rm = FALSE), 
    	# this carries values forward
  ) |>
  mutate(price_mean = na.locf(price_mean, na.rm = FALSE, fromLast = TRUE), 
    	# this carries values backward, if first observation is NA
  ) |> left_join(ID_lookup, by = "ID", keep = FALSE)

# Product-category-level monthly data
data_prodcat_monthly <- data_nozero |>
  group_by(item_category_id, ym) |>
  summarise(
    qty = sum(item_cnt_day),
    revenue = sum(revenue),
    price_mean = sum(revenue) / sum(item_cnt_day)
  ) |>
  as_tsibble(key = item_category_id) |>
  fill_gaps(.full = TRUE) |>
  arrange(item_category_id, ym) |>
  mutate(
    qty = ifelse(is.na(qty), 0, qty),
    revenue = ifelse(is.na(revenue), 0, revenue),
    price_mean = na.locf(price_mean, na.rm = FALSE)
  ) |>
  mutate(
    price_mean = na.locf(price_mean, na.rm = FALSE, fromLast = TRUE)
  )

# Product-level monthly data
data_product_monthly <- data_nozero |>
  group_by(item_id, item_category_id, ym) |>
  summarise(
    product_qty = sum(item_cnt_day),
    product_revenue = sum(revenue),
    product_price_mean = sum(revenue) / sum(item_cnt_day)
  ) |>
  as_tsibble(key = c(item_id, item_category_id)) |>
  fill_gaps(.full = TRUE) |>
  arrange(item_id, ym) |>
  mutate(
    product_qty = ifelse(is.na(product_qty), 0, product_qty),
    product_revenue = ifelse(is.na(product_revenue), 0, product_revenue),
    product_price_mean = na.locf(product_price_mean, na.rm = FALSE)
  ) |>
  mutate(
    product_price_mean = na.locf(product_price_mean, na.rm = FALSE, fromLast = TRUE)
  )

# Store-level monthly data
data_store_monthly <- data_nozero |>
  group_by(shop_id, ym) |>
  summarise(
    qty = sum(item_cnt_day),
    revenue = sum(revenue),
    price_mean = sum(revenue) / sum(item_cnt_day)
  ) |>
  as_tsibble(key = shop_id) |>
  fill_gaps(.full = TRUE) |>
  arrange(shop_id, ym) |>
  mutate(
    qty = ifelse(is.na(qty), 0, qty),
    revenue = ifelse(is.na(revenue), 0, revenue),
    price_mean = na.locf(price_mean, na.rm = FALSE)
  ) |>
  mutate(
    price_mean = na.locf(price_mean, na.rm = FALSE, fromLast = TRUE)
  )

# Save the created tables
save(list = c(
  "data",
  "data_nozero",
  "data_product_monthly",
  "data_prodcat_monthly",
  "data_store_monthly",
  "data_monthly"
), file = here("../data/processed_data.RData")
)
