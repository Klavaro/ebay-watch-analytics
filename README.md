# 🛍️ eBay Watch Analytics – Capstone Project

This project builds a data modeling solution using dbt and Snowflake to analyze eBay item listing data. The goal is to transform raw eBay listing data into dimensional models and business-oriented marts to answer real-life business questions.

## 🎯 Objective

To build a dimensional model (Kimball-style) of eBay listings and generate analytical marts to help businesses make informed decisions about item pricing, visibility, market trends, and performance.

## 🧱 Architecture Overview

```text
eBay API → Snowflake (RAW → STAGING → STAR SCHEMA → MARTS) → dbt (transformations, snapshots, tests)
```

## 🧠 Key Features

- **Staging models**: Clean and flatten JSON data into tabular format.
- **Normalized models**: Dimensions like `category`, `condition`, and `seller` are normalized.
- **Star schema**: Fact table centered on item listings with surrogate keys.
- **Bridge tables**: Capture many-to-many relationships like images, shipping options, and buying methods.
- **Snapshots**: Track changes in seller data using SCD Type 2.
- **Marts**: Business-use case aggregates to answer performance and pricing questions.

## 🔎 Business Questions Answered

### `mart_item_performance`

**Purpose**: Aggregate item-level metrics to understand visibility, pricing, and promotional features.

- How do pricing and listing features (like coupons or top-rated experience) affect performance?
- Which countries or categories consistently perform well?

### `mart_condition_price_index`

**Purpose**: Monitor pricing trends by condition.

- Are new items priced significantly higher than used ones?
- How does item condition correlate with category and value?

## 🗃️ Dimensional Model Overview

![ER Diagram](er_diagram.png)

### Fact Table: `fact_item_listing`

- Grain: One row per item listing.
- Contains keys to dimensions and listing metadata like price, condition, coupons, etc.

### Dimension Tables:

- `dim_condition`: Normalized item conditions.
- `dim_category`: Leaf-level eBay categories.
- `dim_seller`: With SCD Type 2 applied to capture feedback changes.
- `dim_date`: Derived from origin, creation, and load timestamps.

### Bridge Tables:

- `bridge_item_image`: All image URLs per item (thumbnail & additional).
- `bridge_item_buying_option`: Available buying methods per item.
- `bridge_item_shipping_option`: Captures all shipping options.

## 🔄 Snapshots

### `snap_seller`

Tracks SCD Type 2 changes in seller feedback data over time.

- Detects changes in feedback score and percentage.
- Used to preserve seller history while referencing in `dim_seller`.

## 🧪 Data Quality with dbt Tests

Includes:

- Uniqueness and null tests on surrogate keys.
- Relationship tests between fact and dimensions.
- Accepted values for controlled vocabularies (e.g., `priority_listing` as TRUE/FALSE).

## 📚 Technologies

| Tool      | Purpose                          |
| --------- | -------------------------------- |
| dbt       | Data transformation and modeling |
| Snowflake | Cloud data warehouse             |
| GitHub    | Source control and collaboration |

## 🗂️ Project Structure

```bash
.
├── models/
│   ├── staging/
│   ├── dimensions/
│   ├── facts/
│   ├── bridge/
│   ├── marts/
├── snapshots/
├── macros/
├── schema.yml
├── README.md
```

## ⚙️ How to Run

1. Clone the repository.
2. Configure your `profiles.yml` for Snowflake.
3. Run transformations:

```bash
dbt run
```

4. Run tests:

```bash
dbt test
```

5. Capture historical changes:

```bash
dbt snapshot
```

## 👤 Author

**Your Name**  
Capstone Project for Data Engineering/Analytics  
[GitHub](#) | [LinkedIn](#)
