# Exercise:

## Add Marts Models (Core)
This is the layer where everything comes together and we start to arrange all of our `atoms` (staging models) and `molecules` (intermediate models) into full-fledged cells that identity and purpose.

You can check [dbt docs](https://docs.getdbt.com/best-practices/how-we-structure/4-marts) for more details about marts.

## Task: Create dimension and fact tables
The goal is to implement the models that are represented on the Snowflake Schema below.

[Data Model - Snowflake Schema](https://miro.com/app/board/uXjVN-3i7mo=/?moveToWidget=3458764574588845913&cot=14)

The fact and dimension tables are described below.

### Fact table
- **fct_reviews:** a fact table that gets data from `stg_boardgames__reviews` model. Each record in the fact table (also known as the [grain](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/grain/)) is a review with a rating for a boardgame.

### Dimension tables
- **dim_users:** a dimension table that gets data from `int_users_countries_joined` intermediate model that combines info from `users` and `countries`.
- **dim_boardgames:** a dimension table that gets data from `int_boardgames__boardgames_filtered`.
- **dim_artists:** a sub-dimension table associated to `dim_boardgames` that gets data from `stg_boardgames__artists` combined with `int_boardgames__boardgames_filtered`.
- **dim_categories:** a sub-dimension table associated to `dim_boardgames` that gets data from `stg_boardgames__categories` combined with `int_boardgames__boardgames_filtered`.
- **dim_designers:** a sub-dimension table associated to `dim_boardgames` that gets data from `stg_boardgames__designers` combined with `int_boardgames__boardgames_filtered`.
- **dim_mechanics:** a sub-dimension table associated to `dim_boardgames` that gets data from `stg_boardgames__mechanics` combined with `int_boardgames__boardgames_filtered`.
- **dim_publishers:** a sub-dimension table associated to `dim_boardgames` that gets data from `stg_boardgames__publishers` combined with `int_boardgames__boardgames_filtered`.
- **dim_rankings:** a sub-dimension table associated to `dim_boardgames` that gets data from `stg_boardgames__rankings` combined with `int_boardgames__boardgames_filtered`.

All these models should be materialized as tables.
**Hint:** You can use a `+materialized` config to accomplish that.

### Surrogate Keys
A surrogate key in a database is a unique identifier for each entity in the database. The surrogate key is not derived from application data, unlike a natural (or business) key.

To create the surrogate key for each table we're going to use the `dbt_utils` package.

To do that, we need to create a `packages.yml` file on the main folder (next to `dbt_project.yml` file), with the following:

```yaml
packages:
  - package: dbt-labs/dbt_utils
    version: 1.1.1
```

Check the latest version available [here](https://hub.getdbt.com/dbt-labs/dbt_utils/latest/).

Click [here](https://github.com/dbt-labs/dbt-utils) to check other macros that are available from `dbt_utils` package. It has a lot of useful features available.

Run `dbt deps` to install the package.

To generate the surrogate key, we use a dbt macro that is provided by the `dbt_utils` package called `generate_surrogate_key()`. The generate surrogate key macro uses the appropriate hashing function from your database to generate a surrogate key from a list of key columns (e.g. md5(), hash()). Read more about the [generate_surrogate_key](https://docs.getdbt.com/blog/sql-surrogate-keys) macro.

See the example below, on how to use it:

```sql
{{ dbt_utils.generate_surrogate_key(['field_a', 'field_b'[,...]]) }}
```

### Add Tests to Models
Define the following generic out-of-the-box tests whenever applies:
- unique
- relationships
  - Customize the data test name to something more *user friendly*. Check this [previous challenge](../13_customize_data_test_name/13_customize_data_test_name.md) to refresh your mind on how to do it.


### Change materialization of dim_rankings to incremental
Incremental models are built as tables in your data warehouse. The first time a model is run, the table is built by transforming all rows of source data. On subsequent runs, dbt transforms only the rows in your source data that you tell dbt to filter for, inserting them into the target table which is the table that has already been built.

**Reminder:** Not only do we need to insert the new records into the target table from daily updates, but we also have to adjust these two columns from records of previous updates:
- `is_current`
- `valid_to` 

Let's see the following data from `stg_boardgames__rankings` model for `boardgame_id = 1`:

| BOARDGAME_ID | BOARDGAME_RANK | UPDATED_AT              | VALID_FROM              | VALID_TO                | IS_CURRENT |
|:------------:|----------------|-------------------------|-------------------------|-------------------------|------------|
| 1            | 406            | 2024-01-03 17:55:26.883 | 2024-01-03 17:55:26.883 | 2024-01-04 10:03:34.847 | FALSE      |
| 1            | 406            | 2024-01-04 10:03:34.847 | 2024-01-04 10:03:34.847 | 2024-01-05 10:03:22.027 | FALSE      |
| 1            | 407            | 2024-01-05 10:03:22.027 | 2024-01-05 10:03:22.027 | 2024-01-06 10:03:23.305 | FALSE      |
| 1            | 407            | 2024-01-06 10:03:23.305 | 2024-01-06 10:03:23.305 | 2024-01-08 09:23:00.513 | FALSE      |
| 1            | 407            | 2024-01-08 09:23:00.513 | 2024-01-08 09:23:00.513 | 2024-01-08 10:03:34.912 | FALSE      |
| 1            | 405            | 2024-01-08 10:03:34.912 | 2024-01-08 10:03:34.912 | 2024-01-09 12:14:34.614 | FALSE      |
| 1            | 406            | 2024-01-09 12:14:34.614 | 2024-01-09 12:14:34.614 | 2024-01-10 10:03:22.031 | FALSE      |
| 1            | 406            | 2024-01-10 10:03:22.031 | 2024-01-10 10:03:22.031 | 2024-01-11 10:03:30.920 | FALSE      |
| 1            | 406            | 2024-01-11 10:03:30.920 | 2024-01-11 10:03:30.920 | 2024-01-12 10:03:42.098 | FALSE      |
| 1            | 406            | 2024-01-12 10:03:42.098 | 2024-01-12 10:03:42.098 | 2024-01-13 10:03:17.592 | FALSE      |
| 1            | 406            | 2024-01-13 10:03:17.592 | 2024-01-13 10:03:17.592 | 2024-01-14 10:03:20.833 | FALSE      |
| 1            | 406            | 2024-01-14 10:03:20.833 | 2024-01-14 10:03:20.833 | 2024-01-15 10:03:38.702 | FALSE      |
| 1            | 406            | 2024-01-15 10:03:38.702 | 2024-01-15 10:03:38.702 | 2024-01-16 10:03:35.589 | FALSE      |
| 1            | 406            | 2024-01-16 10:03:35.589 | 2024-01-16 10:03:35.589 | 2024-01-17 10:03:39.218 | FALSE      |
| 1            | 406            | 2024-01-17 10:03:39.218 | 2024-01-17 10:03:39.218 | 2024-01-18 10:03:34.641 | FALSE      |
| 1            | 406            | 2024-01-18 10:03:34.641 | 2024-01-18 10:03:34.641 | 2024-01-19 10:03:34.534 | FALSE      |
| 1            | 407            | 2024-01-19 10:03:34.534 | 2024-01-19 10:03:34.534 | 9999-12-31 00:00:00.000 | TRUE       |

As you can see, the most recent record has the values:
- `is_current` = true
- `valid_to` = '9999-12-31 00:00:00.000'

When the next update is performed and if we only append the new records to the table, using the `incremental` materialization, what would happen to the records from the previous day?
- Those records wouldn't be updated, so it would keep on getting multiple records for the same `boardgame_id` with duplicate values for `is_current` and `valid_to` columns.

To solve that issue, we also have to implement an `incremental_strategy` to merge those updates to records already existing on the table.

#### Using incremental materializations
To use incremental models you need to:
- Define a config block with `materialized='incremental'`
- Tell dbt how to filter the rows on an incremental run
- The unique key of the model (if any)

To tell dbt which rows it should transform on an incremental run, wrap valid SQL that filters for these rows in the `is_incremental()` macro.

For example, a model that includes a computationally slow transformation on a column can be built incrementally, as follows:

```sql
{{
    config(
        materialized='incremental'
    )
}}

select
    *,
    my_slow_function(my_column)

from raw_app_data.events

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where event_time > (select max(event_time) from {{ this }})

{% endif %}
```

##### Define incremental strategy
The `incremental_strategy` config can either be specified directly on single models, or for all models in your `dbt_project.yml` file:

```yaml
# dbt_project.yml
...

models:
  +incremental_strategy: "insert_overwrite"
```

or:

```sql
{{
  -- my_model.sql
  config(
    materialized='incremental',
    unique_key='date_day',
    incremental_strategy='merge',
    ...
  )
}}

select ...
```

If you are using the `merge` strategy and have specified a unique_key, by default, dbt will entirely overwrite matched rows with new values.

To avoid that, you may optionally pass a list of column names to a `merge_update_columns` config. In that case, dbt will update *only* the columns specified by the config, and keep the previous values of other columns.

```sql
{{
    -- my_model.sql
  config(
    materialized = 'incremental',
    unique_key = 'id',
    merge_update_columns = ['email', 'ip_address'],
    ...
  )
}}

select ...
```

Alternatively, you can specify a list of columns to exclude from being updated by passing a list of column names to a `merge_exclude_columns` config.

**Important:** To be able to update and merge with data from previous records, we have to use a different filter condition.
The following doesn't work because it will only search for new records:

```sql
--my_model.sql
...

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses > to include records whose timestamp occurred since the last run of this model)
  where event_time > (select max(event_time) from {{ this }})

{% endif %}
```

We're going to use the following filter condition to look only for the last 3 days:
```sql
--my_model.sql
...

{% if is_incremental() %}

  -- this filter will only be applied on an incremental run
  -- (uses >= to include records arriving later than the previous 3 days)
  where updated_at > dateadd(day, -3, current_date)

{% endif %}
```



You can check [dbt docs](https://docs.getdbt.com/docs/build/incremental-models) for more details about incremental models.

### Task: Use `incremental_strategy`=merge for `dim_rankings`
Now it's time to apply the features showed above.

We want to configure the following:
- **incremental_strategy:** merge
- **columns to be updated:**
  - `is_current`
  - `valid_to`
- apply filter condition to update only records from the last 3 days 

---

### Solution

- [fct_reviews.sql](./core/fct_reviews.sql)
- [dim_users.sql](./core/dim_users.sql)
- [dim_artists.sql](./core/dim_artists.sql)
- [dim_boardgames.sql](./core/dim_boardgames.sql)
- [dim_categories.sql](./core/dim_categories.sql)
- [dim_designers.sql](./core/dim_designers.sql)
- [dim_mechanics.sql](./core/dim_mechanics.sql)
- [dim_publishers.sql](./core/dim_publishers.sql)
- [dim_rankings.sql - materialized='table'](./core/dim_rankings.sql)
- [dim_rankings.sql - materialized='incremental'](./core/dim_rankings_incremental.sql)
- [_core__models.yml](./core/_core__models.yml)
- [dbt_project.yml](dbt_project.yml)
- [packages.yml](./packages.yml)

---

[Return to Project Challenges](../../../README.md#9-project-challenges)