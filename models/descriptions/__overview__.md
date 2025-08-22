{% docs __overview__ %}

# Welcome to the Flipside Crypto Movement Models Documentation

## **What does this documentation cover?**
The documentation included here details the design of the Movement
 tables and views available via [Flipside Crypto.](https://flipsidecrypto.xyz/) For more information on how these models are built, please see [the github repository.](https://github.com/flipsideCrypto/movement-models/)

## **How do I use these docs?**
The easiest way to navigate this documentation is to use the Quick Links below. These links will take you to the documentation for each table, which contains a description, a list of the columns, and other helpful information.

If you are experienced with dbt docs, feel free to use the sidebar to navigate the documentation, as well as explore the relationships between tables and the logic building them.

There is more information on how to use dbt docs in the last section of this document.

## **Quick Links to Table Documentation**

**Click on the links below to jump to the documentation for each schema.**

### Core Tables (`movement`.`CORE`.`<table_name>`)

**Dimension Tables:**
- [core.dim_labels](#!/model/model.movement_models.core__dim_labels)

**Fact Tables:**
- [core.fact_blocks](#!/model/model.movement_models.core__fact_blocks)
- [core.fact_changes](#!/model/model.movement_models.core__fact_changes)
- [core.fact_events](#!/model/model.movement_models.core__fact_events)
- [core.fact_transactions](#!/model/model.movement_models.core__fact_transactions)
- [core.fact_transactions_block_metadata](#!/model/model.movement_models.core__fact_transactions_block_metadata)
- [core.fact_transactions_state_checkpoint](#!/model/model.movement_models.core__fact_transactions_state_checkpoint)
- [core.fact_transfers](#!/model/model.movement_models.core__fact_transfers)

**Convenience Views:**
- [core.ez_transfers](#!/model/model.movement_models.core__ez_transfers)

### Price Tables (`movement`.`PRICE`.`<table_name>`)

**Dimension Tables:**
- [price.dim_asset_metadata](#!/model/model.movement_models.price__dim_asset_metadata)

**Fact Tables:**
- [price.fact_prices_ohlc_hourly](#!/model/model.movement_models.price__fact_prices_ohlc_hourly)

**Convenience Views:**
- [price.ez_prices_hourly](#!/model/model.movement_models.price__ez_prices_hourly)
- [price.ez_asset_metadata](#!/model/model.movement_models.price__ez_asset_metadata)

### DeFi Tables (`movement`.`DEFI`.`<table_name>`)

**Fact Tables:**
- [defi.fact_bridge_activity](#!/model/model.movement_models.defi__fact_bridge_activity)

### NFT Tables (`movement`.`NFT`.`<table_name>`)

**Fact Tables:**
- [nft.fact_nft_mints](#!/model/model.movement_models.nft__fact_nft_mints)
- [nft.fact_nft_sales](#!/model/model.movement_models.nft__fact_nft_sales)

### Stats Tables (`movement`.`STATS`.`<table_name>`)

**Convenience Views:**
- [stats.ez_core_metrics_hourly](#!/model/model.movement_models.stats__ez_core_metrics_hourly)

The movement models are built a few different ways, but the core fact tables are built using three layers of sql models: **bronze, silver, and gold (or core).**

- Bronze: Data is loaded in from the source as a view
- Silver: All necessary parsing, filtering, de-duping, and other transformations are done here
- Gold (or core): Final views and tables that are available publicly

The dimension tables are sourced from a variety of on-chain and off-chain sources.

Convenience views (denoted ez_) are a combination of different fact and dimension tables. These views are built to make it easier to query the data.

## **Using dbt docs**
### Navigation

You can use the ```Project``` and ```Database``` navigation tabs on the left side of the window to explore the models in the project.

### Database Tab

This view shows relations (tables and views) grouped into database schemas. Note that ephemeral models are *not* shown in this interface, as they do not exist in the database.

### Graph Exploration

You can click the blue icon on the bottom-right corner of the page to view the lineage graph of your models.

On model pages, you'll see the immediate parents and children of the model you're exploring. By clicking the Expand button at the top-right of this lineage pane, you'll be able to see all of the models that are used to build, or are built from, the model you're exploring.

Once expanded, you'll be able to use the ```--models``` and ```--exclude``` model selection syntax to filter the models in the graph. For more information on model selection, check out the [dbt docs](https://docs.getdbt.com/docs/model-selection-syntax).

Note that you can also right-click on models to interactively filter and explore the graph.


### **More information**
- [Flipside](https://flipsidecrypto.xyz/)
- [Community](https://docs.flipsidecrypto.xyz/welcome-to-flipside/flipside-community-overview)
- [Github](https://github.com/FlipsideCrypto/movement-models)
- [What is dbt?](https://docs.getdbt.com/docs/introduction)

{% enddocs %}