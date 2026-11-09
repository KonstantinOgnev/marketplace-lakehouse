# Microsoft Fabric Analytics Layer

## Overview

This directory contains the Microsoft Fabric and Power BI analytics components of the Marketplace Lakehouse project.

The solution extends an Azure Databricks Lakehouse with a business-facing analytical layer, enabling dimensional analysis, KPI monitoring, and executive reporting.

## Architecture

```text
Azure Databricks
    |
    | Medallion Architecture
    v
Bronze → Silver → Gold
                     |
                     v
          Microsoft Fabric
          OneLake / Lakehouse
          SQL Analytics Endpoint
                     |
                     v
             Semantic Model
             12 Tables
             20 DAX Measures
             6 Relationships
                     |
                     v
              Power BI Report
         Marketplace Executive Overview
```

## Semantic Model

The semantic model is based on Gold-layer marketplace data and supports business analysis across orders, payments, sellers, products, users, and customer activity.

The model contains:

- 12 tables, including dimensions, facts, and pre-aggregated datasets.
- 20 DAX measures for analytical reporting.
- 6 relationships between model objects.
- 1 Power Query expression for the underlying data connection.

The exported TMDL definition is available in:

[View the sanitized TMDL semantic model](semantic-model/full_model_portfolio.tmdl)

This is an anonymized portfolio snapshot. The original environment-specific connection has been replaced with placeholder values.

The file preserves the model definitions but is not a complete, independently deployable Power BI semantic model project.

## Power BI Report

The `report/` directory contains the Power BI Project source for **Marketplace Executive Overview**.

The report includes analytical views covering marketplace orders, Gross Merchandise Value (GMV), Average Order Value (AOV), revenue recognition, seller performance, and activity trends.

The report definitions are stored as source-controlled JSON files, making the visual layer reviewable without distributing the underlying business data.

The report source is available as a
[Power BI Project (PBIP)](report/Marketplace%20Executive%20Overview.pbip),
with its [report definitions](report/Marketplace%20Executive%20Overview.Report/definition/).

## Security and Portability

For public portfolio use:

- Live connection details have been removed from the exported TMDL.
- Local Power BI settings, PBIX files, and original environment-specific exports are excluded from Git.
- No credentials or datasets are intentionally included.
- Restoring a working connection requires environment-specific configuration and access to the relevant Fabric resources.

The report and semantic model are provided as technical portfolio artifacts rather than a ready-to-run standalone deployment.

## Technology Stack

Azure Databricks, Delta Lake, Unity Catalog, Lakeflow Jobs, Microsoft Fabric, OneLake, Power BI, DAX, TMDL, and Power BI Project (PBIP).