# Architecture

## Scenario

A fictional online retailer delivers three files: customers and products as CSV, and orders as
JSON Lines with nested items. The deliveries contain the problems real sources have: malformed
records, repeated orders, stale customer versions, broken references and impossible values.

The pipeline must turn them into reliable revenue reporting without losing any evidence of what
arrived.

## Data flow

```mermaid
flowchart TD
    subgraph Raw
        C1[customers.csv]
        P1[products.csv]
        O1[orders.jsonl]
    end
    subgraph Bronze
        C2[customers]
        P2[products]
        O2[orders]
    end
    subgraph Silver
        C3[customers]
        P3[products]
        O3[orders]
        I3[order_items]
    end
    subgraph Gold
        D[daily_revenue]
        L[customer_lifetime_value]
        R[product_performance]
    end
    C1 --> C2 --> C3
    P1 --> P2 --> P3
    O1 --> O2 --> O3
    O2 --> I3
    C3 -- known_customer --> O3
    P3 -- known_product --> I3
    O3 --> D & L & R
    I3 --> D & L & R
    C3 --> L
    P3 --> R
```

## Layer contracts

### Bronze

- Every field is read as a string, so a bad value cannot fail ingestion.
- Malformed JSON is kept in `_corrupt_record`.
- Each row carries `_source_file`, `_ingested_at` and `_batch_id`.
- Data is partitioned by `_batch_id`. Overwriting is dynamic, so re-running a batch replaces only
  that batch.

### Silver

| Table | Key | Rules |
| --- | --- | --- |
| `customers` | `customer_id` | ID present, valid email, parsable `updated_at`. Newest version wins. |
| `products` | `product_id` | ID present, non-negative price, parsable `updated_at`. Newest version wins. |
| `orders` | `order_id` | Well-formed record, ID present, known customer, known status, parsable timestamp, at least one item. |
| `order_items` | `order_id`, `line_number` | Known product, positive quantity, non-negative price. |

A rule that evaluates to NULL counts as broken. Silver is rebuilt from every Bronze batch on each
run.

A rejected line does not reject its order. The order stays valid and only the line moves to
quarantine, because the rest of the sale really happened.

### Gold

| Mart | Grain | Answers |
| --- | --- | --- |
| `daily_revenue` | Day (UTC) | How much do we sell per day? |
| `customer_lifetime_value` | Customer | Who are our most valuable customers? |
| `product_performance` | Product | What leads each category? |

Revenue is recognised for orders that are `placed`, `shipped` or `delivered`. Orders that were
`cancelled` or `returned` are excluded.

## Quality gate

After Gold is built, the gate checks:

- Primary key uniqueness on Silver tables and Gold marts.
- Required fields are populated.
- Referential integrity between Silver tables.
- Reconciliation: the three marts and the Silver lines report the same revenue, using exact decimal
  arithmetic.

The report is written first, so a failed run still leaves evidence. The pipeline then raises
`DataQualityError` and the command exits with a non-zero code.

## Decisions

| Decision | Why | Trade-off |
| --- | --- | --- |
| Pure transformations separated from I/O | Almost all logic can be tested without touching disk | `pipeline.py` has to wire reads and writes explicitly |
| Full Silver rebuild on each run | Simple, and correct by construction when data is repeated | Cost grows with history; incremental loading is on the roadmap |
| Quarantine tables rather than dropping or failing | Keeps evidence and lets valid data flow | Quarantine needs monitoring |
| Gold in `.sql` files | Business logic stays readable by analysts and reviewable on its own | Requires a small templating step for shared constants |
| Parquet rather than Delta Lake | No extra dependency for the first version | No ACID guarantees or time travel yet |
| Synthetic data | Reproducible, free of private data, and defects are known exactly | Volumes and distributions are not those of a real business |
| Generator states expected counts | Tests compare with a source independent of the code under test | The generator has to be kept in sync when defects are added |

## Known limitations

- A run overwrites Silver and Gold in place, so a crash mid-write can leave a layer partly written.
- Orders that lose every line to quarantine stay in Silver without items and contribute no revenue.
- The quality gate runs after Gold is written, so a failed run leaves the failing output in place.
  It signals failure through the exit code and the report.
