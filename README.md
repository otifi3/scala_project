```markdown
# Scala Rule Engine for Retail Order Discounts

## Table of Contents

- [Project Overview](#project-overview)
- [Features](#features)
- [Technical Details](#technical-details)
- [Project Structure](#project-structure)
- [Setup and Running](#setup-and-running)
- [Discount Rules Summary](#discount-rules-summary)
- [Logging](#logging)
- [Project References](#project-references)
- [Notes](#notes)
- [Author](#-author)



## Project Overview

This Scala project implements a **rule engine** that processes retail store orders to automatically calculate applicable discounts based on a set of business rules. It reads transaction data from a CSV file, applies qualifying rules to determine discounts, computes the final prices, and loads the processed data into a PostgreSQL database. The system also logs important events and errors in a dedicated log file.

---

## Features

- **Qualifying rules** based on:
  - Product expiry date (less than 30 days remaining)
  - Product category discounts (cheese, wine)
  - Special date discounts (orders on March 23rd)
  - Quantity-based discounts (bulk purchase)
  - Channel and payment method discounts (app sales, Visa payments)

- **Discount calculation**:
  - If multiple discounts apply, the two highest discounts are averaged.
  - Final price is calculated after applying the average discount.

- **Data persistence**:
  - Results are stored in PostgreSQL tables for further analysis or reporting.

- **Logging**:
  - Engine events, errors, and processing status are logged in `logs/rule_engine.log` with timestamps and log levels.

---

## Technical Details

- **Functional Programming**:  
  Core logic is implemented using pure functions with no mutable variables or loops.  
  All functions are deterministic and side-effect free.

- **Scala Features Used**:
  - Case classes for immutable data modeling.
  - Higher-order functions for rule definitions.
  - `Using` for safe resource management.
  - Strong typing with `val` only.

- **Database**:  
  PostgreSQL connection is established using JDBC.  
  Connection details can be found in the `db` object.

- **Logging**:  
  Java’s built-in `java.util.logging` framework is configured to write logs to a file.

---

## Project Structure

```

src/
└── main/
├── scala/
│   └── engine/
│       ├── rules.scala       # Main rule engine application
│       ├── AppLogger.scala   # Logger configuration
│       └── db.scala          # Database connection utility
└── resources/
└── TRX1000.csv           # Input CSV file with order transactions
logs/
└── rule\_engine.log               # Log file generated during execution

````

---

## Setup and Running

1. **Prerequisites**:
   - Scala 2.13+ or Scala 3 installed.
   - SBT build tool installed.
   - PostgreSQL running locally with a database named `scala`.
   - PostgreSQL JDBC driver added as dependency.

2. **Configure Database**:
   - Update the connection details in `db.scala` if needed (URL, user, password).
   - Create the following tables in your database:

```sql
CREATE TABLE processed_orders_2 (
  product_name VARCHAR(100),
  total_before NUMERIC,
  discount NUMERIC,
  total_after NUMERIC
);
````

3. **Input Data**:

   * Place your input CSV file at `src/main/resources/TRX1000.csv`.

4. **Build and Run**:

   * Compile and run the project using SBT:

   ```bash
   sbt run
   ```

---

## Discount Rules Summary

| Rule                                | Discount Calculation                                   |
| ----------------------------------- | ------------------------------------------------------ |
| Less than 30 days to product expiry | (30 - days\_remaining) % discount (e.g., 29 days = 1%) |
| Cheese product                      | 10% discount                                           |
| Wine product                        | 5% discount                                            |
| Sold on March 23                    | 50% discount                                           |
| Quantity 6-9                        | 5% discount                                            |
| Quantity 10-14                      | 7% discount                                            |
| Quantity 15+                        | 10% discount                                           |
| Sold via App                        | Discount based on rounded quantity in multiples of 5   |
| Payment via Visa                    | 50% discount                                           |

If multiple discounts apply, the **top 2 discounts** are averaged for final discount.

---

## Logging

* Logs are saved to `logs/rule_engine.log`.
* Format: `TIMESTAMP LOGLEVEL MESSAGE`
* Example entries:

  ```
  2025-05-14 22:00:00 INFO Starting order processing pipeline...
  2025-05-14 22:01:00 SEVERE Error reading lines from CSV: File not found
  ```

---

## Project References

* [Project requirements and video guide](https://youtu.be/6uwRajbkaqI?si=6OJW_oCXE8Fcq36I)

---

## Notes

* The code is designed to be clean, readable, and purely functional.
* All side effects such as file IO, DB operations, and logging are wrapped appropriately.
* Error handling is included to log and throw exceptions on failures.

---

## 👤 Author

Created by Ahmed Otifi  
🔗 GitHub: [https://github.com/otifi3](https://github.com/otifi3)

---