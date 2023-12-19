-- Databricks notebook source
-- MAGIC %sql
-- MAGIC CREATE OR REPLACE VIEW accounts AS SELECT * FROM capstone.accounts_clean

-- COMMAND ----------

select* from accounts

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE VIEW branch AS SELECT * FROM capstone.branch_clean

-- COMMAND ----------

CREATE OR REPLACE VIEW credit AS SELECT * FROM capstone.credit_clean

-- COMMAND ----------

CREATE OR REPLACE VIEW customers AS SELECT * FROM capstone.customers_clean

-- COMMAND ----------

CREATE OR REPLACE VIEW loans AS SELECT * FROM capstone.loans_clean

-- COMMAND ----------

CREATE OR REPLACE VIEW transactions AS SELECT * FROM capstone.transactions_clean

-- COMMAND ----------

show views

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Count of different type of accounts across branches

-- COMMAND ----------

select *
from 
(
  select accountid, accounttype, branch_id
  from accounts
) as src
PIVOT
(
  count(accountid) for accounttype in ("Current", "Savings", "Business")
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Customers with Multiple Loans and Their Total Debt

-- COMMAND ----------

select customer_id,sum(current_loan_amount) as Total_Loan, sum(monthly_debt) as Total_Monthly_Debt, count(loan_id) as no_of_loans
from loans
group by customer_id
having count(loan_id) > 1

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Loan Amount Trend by home_ownership for Each Loan Purpose

-- COMMAND ----------

select distinct(purpose)
from loans

-- COMMAND ----------

SELECT
  YEAR(loan_sanctioned_date) AS `Year`,
  MONTH(loan_sanctioned_date) AS `Month`,
  SUM(CASE WHEN purpose = 'Home Improvements' THEN current_loan_amount ELSE 0 END) AS `Home Improvements`,
  SUM(CASE WHEN purpose = 'Buy Car' THEN current_loan_amount ELSE 0 END) AS `Buy Car`,
  SUM(CASE WHEN purpose = 'Buy House' THEN current_loan_amount ELSE 0 END) AS `Buy House`,
  SUM(CASE WHEN purpose = 'Debt Consolidation' THEN current_loan_amount ELSE 0 END) AS `Debt Consolidation`,
  -- Add more lines for each purpose as needed
  SUM(CASE WHEN purpose = 'Other' THEN current_loan_amount ELSE 0 END) AS `Other`
FROM
  loans
GROUP BY
  YEAR(loan_sanctioned_date),
  MONTH(loan_sanctioned_date)
ORDER BY
  1,2;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###To classify each branch into one of three tiers: Tier 1, Tier 2, or Tier 3

-- COMMAND ----------

with data as (
select b.branchid, b.bank_city, avg(c.credit_score) as Avg_Credit_Score, count(t.transaction_id) as Num_Transactions
from transactions t JOIN accounts a 
ON t.account_id = a.accountid
JOIN credit c on a.customerid = c.customer_id
JOIN branch b on a.branch_id = b.branchid
group by b.branchid, b.bank_city)
select *,
CASE 
WHEN Avg_Credit_Score >= 700 or Num_Transactions >= 1000 THEN 'Tier 1'

WHEN (Avg_Credit_Score >= 650 and Avg_Credit_Score <= 699) or (Num_Transactions >= 500 and Num_Transactions < 699) THEN 'Tier 2'
ELSE 'Tier 3'
END as Tiers
from data

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###Customers whose credit scores are below the average credit score of all customers

-- COMMAND ----------

select customer_id, credit_score
from credit
group by customer_id, credit_score
having credit_score < (select avg(credit_score) from credit)

-- COMMAND ----------


