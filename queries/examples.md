# Queries to answer the questions in the challenge

## How many users were active on a given day (they made a deposit or withdrawal)

```sql
SELECT
  COUNT(DISTINCT user_id) AS active_users
FROM
  transaction
WHERE
  DATE(event_timestamp) = '2023-07-15';
```

## Identify users haven't made a deposit

```sql
SELECT
  u.id AS user_id
FROM
  user u
LEFT JOIN
  (SELECT DISTINCT user_id FROM transaction WHERE transaction_type = 'deposit') d
ON
  u.id = d.user_id
WHERE
  d.user_id IS NULL;
```

## Identify on a given day which users have made more than 5 deposits historically

```sql
SELECT
  user_id
FROM
  transaction
WHERE
  transaction_type = 'deposit'
  AND DATE(event_timestamp) <= '2023-07-15'
GROUP BY
  user_id
HAVING
  COUNT(transaction_id) > 5;
```

## When was the last time a user made a login

```sql
SELECT
  user_id,
  MAX(event_timestamp) AS last_login
FROM
  user_login
WHERE
    user_id = '26751be1181460baf78db8d5eb7aad39';
```

## How many times a user has made a login between two dates

```sql
SELECT
user_id,
COUNT(login_id) AS login_count
FROM
user_login
WHERE
user_id = '26751be1181460baf78db8d5eb7aad39'
AND event_timestamp BETWEEN '2023-07-15' AND '2023-07-20';
```

## Number of unique currencies deposited on a given day

```sql
SELECT
  COUNT(DISTINCT currency) AS unique_currencies
FROM
    transaction
    WHERE
    DATE(event_timestamp) = '2023-07-15'
    AND transaction_type = 'deposit';
```

## Number of unique currencies withdrew on a given day

```sql
SELECT
  COUNT(DISTINCT currency) AS unique_currencies
FROM
    transaction
    WHERE
    DATE(event_timestamp) = '2023-07-15'
    AND transaction_type = 'withdraw';
```

## Total amount deposited of a given currency on a given day

```sql
SELECT
  SUM(amount) AS total_amount
FROM
    transaction
WHERE
    DATE(event_timestamp) = '2023-07-15'
    AND transaction_type = 'deposit'
    AND currency = 'ars';
```
