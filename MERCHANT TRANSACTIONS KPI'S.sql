select 
COUNT(*) AS TRANSACTIONS,
AVG(amount) as avg_amount,
sum(amount) as total_amount,
--mcc,
case when upper(acceptor) like '%HOMEDEPOT%' OR upper(acceptor) like '%THE HOME DEPOT%'   THEN 'HOMEDEPOT'
     when upper(acceptor) like '%PAYPAL%' THEN 'PAYPAL'
     when upper(acceptor) like '%LOWE%' THEN 'LOWES'
     when upper(acceptor) like '%AMAZON%' OR UPPER(ACCEPTOR) LIKE '%AMZN%'  THEN 'AMAZON'
     when upper(acceptor) like '%PODS %'  THEN 'PODS'
     when upper(acceptor) like '%WAYFAIR%'  THEN 'WAYFAIR'
     when upper(acceptor) like '%WESTELM%'  THEN 'WESTELM'
     when upper(acceptor) like '%TARGET%'  THEN 'TARGET'
     when upper(acceptor) like '%ACE HARDWARE%'  THEN 'ACE HARDWARE'
     END AS ACCEPTOR
from
(SELECT
    to_char(txn.date, 'YYYY-MM-DD') as date
    , coalesce(txn.debit, 0) - coalesce(txn.credit, 0) as amount
    , txn.status
    , merchant.acceptor
    , merchant.region
    , merchant.mcc
    , merchant.mid
    , merchant.street_address as merchant_street_address
    , merchant.city as merchant_city
    , merchant.zip as merchant_zip
    , merchant.mcc as merchant_mcc
    , merchant.mid as merchant_mid
  FROM
    (
      SELECT
          credit / 100 as credit,
          debit / 100 as debit,
          value_date as date,
          status,
          merchant_party_id,
          transaction_id
      FROM
          (
              SELECT DISTINCT ON (transaction_id)
                  *
              FROM
                  ledger
              WHERE
                  party = 'lender' AND
                  transaction_type = 'disbursement' AND
                  method = 'card'
              ORDER BY
                  transaction_id,
                  value_date DESC
          ) AS q
      WHERE
          q.status != 'voided'
    ) as txn,
    (
      SELECT
        name AS acceptor
        , mcc
        , mid
        , street_address
        , city as city
        , postal_code as zip
        , state as region
        , _id
      FROM
        transaction_party
    ) as merchant
  WHERE
    txn.merchant_party_id = merchant._id
  
   and txn.date < CURRENT_DATE
  ORDER BY date asc) mcc
  group by 
  --mcc,
  --acceptor
  case when upper(acceptor) like '%HOMEDEPOT%' OR upper(acceptor) like '%THE HOME DEPOT%'   THEN 'HOMEDEPOT'
     when upper(acceptor) like '%PAYPAL%' THEN 'PAYPAL'
     when upper(acceptor) like '%LOWE%' THEN 'LOWES'
     when upper(acceptor) like '%AMAZON%' OR UPPER(ACCEPTOR) LIKE '%AMZN%'  THEN 'AMAZON'
     when upper(acceptor) like '%PODS %'  THEN 'PODS'
     when upper(acceptor) like '%WAYFAIR%'  THEN 'WAYFAIR'
     when upper(acceptor) like '%WESTELM%'  THEN 'WESTELM'
     when upper(acceptor) like '%TARGET%'  THEN 'TARGET'
     when upper(acceptor) like '%ACE HARDWARE%'  THEN 'ACE HARDWARE'
     ELSE UPPER(ACCEPTOR)
     END 
  having sum(amount)>0
  order by 1 desc
 