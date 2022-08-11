---FIRST 15 DAYS VIEW
select
--DISBURSEMENTS
greatest(
    sum(
        case
            when status = 'cleared'
            and transaction_type = 'disbursement' then (cleared.debit) / 100
            else 0
        end
    ),
    0
) as cleared_gross_disbursement_amount,
greatest(
    sum(
        case
            when status = 'cleared'
            and transaction_type = 'disbursement' then (cleared.debit) / 100
            else 0
        end
    ) - sum(
        case
            when status = 'cleared'
            and transaction_type = 'disbursement' then (cleared.credit) / 100
            else 0
        end
    ),
    0
) as cleared_net_disbursement_amount,
--REFUNDS 
greatest(
    sum(
        case
            when status = 'cleared'
            and transaction_type = 'disbursement' then (cleared.credit) / 100
            else 0
        end
    ),
    0
) as cleared_refund_amount,
--REPAYMENTS 
greatest(
    sum(
        case
            when status = 'cleared'
            and transaction_type = 'repayment' then (cleared.credit) / 100
            else 0
        end
    ),
    0
) as cleared_repayment_amount,
greatest(
    sum(
        case
            when status = 'cleared'
            and transaction_type = 'repayment' then (cleared.credit) / 100
            else 0
        end
    ) - sum(
        case
            when status = 'cleared'
            and transaction_type = 'repayment' then(cleared.debit) / 100
            else 0
        end
    ),
    0
) as cleared_net_repayments_amount,
--OUTSTANDING PRINCIPAL BALANCE--rounding and casting as numeric plus adding 4 decimals
round(
    cast(
        greatest(
            sum(
                case
                    when status = 'cleared'
                    and transaction_type = 'disbursement' then (cleared.debit) / 100
                    else 0
                end
            ) - sum(
                case
                    when status = 'cleared'
                    and transaction_type = 'disbursement' then (cleared.credit) / 100
                    else 0
                end
            ) - sum(
                case
                    when status = 'cleared'
                    and transaction_type = 'repayment' then (cleared.credit) / 100
                    else 0
                end
            ),
            0
        ) as numeric
    ),
    2
) as outstanding_principal_balance,
-- TOTAL COLLECTIONS
greatest(
    (
        sum(
            case
                when status = 'cleared'
                and transaction_type = 'repayment' then (cleared.credit) / 100
                else 0
            end
        ) - sum(
            case
                when status = 'cleared'
                and transaction_type = 'repayment' then (cleared.debit) / 100
                else 0
            end
        )
    ) + sum(
        case
            when status = 'cleared'
            and transaction_type = 'disbursement' then (cleared.credit) / 100
            else 0
        end
    ),
    0
) as total_collections_cleared,
cleared.loan_id as loan_id,
user_id
from
    (
        select
            distinct on (transaction_id) *
        from
            ledger
        where
            party = 'lender'
            and status = 'cleared'
            and cast(timezone('America/New_York', created_at) as date) >= date_trunc('month', current_date)
            and cast(timezone('America/New_York', created_at) as date) <= (date_trunc('month', current_date)) + interval '14' day
--where
--loan_id in (
  --      '5d9357690834cc0016dbe066',
  --      '604bc4ff39004c0034143465') 
) cleared
group by
    cleared.loan_id,
    user_id;