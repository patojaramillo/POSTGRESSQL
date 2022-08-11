--most updated code
with ledger_cleared as(
    select
        distinct on (transaction_id) ledger.transaction_id,
        ledger.loan_id,
        ledger.method,
        to_char(
            case
                when ledger.method in ('direct', 'check')
                and ledger.external_post_date is not null then timezone('America/New_York',ledger.external_post_date)
                when ledger.method = 'direct' then timezone('America/New_York',ledger.value_date)
                else timezone('America/New_York',ledger.created_at)
            end,
            'MM/DD/YYYY'
        ) as date,
        transaction_type as ledger_item_type,
        debit / 100 as debit,
        credit / 100 as credit
    from
        ledger
        left join (
            select
                distinct on (transaction_id) transaction_id,
                created_at
            from
                ledger
            where
                party = 'lender'
                and status = 'voided'
            order by
                transaction_id,
                created_at desc
        ) ledger_initial on ledger.transaction_id = ledger_initial.transaction_id
        and ledger.created_at < ledger_initial.created_at
    where
        party = 'lender'
        and (
            (
                status = 'cleared'
                and method in ('direct', 'check')
            )
            or (
                status != 'voided'
                and method = 'card'
            )
        )
        and ledger_initial.transaction_id is null
    order by
        ledger.transaction_id,
        ledger.created_at desc
),
external_ledger as (
    select
        ledger_cleared.transaction_id,
        ledger_cleared.date,
        ledger_cleared.ledger_item_type,
        case
            when ledger_cleared.method = 'direct'
            and notable_loans.is_ach_only = 'false' then 'CASH_ADVANCE'
            when ledger_cleared.method = 'direct' then 'ACH_LOAN'
            when ledger_cleared.method = 'card' then 'CARD'
            when ledger_cleared.method = 'check' then 'CHECK'
        end as transaction_type,
        ledger_cleared.debit,
        ledger_cleared.credit,
        notable_loans.slug as key,
        notable_loans.principal as loan_principal,
        notable_loans.is_ach_only as is_ach,
        notable_loan_applications.compass_agent_master_id as agent_id,
        concat(
            notable_loan_applications.first_name,
            ' ',
            notable_loan_applications.last_name
        ) as applicant,
        notable_loan_applications.listing_address_line1 as listing_address_line_1,
        notable_loan_applications.listing_address_line2 as listing_address_line_2,
        notable_loan_applications.listing_city as listing_city,
        notable_loan_applications.listing_state as listing_state,
        notable_loan_applications.listing_zip as listing_zip,
        to_char(now(), 'MM/DD/YYYY HH:MM') as report_generated_at
    from
        ledger_cleared
        inner join notable_loans on ledger_cleared.loan_id = notable_loans._id
        inner join notable_loan_applications on notable_loan_applications._id = notable_loans.loan_application_id
    where
        notable_loans._id is not null
        and notable_loans.status != 'EXPIRED'
        and notable_loans.program = 'compass'
        and notable_loan_applications.compass_agent_master_id not in ('AJB00006666')
)
select
    *
from
    external_ledger;


-- when external post date is present use it as it's entered manually by accounting and operations team members
with ledger_cleared as(
    select
    distinct on (transaction_id) ledger.transaction_id,
    ledger.loan_id,
    ledger.method,
    to_char(
        case
            when ledger.method in ('direct', 'check')
            and ledger.external_post_date is not null then ledger.external_post_date
            when ledger.method = 'direct' then ledger.value_date
            else effective_date
        end,
        'MM/DD/YYYY'
    ) as date,
    transaction_type as ledger_item_type,
    debit / 100 as debit,
    credit / 100 as credit
from
    ledger
    left join (
        select
            distinct on (transaction_id) transaction_id,
            created_at
        from
            ledger
        where
            party = 'lender'
            and status = 'voided'
        order by
            transaction_id,
            created_at desc
    ) ledger_initial on ledger.transaction_id = ledger_initial.transaction_id and ledger.created_at < ledger_initial.created_at
where
    party = 'lender'
    and ((status = 'cleared' and method in ('direct','check'))
    or (status != 'voided' and method = 'card'))
    and ledger_initial.transaction_id is null
order by
    ledger.transaction_id,
    ledger.created_at desc
),
loan_applications as (
    select
        notable_loan_applications._id as id,
        notable_loan_applications.listing_address_line1 as listing_address_line_1,
        notable_loan_applications.listing_address_line2 as listing_address_line_2,
        notable_loan_applications.listing_city as listing_city,
        notable_loan_applications.listing_state as listing_state,
        notable_loan_applications.listing_zip as listing_zip,
        notable_loan_applications.compass_agent_master_id as agent_id,
        notable_loans._id as loan_id,
        notable_loans.is_ach_only as is_ach,
        notable_loans.slug as key,
        notable_loans.principal as loan_principal,
        concat(
            notable_loan_applications.first_name,
            ' ',
            notable_loan_applications.last_name
        ) as applicant,
        notable_loan_applications.program as program
    from
        notable_loan_applications
        inner join notable_loans on notable_loan_applications._id = notable_loans.loan_application_id
    where
        notable_loans._id is not null
        and notable_loans.status != 'EXPIRED'
        and notable_loans.program = 'compass'
        and notable_loan_applications.compass_agent_master_id not in ('AJB00006666')
),
external_ledger as (
    select
        ledger_cleared.transaction_id,
        ledger_cleared.date,
        ledger_cleared.ledger_item_type,
        transaction_type.value as transaction_type,
        ledger_cleared.debit,
        ledger_cleared.credit,
        loan_applications.key,
        loan_applications.loan_principal,
        loan_applications.is_ach,
        loan_applications.agent_id,
        loan_applications.applicant,
        loan_applications.listing_address_line_1,
        loan_applications.listing_address_line_2,
        loan_applications.listing_city,
        loan_applications.listing_state,
        loan_applications.listing_zip,
        to_char(now(), 'MM/DD/YYYY HH:MM') as report_generated_at
    from
        ledger_cleared
        inner join loan_applications on ledger_cleared.loan_id = loan_applications.loan_id,
        lateral (
            select
                case
                    when ledger_cleared.method = 'direct'
                    and is_ach = 'false' then 'CASH_ADVANCE'
                    when ledger_cleared.method = 'direct' then 'ACH_LOAN'
                    when ledger_cleared.method = 'card' then 'CARD'
                    when ledger_cleared.method = 'check' then 'CHECK'
                end as value
        ) as transaction_type
)
select
    *
from
    external_ledger;

--effective date change
with ledger_cleared as(
    select
    distinct on (transaction_id) ledger.transaction_id,
    ledger.loan_id,
    ledger.method,
    to_char(effective_date,'MM/DD/YYYY') as date,
    transaction_type as ledger_item_type,
    debit / 100 as debit,
    credit / 100 as credit
from
    ledger
    left join (
        select
            distinct on (transaction_id) transaction_id,
            created_at
        from
            ledger
        where
            party = 'lender'
            and status = 'voided'
        order by
            transaction_id,
            created_at asc
    ) ledger_initial on ledger.transaction_id = ledger_initial.transaction_id and ledger.created_at<ledger_initial.created_at
where
    party = 'lender'
    and status = 'cleared'
    and ledger_initial.transaction_id is null
order by
    ledger.transaction_id,
    ledger.created_at desc
),
loan_applications as (
    select
        notable_loan_applications._id as id,
        notable_loan_applications.listing_address_line1 as listing_address_line_1,
        notable_loan_applications.listing_address_line2 as listing_address_line_2,
        notable_loan_applications.listing_city as listing_city,
        notable_loan_applications.listing_state as listing_state,
        notable_loan_applications.listing_zip as listing_zip,
        notable_loan_applications.compass_agent_master_id as agent_id,
        notable_loans._id as loan_id,
        notable_loans.is_ach_only as is_ach,
        notable_loans.slug as key,
        notable_loans.principal as loan_principal,
        concat(
            notable_loan_applications.first_name,
            ' ',
            notable_loan_applications.last_name
        ) as applicant,
        notable_loan_applications.program as program
    from
        notable_loan_applications
        inner join notable_loans on notable_loan_applications._id = notable_loans.loan_application_id
    where
        notable_loans._id is not null
        and notable_loans.status != 'EXPIRED'
        and notable_loans.program = 'compass'
        and notable_loan_applications.compass_agent_master_id not in ('AJB00006666')
),
external_ledger as (
    select
        ledger_cleared.transaction_id,
        ledger_cleared.date,
        ledger_cleared.ledger_item_type,
        transaction_type.value as transaction_type,
        ledger_cleared.debit,
        ledger_cleared.credit,
        loan_applications.key,
        loan_applications.loan_principal,
        loan_applications.is_ach,
        loan_applications.agent_id,
        loan_applications.applicant,
        loan_applications.listing_address_line_1,
        loan_applications.listing_address_line_2,
        loan_applications.listing_city,
        loan_applications.listing_state,
        loan_applications.listing_zip,
        to_char(now(), 'MM/DD/YYYY HH:MM') as report_generated_at
    from
        ledger_cleared
        inner join loan_applications on ledger_cleared.loan_id = loan_applications.loan_id,
        lateral (
            select
                case
                    when ledger_cleared.method = 'direct'
                    and is_ach = 'false' then 'CASH_ADVANCE'
                    when ledger_cleared.method = 'direct' then 'ACH_LOAN'
                    when ledger_cleared.method = 'card' then 'CARD'
                    when ledger_cleared.method = 'check' then 'CHECK'
                end as value
        ) as transaction_type
)
select
    *
from
    external_ledger;
    
    
    
    ---tests
    
    select
        distinct on (transaction_id) ledger.*
    from
        ledger left join (select distinct on (transaction_id) transaction_id,created_at from ledger where
    party='lender'
     and
    status ='voided' order by transaction_id,created_at asc) ledger_initial
        on ledger.transaction_id=ledger_initial.transaction_id 
        --and ledger.created_at<ledger_initial.created_at
    where
    party='lender'
    -- and
    --status ='cleared'
   -- and ledger_initial.transaction_id is null 

-- and ledger.transaction_id='fd05da6a-1d47-4f39-90fc-9dd12af38e82'
--and loan_id='5dca08c442438d0016e37557'
--and ledger.transaction_id='00432079-204b-4263-b109-b1fd5a2cccaf'  
and ledger.transaction_id='4a5992d3-6633-4317-b0d5-d0e687aff218'
order by
        ledger.transaction_id,
        ledger.created_at desc;
        
  ---old code
  
with initial_transaction as
(
    select distinct on (transaction_id) created_at, transaction_id
    from ledger
    where party = 'lender' 
    order by transaction_id, created_at asc
),
updated_transaction as(
    select distinct on (transaction_id) *
    from ledger
    where party = 'lender' 
    order by transaction_id, created_at desc
),
loan_apps as (
    select 
    notable_loan_applications._id as id,
    notable_loan_applications.listing_address_line1 as listing_address_line_1,
    notable_loan_applications.listing_address_line2 as listing_address_line_2,
    notable_loan_applications.listing_city as listing_city,
    notable_loan_applications.listing_state as listing_state,
    notable_loan_applications.listing_zip as listing_zip,
    notable_loan_applications.compass_agent_master_id as agent_id,
    notable_loans._id as loan_id,
    concat(notable_loan_applications.first_name, ' ', notable_loan_applications.last_name) as applicant,
    notable_loan_applications.program as program
    from notable_loan_applications
    inner join notable_loans on notable_loan_applications._id = notable_loans.loan_application_id
    where not notable_loans._id is null
),
notable_loans as (
    select notable_loans.slug as key,
    notable_loans.principal as loan_principal, 
    notable_loans.is_ach_only as is_ach,
    notable_loans._id as id
    from notable_loans
    where notable_loans.status != 'EXPIRED'
)


select 
    to_char(transaction_date.value, 'MM/DD/YYYY') as date,
    --to_char(transaction_date.value, 'YY-MM') as "YY-MM", 
    --(initial_transaction.created_at)::timestamp,
    --(updated_transaction.value_date)::timestamp,
    --updated_transaction.external_post_date::timestamp,
    --updated_transaction.method,
    --updated_transaction.method = 'direct' and updated_transaction.external_post_date is not null as "useExternalPostDate",
    updated_transaction.transaction_type as ledger_item_type,
    transaction_type.value as transaction_type,
    updated_transaction.debit/100  as debit, 
    updated_transaction.credit/100 as credit,
    notable_loans.key,
    notable_loans.loan_principal, 
    notable_loans.is_ach,
    loan_applications.agent_id,
    loan_applications.applicant, 
    loan_applications.listing_address_line_1,
    loan_applications.listing_address_line_2,
    loan_applications.listing_city,
    loan_applications.listing_state,
    loan_applications.listing_zip,
    to_char(now(),'MM/DD/YYYY HH:MM') as report_generated_at
from updated_transaction
inner join loan_apps as loan_applications on updated_transaction.loan_id = loan_applications.loan_id
inner join notable_loans on updated_transaction.loan_id = notable_loans.id
inner join initial_transaction on updated_transaction.transaction_id = initial_transaction.transaction_id,
lateral (
    select case
        when updated_transaction.method = 'direct' and notable_loans.is_ach = 'false' 
            then 'CASH_ADVANCE'
        when updated_transaction.method = 'direct' 
            then 'ACH_LOAN'
        when updated_transaction.method = 'card' 
            then 'CARD' 
        when updated_transaction.method = 'check' 
            then 'CHECK'
    end as value
) as transaction_type,

lateral (
    select case 
        -- when external post date is present use it as it's entered manually by accounting and operations team members
        when updated_transaction.method = 'direct' and updated_transaction.external_post_date is not null 
            then updated_transaction.external_post_date
        when  updated_transaction.method = 'direct'
            then updated_transaction.value_date 
        else initial_transaction.created_at
        end as value
) as transaction_date

where 
    ((updated_transaction.status = 'cleared' and updated_transaction.method = 'direct')
    or (updated_transaction.status = 'cleared' and updated_transaction.method = 'check')
    or (updated_transaction.status != 'voided' and updated_transaction.method = 'card'))
   -- [[and (transaction_date.value)::timestamp < ({{endDate}})::timestamp]]
    and loan_applications.program='compass'
    --and loans.data->>'slug' = '24E78A' -- Card disbursement shoud be on 2020-12-18
    --and loans.data->>'slug' = '23XE47' -- ACH repayment should be on 2020-12-18
    --and loans.data->>'slug' = 'CX4G4U' -- Last transaction  before 12-19 is a refund (a negative disbursement) not a repayment
    --and loans.data->>'slug' = '26D26P'
    --and loans.data->>'slug' = '76HK96' -- according to the weekly the amount should be $5,616.84 when pulling for 2020-12-26,
                --that's a timing break because the external_post_date is actually 12/27/2020
    --and loans.data->>'slug' = '9GN34W' should have 63.59 
    --and loans.data->>'slug' = 'MT29RH'
    --and (loans.data->>'slug' = 'Y46VT7' or loans.data->>'slug' = '38RH6F') -- sample data
    -- remove test loans
    and notable_loans.id='5dca08c442438d0016e37557'
order by
    initial_transaction.created_at asc