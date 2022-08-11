with
report_meta as (
    select
        {{ orig_date_end_exc }}::date as period_cutoff,
        {{ orig_date_end_exc }}::date - interval '1 day' as period_end,
        current_date as generation_date
),
target_loans as (
    -- start with the desired loan set so the query planner can better restrict
    -- the set of snapshots it needs to work with, and speed up the query
    select notable_loans.* from notable_loans, report_meta
    where
        notable_loans.portfolio != 'NOTABLE_TEST'
        and notable_loans.program = 'better_home'
        [[ and notable_loans.slug = {{slug}}]]
),
most_recent_snapshot as (
    select
        distinct on(loan_snapshots.loan_id) loan_snapshots.*, round(disbursed_cents_cleared / 100.0,2) as credit_usage_dollars
    from
        loan_snapshots, report_meta, target_loans
    where
        target_loans._id = loan_snapshots.loan_id
        and timezone('America/New_York', loan_snapshots.created_at :: timestamptz) < report_meta.period_cutoff
    order by
        loan_snapshots.loan_id,
        loan_snapshots.created_at desc
),
previous_investor_tape_totals as (
    select
        loan_id,
        round((sum(coalesce(investor_purchases.principal, 0)) / 100.0), 2) as principal,
        round((sum(coalesce(investor_purchases.tape_new_refunds, 0)) / 100.0), 2) as refunds
    from
        investor_purchases, report_meta
    where 
        investor_purchases.purchase_date < report_meta.period_cutoff
    group by loan_id
),
gross_purchasable_disbursements as (
    select
        most_recent_snapshot.loan_id,
        most_recent_snapshot.credit_usage_dollars + coalesce(previous_investor_tape_totals.refunds,0) as dollars -- adds back the investor's share of refunds because at one point that amount was purchasable
    from most_recent_snapshot
    inner join previous_investor_tape_totals on previous_investor_tape_totals.loan_id = most_recent_snapshot.loan_id
),
new_purchasable_amount as (
    select
        gross_purchasable_disbursements.loan_id,
        greatest(
            gross_purchasable_disbursements.dollars
            - coalesce(previous_investor_tape_totals.principal,0)
        ,0.00) as dollars
    from gross_purchasable_disbursements
    inner join previous_investor_tape_totals on previous_investor_tape_totals.loan_id = gross_purchasable_disbursements.loan_id
),
nsf_repayments as ( 
select
    _id,
    loan_id,
    effective_date,
    amount,
    status,
    jpm_status,
    jpm_exceptions,
    payment_slug
from payments
join (
    select
        distinct on(payment_slug)
            payload ->> 'transactionStatus' as jpm_status,
            payload ->> 'exceptions' as jpm_exceptions,
            payment_slug
        from jpm_transaction_statuses
        order by payment_slug, created_at desc
) jpm_statuses
on jpm_statuses.payment_slug = payments.slug
where status = 'failed' and jpm_exceptions::text like '%INSUFFICIENT FUNDS%'
),
entry_impacts as (
    select
        ledger_entries.loan_id,
        ledger_entries.account,
        sum(round(ledger_entries.amount * ledger_accounts.normal_balance / 100.0,2)) as normalized_dollars
    from report_meta, ledger_entries
    join ledger_transactions on ledger_transactions._id = ledger_entries.ledger_transaction_id
    join ledger_accounts on ledger_accounts.name = ledger_entries.account
    where ledger_transactions.effective_date < report_meta.period_cutoff
    group by ledger_entries.loan_id, ledger_entries.account
),
balances as (
    select
        loan_id,
        jsonb_object_agg(account, normalized_dollars) as normalized
    from entry_impacts
    group by loan_id
),
total_investor_refunds as (
    select
        loan_id,
        coalesce((balances.normalized ->> 'investor_accumulated_refunds')::numeric,0) as dollars
    from
        balances
),
new_refunds as (
    select
        total_investor_refunds.loan_id,
        total_investor_refunds.dollars - previous_investor_tape_totals.refunds as dollars
    from total_investor_refunds
    join previous_investor_tape_totals on previous_investor_tape_totals.loan_id = total_investor_refunds.loan_id
)

select distinct
    -- report metadata
    to_char(report_meta.generation_date, 'yyyy-mm-dd') as report_generation_date,
    to_char(report_meta.period_end, 'yyyy-mm-dd') as period_end,

    -- identifiers
    concat(
        'https://notebook.concierge.notable-ops.com/admin/usersview/borrower_overview/?id=',
        notable_users._id
    ) as notebook_link,
    upper(notable_users.first_name) as first_name,
    upper(notable_users.last_name) as last_name,
    notable_loan_applications.better_home_external_id as external_id,
    target_loans.slug as loan_id,
    target_loans.status as loan_status,
    case
    when
    target_loans.status = 'REPAID'
    then TRUE
    end as is_repaid,
    case
    when
    target_loans.status = 'REPAID'
    then target_loans.repaid_date
    end as repaid_date,

    -- terms
    round(most_recent_snapshot.principal_cents / 100.0, 2) as credit_limit,
    round(most_recent_snapshot.disbursed_cents_cleared/100.0, 2) as Aggregate_Original_Principal_Balance,

    -- lifecycle
    case when most_recent_snapshot.days_past_due > 0 then
        most_recent_snapshot.days_past_due
    end as days_past_due,

    -- refunds
    total_investor_refunds.dollars as total_investor_refunds,
    previous_investor_tape_totals.refunds as previous_investor_refunds,
    new_refunds.dollars as outstanding_investor_refunds,

    -- nsf
    round(nsf_repayments.amount/100.00, 2) as nsf_repayments,
    
    -- purchase
    case when previous_investor_tape_totals.loan_id is not null then new_purchasable_amount.dollars end as additional_draws_to_purchase,
    
    --investor
    previous_investor_tape_totals.principal as investor_purchased_principal,
    coalesce((balances.normalized ->> 'investor_accumulated_interest')::numeric,0) as investor_purchased_interest,
    coalesce((balances.normalized ->> 'investor_accumulated_principal_repayments')::numeric,0) as _investor_accumulated_principal_repayments,
    coalesce((balances.normalized ->> 'investor_accumulated_interest_repayments')::numeric,0) as _investor_accumulated_interest_repayments,
    coalesce((balances.normalized ->> 'investor_accumulated_principal_repayments')::numeric,0)+ coalesce((balances.normalized ->> 'investor_accumulated_interest_repayments')::numeric,0) as investor_accumulated_principal_and_interest_repayments,

    -- qa
    coalesce((balances.normalized ->> 'principal')::numeric,0) as principal,
    coalesce((balances.normalized ->> 'writeoffs_principal')::numeric,0) as  writeoffs_principal,
    coalesce((balances.normalized ->> 'accumulated_principal_repayments')::numeric,0)+ coalesce((balances.normalized ->> 'writeoffs_principal')::numeric,0) as accumulated_principal_repayments,
    coalesce((balances.normalized ->> 'writeoffs_interest')::numeric,0) as writeoffs_interest,
    coalesce((balances.normalized ->> 'accumulated_interest_repayments')::numeric,0)+ coalesce((balances.normalized ->> 'writeoffs_interest')::numeric,0) as accumulated_interest_repayments,
    coalesce((balances.normalized ->> 'notable_earned_servicing_fee')::numeric,0) as notable_earned_servicing_fee
    
from
    report_meta,
    target_loans
    inner join notable_users on notable_users._id = target_loans.user_id
    inner join notable_loan_applications on notable_loan_applications._id = target_loans.loan_application_id
    inner join previous_investor_tape_totals on previous_investor_tape_totals.loan_id = target_loans._id
    left join most_recent_snapshot on most_recent_snapshot.loan_id = target_loans._id
    left join gross_purchasable_disbursements on gross_purchasable_disbursements.loan_id = target_loans._id
    left join new_purchasable_amount on new_purchasable_amount.loan_id = target_loans._id
    left join new_refunds on new_refunds.loan_id = target_loans._id
    left join total_investor_refunds on total_investor_refunds.loan_id = target_loans._id
    left join nsf_repayments on target_loans._id =nsf_repayments.loan_id
    -- qa
    left join balances on balances.loan_id = target_loans._id;
