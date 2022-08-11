--CODE FOR ACCELERATION EVENTS
--create or replace view temp_acceleration_events as
select
    distinct loan_id,
    canceled,
    type as loan_type,
    cast(timezone('America/New_York', created_at) as date) as created_at,
    notes,
    to_char(acceleration_events.date, 'mm/dd/yyyy') as acceleration_event_date
from
    acceleration_events
where
    (
        (
            lower(notes) = 'automated import'
            and "date" <= current_date
        )
        or cast(timezone('America/New_York', created_at) as date) <= current_date
    )
    and canceled != true;

--CODE FOR UPSIZING
--create or replace view temp_upsizes as
select
    lpa.adjustment_amount,
    cast(
        timezone('America/New_York', lpa.created_at) as date
    ) as lpa_created_at,
    lpa.loan_id,
    timezone(
        'America/New_York',
        timezone('UTC',(l.authorized_at) :: date)
    ) as time_to_upsize,
    timezone('America/New_York', lpa.created_at) - timezone(
        'America/New_York',
        (l.authorized_at) :: timestamp
    ) as time_to_upsize
from
    loan_principal_adjustment lpa,
    notable_loans l
where
    lpa.loan_id = l._id
    and adjustment_amount > 0;

---CODE FOR LOAN_SNAPSHOTS

--create or replace view temp_loan_snapshots as
select
    distinct on (loan_snapshots.loan_id) loan_snapshots.loan_id as loan_id,
    loan_snapshots.status as status,
    timezone(
        'America/New_York',
        loan_snapshots.active_acceleration_event_date
    ) as acceleration_event_date,
    loan_snapshots.active_acceleration_event_type as acceleration_event_type,
    timezone(
        'America/New_York',
        loan_snapshots.first_disbursement_datetime
    ) as first_disbursement_datetime,
    timezone(
        'America/New_York',
        loan_snapshots.last_repayment_datetime
    ) as last_repayment_datetime,
    to_char(
        timezone(
            'America/New_York',
            loan_snapshots.last_repayment_datetime
        ),
        'MM/DD/YYYY'
    ) as last_repayment_date_est,
    loan_snapshots.repayment_status as repayment_status,
    timezone(
        'America/New_York',
        loan_snapshots.created_at :: timestamptz
    ) as created_at
from
    loan_snapshots
where
    timezone(
        'America/New_York',
        loan_snapshots.created_at :: timestamptz
    ) <= current_date;
    
    
-- create or replace view temp_loan_snapshots as
select
    distinct on (loan_snapshots.loan_id) loan_snapshots.loan_id as loan_id,
    loan_snapshots.status as status,
    timezone(
        'America/New_York',
        loan_snapshots.active_acceleration_event_date
    ) as acceleration_event_date,
    loan_snapshots.active_acceleration_event_type as acceleration_event_type,
    timezone(
        'America/New_York',
        loan_snapshots.first_disbursement_datetime
    ) as first_disbursement_datetime,
    timezone(
        'America/New_York',
        loan_snapshots.last_repayment_datetime
    ) as last_repayment_datetime,
    to_char(
        timezone(
            'America/New_York',
            loan_snapshots.last_repayment_datetime
        ),
        'MM/DD/YYYY'
    ) as last_repayment_date_est,
    loan_snapshots.repayment_status as repayment_status,
    timezone(
        'America/New_York',
        loan_snapshots.created_at :: timestamptz
    ) as created_at,
    to_char(
        case
            when loan_snapshots.active_acceleration_event_date < (l.origination_Date + interval '12 months') then loan_snapshots.active_acceleration_event_date
            else (l.origination_Date + interval '12 months')
        end,
        'MM-DD-YYYY'
    ) as date_added_to_collections
from
    notable_loans l
    left join loan_snapshots on l._id = loan_snapshots.loan_id
where
    timezone(
        'America/New_York',
        loan_snapshots.created_at :: timestamptz
    ) <= current_date;

