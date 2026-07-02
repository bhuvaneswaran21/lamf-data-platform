

\copy lamf_src.scheme       FROM '/data/generated/schemes.csv'        CSV HEADER
\copy lamf_src.customer     FROM '/data/generated/customers.csv'      CSV HEADER
\copy lamf_src.mf_holding   FROM '/data/generated/holdings.csv'       CSV HEADER
\copy lamf_src.loan         FROM '/data/generated/loans.csv'          CSV HEADER
\copy lamf_src.disbursement FROM '/data/generated/disbursements.csv'  CSV HEADER
\copy lamf_src.repayment    FROM '/data/generated/repayments.csv'     CSV HEADER

SELECT 'customer' t, count(*) FROM lamf_src.customer
UNION ALL SELECT 'loan', count(*) FROM lamf_src.loan
UNION ALL SELECT 'mf_holding', count(*) FROM lamf_src.mf_holding;
