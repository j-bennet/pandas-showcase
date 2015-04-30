import pandas as pd

# Read a fixed-width file
zp = pd.read_fwf(
    'data/ZIP.DAT',
    widths=[5, 2, 28, 1, 5, 7, 8, 3, 6, 1, 1, 4, 4, 3],
    names=['zip_code', 'state_code', 'city_name', 'type', 'county_fips',
           'lat', 'lon', 'area_code', 'fin_code', 'last_line',
           'facility', 'msa_code', 'pmsa_code', 'filler'],
    usecols=[0, 1, 2, 4, 5, 6, 7, 11, 12],
    converters={'zip_code': str, 'county_fips': str, 'area_code': str,
                'msa_code': str, 'pmsa_code': str},
    header=None,
    skiprows=2
)

# Filter out records without area code: FPO/APO, footer
zp = zp[~zp.area_code.isnull()]

ms = pd.read_fwf(
    'data/MSA.DAT',
    widths=[4, 4, 60, 2, 8],
    names=['msa_code', 'type', 'msa_name', 'cmsa', 'population'],
    usecols=[0, 1, 2, 3],
    converters={'msa_code': str, 'type': str, 'msa_name': str, 'cmsa_code': str},
    header=None
)

# Show top records
print zp.head(10)

# Sort
print zp.sort(columns=['zip_code']).head(10)

# Select subset of cols
print zp['zip_code']

# Select subset of rows
print zp[0:3]

# Select just one cols
print zp.loc[0:3, ['zip_code']]

# Select rows on condition
print zp[zp.zip_code == '90703']

# Select on "IN" condition
print zp[zp.zip_code.isin(['90401', '90703'])]

# Select all rows that have missing values (for sanity check)
print zp[zp.isnull().any(axis=1)]

# Save it as Excel: needs openpyxl==1.8.6 installed
zp.to_excel('data/zip.xlsx', sheet_name='zip_code')

# Join zip codes on MSA, to see MSA names
zms = zp.merge(ms, left_on="msa", right_on="code")

# Select only needed cols
zip_codes = zp[["zip_code", "state_code", "city_name", "msa_code", "area_code"]]

# Rename "msa" to "msa_code"
# zip_codes = zip_codes.rename(columns={'msa': 'msa_code'})

# Select only needed cols
msa_codes = ms[['msa_code', 'msa_name']]

# Rename "code" to "msa_code" and "name" to "msa_name"
# msa_codes = msa_codes.rename(columns={'code': 'msa_code', 'name': 'msa_name'})

"""
CREATE TABLE zip_code
(
  zip_code character varying(5) NOT NULL,
  state_code character varying(2) NOT NULL,
  city_name character varying(28) NOT NULL,
  area_code character varying(3) NOT NULL,
  msa_code character(4),
  CONSTRAINT zip_code_pkey PRIMARY KEY (zip_code, state_code, city_name)
)
WITH (
  OIDS=FALSE
);

CREATE TABLE msa
(
  msa_code character(4) not null,
  msa_name character(60) not null,
  CONSTRAINT msa_pkey PRIMARY KEY (msa_code)
)
WITH (
  OIDS=FALSE
);
"""

from sqlalchemy import create_engine

engine = create_engine('postgresql://username:pass@localhost:5432/playground')

zip_codes.to_sql('zip_code', engine, index=False, if_exists='append')
msa_codes.to_sql('msa', engine, index=False, if_exists='append')