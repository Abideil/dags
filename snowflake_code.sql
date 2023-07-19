
create or replace table raw_data(
  raw_col VARIANT
);

create or replace table struct_data(
  id INT
, name VARCHAR
, year INT
, metacritic_rating INT
, reviewer_rating INT
, positivity_ratio NUMBER(20,15)
, to_beat_main NUMBER(7,2)
, to_beat_extra NUMBER(7,2)
, to_beat_completionist NUMBER(7,2)
, extra_content_length NUMBER(20,15)
, tags VARCHAR);


create or replace table rating_by_year(
  year INT
, max_mr INT
, max_rr INT);


CREATE OR REPLACE FUNCTION PARSE_CSV(CSV STRING, DELIMITER STRING, QUOTECHAR STRING)
RETURNS TABLE (V VARIANT)
LANGUAGE PYTHON
RUNTIME_VERSION=3.8
HANDLER='CsvParser'
AS $$
import csv

class CsvParser:
    def __init__(self):
        # Allow fields up to the VARCHAR size limit
        csv.field_size_limit(16777216)
        self._isFirstRow = True
        self._headers = []

    def process(self, CSV, DELIMITER, QUOTECHAR):
        # If the first row in a partition, store the headers
        if self._isFirstRow:
            self._isFirstRow = False
            # csv.reader to split up the headers
            reader = csv.reader([CSV], delimiter=DELIMITER, quotechar=QUOTECHAR)
            # Convert field names to upper case for consistency
            self._headers = list(map(lambda h: h.upper(), list(reader)[0]))
        else:
            # A DictReader allows us to provide the headers as a parameter
            reader = csv.DictReader(
                [CSV],
                fieldnames=self._headers,
                delimiter=DELIMITER,
                quotechar=QUOTECHAR,
            )
            for row in reader:
                # CSV are often sparse because each record has every field
                # Remove empty values to improve performance
                res = { k:v for (k,v) in row.items() if v }
                yield (res,)
$$;


-- Create a file format that won't split up records by a delimiter
CREATE FILE FORMAT IF NOT EXISTS TEXT_FORMAT 
TYPE = 'CSV' 
FIELD_DELIMITER = NONE
SKIP_BLANK_LINES = TRUE
ESCAPE_UNENCLOSED_FIELD = NONE;