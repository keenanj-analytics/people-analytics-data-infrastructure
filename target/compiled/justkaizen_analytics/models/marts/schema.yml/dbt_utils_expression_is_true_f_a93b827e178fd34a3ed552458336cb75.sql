



select
    1
from `just-kaizen-ai`.`raw_marts`.`fct_compensation_parity`

where not(band_position between 0 and 1)

