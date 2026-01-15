{#

    This macro provides a cross-database compatible way to extract a substring
    using regular expressions. Uses regexp_substr for most databases and
    regexp_extract for DuckDB.

#}db

{%- macro regexp_substr(string, pattern, position=1, occurrence=1, match_type='') -%}

    {{ return(adapter.dispatch('regexp_substr')(string, pattern, position, occurrence, match_type)) }}

{%- endmacro -%}

{%- macro duckdb__regexp_substr(string, pattern, position, occurrence, match_type) -%}

    regexp_extract( {{ string }}, {{ pattern }} )

{%- endmacro -%}

{%- macro default__regexp_substr(string, pattern, position, occurrence, match_type) -%}

    regexp_substr( {{ string }}, {{ pattern }}, {{ position }}, {{ occurrence }}, {{ match_type }} )

{%- endmacro -%}

{%- macro bigquery__regexp_substr(string, pattern, position, occurrence, match_type) -%}

    regexp_extract_all( {{ string }}, {{ pattern }} )[safe_offset(0)]

{%- endmacro -%}

{%- macro redshift__regexp_substr(string, pattern, position, occurrence, match_type) -%}

    regexp_substr( {{ string }}, {{ pattern }}, {{ position }}, {{ occurrence }} )

{%- endmacro -%}

{%- macro snowflake__regexp_substr(string, pattern, position, occurrence, match_type) -%}

    regexp_substr( {{ string }}, {{ pattern }}, {{ position }}, {{ occurrence }}, {{ match_type }} )

{%- endmacro -%}
