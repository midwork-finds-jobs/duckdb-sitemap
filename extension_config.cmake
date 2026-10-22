# This file is included by DuckDB's build system. It specifies which extension to load

# Extension from this repo
duckdb_extension_load(sitemap
    SOURCE_DIR ${CMAKE_CURRENT_LIST_DIR}
    LOAD_TESTS
)

# DuckDB >= 2.0 builds but no longer links loaded extensions; link it so unittest can `require sitemap`
if(COMMAND duckdb_extension_statically_link)
  duckdb_extension_statically_link(sitemap)
endif()
