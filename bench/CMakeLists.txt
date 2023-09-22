project(bench)

enable_tnt_compile_flags()

include_directories("${PROJECT_SOURCE_DIR}/../connector/c/include")

add_executable(bench main.c)
set_source_files_compile_flags("TARANTOOL" main.c)
target_link_libraries(bench tntrpl tntnet tntsql tnt)

install (TARGETS bench DESTINATION bin)

