cmake_minimum_required(VERSION 3.15)
project(query_joiner)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_FLAGS "-Ofast")
link_libraries(-lpthread)

add_executable(query_joiner main.cpp array.h common.h pair.h metaprogramming.h relation_data.h relation_data.cpp
        report_utils.cpp report_utils.h utils.h utils.cpp relation_storage.h relation_storage.cpp
        joinable.cpp joinable.h tokenizer.h tokenizer.cpp command_interpreter.h command_interpreter.cpp parse.cpp parse.h
        intermediate_result.h intermediate_result.cpp task_scheduler.cpp task_scheduler.h queue.h query_executor.cpp query_executor.h scoped_timer.h)

target_link_libraries(query_joiner pthread)

add_executable(test_create_relation_from_file tests/test_create_relation_from_file.cpp relation_data.cpp relation_data.h
        file_manager.cpp file_manager.h joinable.h joinable.cpp report_utils.h report_utils.cpp)

add_executable(test_initialize_relations_and_queries tests/test_initialize_relations_and_queries.cpp
        command_interpreter.h command_interpreter.cpp relation_storage.cpp relation_storage.h utils.h utils.cpp
        stretchy_buf.h tokenizer.cpp tokenizer.h relation_data.h relation_data.cpp joinable.h joinable.cpp
        report_utils.h report_utils.cpp)

add_executable(test_command_interpreter tests/command_interpreter/command_interpreter_tests.cpp command_interpreter.h command_interpreter.cpp
        report_utils.h report_utils.cpp stretchy_buf.h utils.cpp utils.h tokenizer.cpp tokenizer.h joinable.h joinable.cpp)

add_executable(test_joinable tests/joinable_tests.cpp common.h joinable.cpp joinable.h report_utils.cpp report_utils.h)

add_executable(test_task_scheduler tests/test_task_scheduler.cpp task_scheduler.h task_scheduler.cpp report_utils.cpp
        report_utils.h queue.h)

add_executable(test_query_executor tests/test_query_executor.cpp
        array.h  common.h pair.h metaprogramming.h relation_data.h relation_data.cpp
        report_utils.cpp report_utils.h utils.h utils.cpp relation_storage.h relation_storage.cpp
        joinable.cpp joinable.h tokenizer.h tokenizer.cpp command_interpreter.h command_interpreter.cpp parse.cpp parse.h
        intermediate_result.h intermediate_result.cpp task_scheduler.cpp task_scheduler.h queue.h query_executor.cpp query_executor.h)

target_link_libraries(test_task_scheduler pthread)