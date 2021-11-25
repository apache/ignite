# Distributed under the OSI-approved BSD 3-Clause License. Contains modified code from
# CMake distribution, see https://cmake.org/licensing for details.

# Define lists used internally
set(_odbc_include_paths)
set(_odbc_lib_paths)
set(_odbc_lib_names)
set(_odbc_required_libs_names)

### Try Windows Kits ##########################################################
if(WIN32)
    # List names of ODBC libraries on Windows
    if(NOT MINGW)
        set(ODBC_LIBRARY odbc32.lib)
    else()
        set(ODBC_LIBRARY libodbc32.a)
    endif()
    
    set(_odbc_lib_names odbc32;)

    # List additional libraries required to use ODBC library
    if(MSVC OR CMAKE_CXX_COMPILER_ID MATCHES "Intel")
        set(_odbc_required_libs_names odbccp32;ws2_32)
    elseif(MINGW)
        set(_odbc_required_libs_names odbccp32)
    endif()
endif()

### Try unixODBC or iODBC config program ######################################
if (UNIX)
    find_program(ODBC_CONFIG
            NAMES odbc_config iodbc-config
            DOC "Path to unixODBC config program")
    mark_as_advanced(ODBC_CONFIG)
endif()

if (UNIX AND ODBC_CONFIG)
    # unixODBC and iODBC accept unified command line options
    execute_process(COMMAND ${ODBC_CONFIG} --cflags
            OUTPUT_VARIABLE _cflags OUTPUT_STRIP_TRAILING_WHITESPACE)
    execute_process(COMMAND ${ODBC_CONFIG} --libs
            OUTPUT_VARIABLE _libs OUTPUT_STRIP_TRAILING_WHITESPACE)

    # Collect paths of include directories from CFLAGS
    separate_arguments(_cflags UNIX_COMMAND "${_cflags}")
    foreach(arg IN LISTS _cflags)
        if("${arg}" MATCHES "^-I(.*)$")
            list(APPEND _odbc_include_paths "${CMAKE_MATCH_1}")
        endif()
    endforeach()
    unset(_cflags)

    # Collect paths of library names and directories from LIBS
    separate_arguments(_libs UNIX_COMMAND "${_libs}")
    foreach(arg IN LISTS _libs)
        if("${arg}" MATCHES "^-L(.*)$")
            list(APPEND _odbc_lib_paths "${CMAKE_MATCH_1}")
        elseif("${arg}" MATCHES "^-l(.*)$")
            set(_lib_name ${CMAKE_MATCH_1})
            string(REGEX MATCH "odbc" _is_odbc ${_lib_name})
            if(_is_odbc)
                list(APPEND _odbc_lib_names ${_lib_name})
                if (${_lib_name} STREQUAL odbc)
                    list(APPEND _odbc_required_libs_names odbcinst)
                endif()
            else()
                list(APPEND _odbc_required_libs_names ${_lib_name})
            endif()
            unset(_lib_name)
        endif()
    endforeach()
    unset(_libs)
endif()

### Try unixODBC or iODBC in include/lib filesystems ##########################
if (UNIX AND NOT ODBC_CONFIG)
    # List names of both ODBC libraries and unixODBC
    set(_odbc_lib_names odbc;odbcinst;iodbc;unixodbc;)
endif()

### Find include directories ##################################################
find_path(ODBC_INCLUDE_DIR
        NAMES sql.h
        PATHS ${_odbc_include_paths})

if(NOT ODBC_INCLUDE_DIR AND WIN32)
    set(ODBC_INCLUDE_DIR "")
endif()

### Find libraries ############################################################
if(NOT ODBC_LIBRARY)
    find_library(ODBC_LIBRARY
            NAMES ${_odbc_lib_names}
            PATHS ${_odbc_lib_paths}
            PATH_SUFFIXES odbc)

    foreach(_lib IN LISTS _odbc_required_libs_names)
        find_library(_lib_path
                NAMES ${_lib}
                PATHS ${_odbc_lib_paths} # system paths or collected from ODBC_CONFIG
                PATH_SUFFIXES odbc)
        if(_lib_path)
            list(APPEND _odbc_required_libs_paths ${_lib_path})
        endif()
        unset(_lib_path CACHE)
    endforeach()
endif()

# Unset internal lists as no longer used
unset(_odbc_include_paths)
unset(_odbc_lib_paths)
unset(_odbc_lib_names)
unset(_odbc_required_libs_names)

### Set result variables ######################################################
set(_odbc_required_vars ODBC_LIBRARY)
if(NOT WIN32)
    list(APPEND _odbc_required_vars ODBC_INCLUDE_DIR)
endif()

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(ODBC DEFAULT_MSG ${_odbc_required_vars})

unset(_odbc_required_vars)

mark_as_advanced(ODBC_LIBRARY ODBC_INCLUDE_DIR)

set(ODBC_INCLUDE_DIRS ${ODBC_INCLUDE_DIR})
list(APPEND ODBC_LIBRARIES ${ODBC_LIBRARY})
list(APPEND ODBC_LIBRARIES ${_odbc_required_libs_paths})

### Import targets ############################################################
if(ODBC_FOUND)
    if(NOT TARGET ODBC::ODBC)
        if(IS_ABSOLUTE "${ODBC_LIBRARY}")
            add_library(ODBC::ODBC UNKNOWN IMPORTED)
            set_target_properties(ODBC::ODBC PROPERTIES
                    IMPORTED_LINK_INTERFACE_LANGUAGES "C"
                    IMPORTED_LOCATION "${ODBC_LIBRARY}")
        else()
            add_library(ODBC::ODBC INTERFACE IMPORTED)
            set_target_properties(ODBC::ODBC PROPERTIES
                    IMPORTED_LIBNAME "${ODBC_LIBRARY}")
        endif()
        set_target_properties(ODBC::ODBC PROPERTIES
                INTERFACE_INCLUDE_DIRECTORIES "${ODBC_INCLUDE_DIR}")

        if(_odbc_required_libs_paths)
            set_property(TARGET ODBC::ODBC APPEND PROPERTY
                    INTERFACE_LINK_LIBRARIES "${_odbc_required_libs_paths}")
        endif()
    endif()
endif()

unset(_odbc_required_libs_paths)
