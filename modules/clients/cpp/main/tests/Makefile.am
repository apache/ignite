PARENT_DIR = $(srcdir)/..

ACLOCAL_AMFLAGS="-Im4"

AM_CPPFLAGS = -I$(PARENT_DIR)/include -DGRIDGAIN_EXPORT -DBOOST_SPIRIT_THREADSAFE
AM_CXXFLAGS = -Wall -fpermissive -std=c++0x

noinst_PROGRAMS = gridgain-tests gridgain-router-tests gridgain-threadpool-load-test gridgain-async-client-load-test \
    gridgain-cache-benchmark \
    router-example \
    gridgain-leak-test \
    gridgain-nio-echo-benchmark \
    gridgain-marshalling-benchmark

gridgain_nio_echo_benchmark_SOURCES = NIOEchoBenchmark.cpp
gridgain_nio_echo_benchmark_LDADD = -lboost_system -lboost_thread -lboost_program_options
gridgain_nio_echo_benchmark_LDFLAGS = -static-libtool-libs -L/usr/local/lib

gridgain_marshalling_benchmark_SOURCES = load/gridclientmarshallingbenchmark.cpp gridtestcommon.cpp
gridgain_marshalling_benchmark_LDADD = $(PARENT_DIR)/libgridgain-client.la -lboost_system -lboost_thread -lboost_program_options -lcrypto -lssl
gridgain_marshalling_benchmark_LDFLAGS = -static-libtool-libs -L/usr/local/lib

gridgain_tests_SOURCES = gridtestcommon.cpp gridclientaffinityselftest.cpp gridclientabstractselftest.cpp gridclientconsistenthashtest.cpp gridclientexampletest.cpp gridclientfactoryfixture.hpp gridclientfactoryselfttest.cpp gridthreadpoolselftest.cpp gridclientconnectionpoolselftest.cpp gridclientportabletest.cpp gridclienthashtest.cpp gridportablemarshallertest.cpp gridclientqueriestest.cpp \
    teamcity_messages.cpp teamcity_boost.cpp
gridgain_tests_LDADD = $(PARENT_DIR)/libgridgain-client.la -lboost_system -lboost_thread -lcrypto -lssl -lboost_unit_test_framework
gridgain_tests_LDFLAGS = -static-libtool-libs -L/usr/local/lib

gridgain_threadpool_load_test_SOURCES = load/gridthreadpoolloadtest.cpp gridtestcommon.cpp
gridgain_threadpool_load_test_LDADD = $(PARENT_DIR)/libgridgain-client.la -lboost_system -lboost_thread -lcrypto -lssl
gridgain_threadpool_load_test_LDFLAGS = -static-libtool-libs -L/usr/local/lib

gridgain_async_client_load_test_SOURCES = load/gridclientasyncloadtest.cpp gridtestcommon.cpp
gridgain_async_client_load_test_LDADD = $(PARENT_DIR)/libgridgain-client.la -lboost_system -lboost_thread -lcrypto -lssl
gridgain_async_client_load_test_LDFLAGS = -static-libtool-libs -L/usr/local/lib

gridgain_router_tests_SOURCES = gridtestcommon.cpp gridclientabstractselftest.cpp gridclientfactoryfixture.hpp gridclientrouterselftest.cpp\
    teamcity_messages.cpp teamcity_boost.cpp
gridgain_router_tests_CPPFLAGS = -I$(PARENT_DIR)/include -DGRIDGAIN_EXPORT -DGRIDGAIN_ROUTER_TEST -DBOOST_SPIRIT_THREADSAFE
gridgain_router_tests_LDADD = $(PARENT_DIR)/libgridgain-client.la -lboost_system -lboost_thread -lcrypto -lssl
gridgain_router_tests_LDFLAGS = -static-libtool-libs -L/usr/local/lib

router_example_SOURCES = integration/gridroutertest.cpp
router_example_LDADD = $(PARENT_DIR)/libgridgain-client.la -lboost_system -lboost_thread -lcrypto -lssl
router_example_LDFLAGS = -static-libtool-libs -L/usr/local/lib

gridgain_cache_benchmark_SOURCES = load/gridclientcachebenchmark.cpp gridtestcommon.cpp
gridgain_cache_benchmark_LDADD = $(PARENT_DIR)/libgridgain-client.la -lboost_system -lboost_thread -lcrypto -lssl
gridgain_cache_benchmark_LDFLAGS = -static-libtool-libs -L/usr/local/lib

gridgain_leak_test_SOURCES = gridtestcommon.cpp load/gridclientmemoryleaktest.cpp
gridgain_leak_test_LDADD = $(PARENT_DIR)/libgridgain-client.la -lboost_system -lboost_thread -lcrypto -lssl
gridgain_leak_test_LDFLAGS = -static-libtool-libs -L/usr/local/lib

EXTRA_DIST=$(wildcard *.cpp) $(wildcard src/*.cpp) $(wildcard include/*.hpp)

run-check: check
	./gridgain-tests -p

clean-local: clean-check
	$(RM) *.gcno *.gcda

clean-check:
	$(RM) $(gridgain_tests_OBJECTS)
