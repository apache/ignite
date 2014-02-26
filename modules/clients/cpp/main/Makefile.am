ACLOCAL_AMFLAGS = "-Im4"

include aminclude.am

SUBDIRS = . include $(TESTS_DIR)
DIST_SUBDIRS = . include $(TESTS_DIR)

AM_CPPFLAGS = -I$(srcdir)/include -DGRIDGAIN_EXPORT -DBOOST_SPIRIT_THREADSAFE
AM_CXXFLAGS = -Wall -std=c++0x
LIB_LDFLAGS = -no-undefined -version-info 1

COMMON_SRC = src/model/gridclientuuid.cpp src/model/gridclientvariant.cpp \
    src/impl/gridclientcomputeprojection.cpp src/impl/gridclientconfiguration.cpp src/impl/gridclientdataconfiguration.cpp src/impl/gridclientdatametrics.cpp src/impl/gridclientdataprojection.cpp src/impl/gridclientfactory.cpp src/impl/gridclientimpl.cpp src/impl/gridclientnode.cpp src/impl/gridclientpartionedataaffinity.cpp src/impl/gridclientprojectionimpl.cpp src/impl/gridclientprotocolconfiguration.cpp src/impl/gridclientrandombalancer.cpp src/impl/gridclientroundrobinbalancer.cpp src/impl/gridclientrandomrouterbalancer.cpp src/impl/gridclienttopology.cpp src/impl/gridfuture.cpp src/impl/cmd/gridclientcommandexecutorfactory.cpp src/impl/cmd/gridclienthttpcommandexecutor.cpp src/impl/cmd/gridclienttcpcommandexecutor.cpp src/impl/cmd/gridechocommand.cpp src/impl/connection/gridclientconnection.cpp src/impl/connection/gridclientconnectionpool.cpp src/impl/connection/gridclienthttpconnection.cpp src/impl/connection/gridclienttcpconnection.cpp \
    src/impl/hash/gridbytearrayshasheableobject.cpp src/impl/hash/gridconsistenthashimpl.cpp src/impl/hash/griddoublehasheableobject.cpp src/impl/hash/gridfloathasheableobject.cpp src/impl/hash/gridstringhasheableobject.cpp src/impl/hash/gridclientwidestringhasheableobject.cpp src/impl/hash/gridvarianthasheableobject.cpp src/impl/hash/md5.cpp \
    src/impl/marshaller/json/gridclientjsonmarshaller.cpp src/impl/marshaller/protobuf/ClientMessages.pb.cpp src/impl/marshaller/protobuf/gridclientprotobufmarshaller.cpp src/impl/marshaller/protobuf/gridclientprotobufutils.cpp \
    src/impl/utils/gridclientlog.cpp src/impl/utils/gridutil.cpp src/impl/utils/gridthreadpool.cpp

lib_LTLIBRARIES = libgridgain-client.la
libgridgain_client_la_SOURCES = $(COMMON_SRC)
libgridgain_client_la_LIBADD = -lboost_system -lboost_thread -lcrypto -lprotobuf -lssl -lboost_program_options
libgridgain_client_la_LDFLAGS = $(LIB_LDFLAGS) -L/usr/local/lib -release $(PACKAGE_VERSION)

EXTRA_DIST=$(wildcard src/*.cpp) $(wildcard include/*.hpp)

pkgconfigdir = $(libdir)/pkgconfig
pkgconfig_DATA = gridgain-client.pc

clean-local:
	$(RM) *.gcno *.gcda

clean-docs:
	$(RM) $(DX_CLEANFILES)
