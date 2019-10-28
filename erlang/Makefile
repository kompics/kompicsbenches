FULL = 1
PROJECT = erlang_benchmarks
PROJECT_DESCRIPTION = Erlang Benchmark implementations for the Kompics Benchmark Suite
PROJECT_VERSION = 0.1.0
PROJECT_MOD = erlang_benchmarks

# ERLANG_OTP = OTP-22.0.2

SP = 4

DOC_DEPS = edown
EDOC_OPTS = {doclet, edown_doclet}

#DEPS = grpc grpc_client gpb
DEPS = grpc gpb
dep_grpc = git https://github.com/Bathtor/grpc master
#dep_grpc = ln /Users/lkroll/Documents/Programming/grpc-erlang/grpc
#DEP_PLUGINS = grpc


#BUILD_DEPS = gpb
# no debug
ERLC_OPTS = +warn_export_vars +warn_shadow_vars +warn_obsolete_guard
# with debug
# ERLC_OPTS = +debug_info +warn_export_vars +warn_shadow_vars +warn_obsolete_guard

DIALYZER_OPTS = -Werror_handling -Wrace_conditions -Wunmatched_returns

include erlang.mk
