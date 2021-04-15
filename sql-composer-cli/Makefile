# override with TARGET=release to test against release builds
TARGET=debug


# Potentially change/overridable.  YMMV:
# TQL is relative to COMPOSER_DIR
TQL?=src/tests/values/double-include.tql
BIND=[a: ['a_value'], b: ['b_value'], c: ['c_value'], d: ['d_value'], e: ['e_value'], f: ['f_value']]
VERBOSE=-vvv

# No user servicable parts:
COMPOSER_DIR=../sql-composer
SQLC=../target/${TARGET}/sqlc
EMPTY=
SPACE= $(EMPTY) $(EMPTY)
COMMA= ,

### Read the passwords and ports into Makefile variables
include ../.env

.PHONY: default
.PHONY: all mysql postgres sqlite
.PHONY: cargo_build
.PHONY: sqlc

default:
	@echo "Available targets are mysql, sqlite, postgres or all. Targets may be mixed."
	@echo
	@echo "Tests are run against debug target, override with make TARGET=release ..."

all: sqlite mysql postgres

# empty @true recipe is to avoid "nothing to be done for $target"
# messages when building multiple targets.
mysql sqlite postgres: sqlc
	@true

cargo_build:
	cargo build ${FEATURES} ${CARGO_TARGET}

# must cd into composer dir for double_include to find included files from relative paths
sqlc: cargo_build
	@cd ${COMPOSER_DIR} && for uri in ${URI}; \
	  do echo $${uri%%:*}:                    \
	  && echo '  ${SQLC} query \\'            \
	  && echo "  --uri $$uri \\"              \
	  && echo '  --path "${TQL}" \\'          \
	  && echo '  --bind "${BIND}" ${VERBOSE}' \
	  && ${SQLC} query --uri "$$uri" --path "${TQL}" --bind "${BIND}" ${VERBOSE} \
	  && echo ;\
	done


###########
# Configure all the variable substitution to get this to work
#   * Set up Feature and URI values for each test target
#   * if multiple targets are selected, they will be run in the order listed here.
#   * TARGET debug/release handling
#   * FEATURES parsing

# sqlite
ifneq (, ${filter sqlite all,${MAKECMDGOALS}})
  FEATURE += dbd-rusqlite
  URI += sqlite://:memory:
endif

# mysql
ifneq (, ${filter mysql all,${MAKECMDGOALS}})
  FEATURE += dbd-mysql
  URI += mysql://runner:${MYSQL_DATABASE_PASS}@0.0.0.0:${MYSQL_DATABASE_PORT}/sql_composer
endif

# postgres
ifneq (, ${filter postgres all,${MAKECMDGOALS}})
  FEATURE += dbd-postgres
  URI += postgres://runner:${PG_DATABASE_PASS}@0.0.0.0:${PG_DATABASE_PORT}/sql_composer
endif

# special case for all
ifneq (, ${filter all,${MAKECMDGOALS}})
  FEATURE = all
endif

# poor ergonomics of cargo features:
# * --features requires a single argument of a comma separated list of features
# * --features and --all-features are mutually exclusive
ifeq (${FEATURE},all)
  FEATURES = --all-features
else
  FEATURES = --features ${subst ${SPACE},${COMMA},${FEATURE}}
endif

ifeq (${TARGET},release)
  CARGO_TARGET=--release
endif
