export GO111MODULE=on

all: juicefs

REVISION := $(shell git rev-parse --short HEAD 2>/dev/null)
REVISIONDATE := $(shell git log -1 --pretty=format:'%cd' --date short 2>/dev/null)
PKG := github.com/juicedata/juicefs/pkg/version
# The Plori profile keeps two metadata engines: `redis` (shared volume) and
# `sqlite3` (per-Agent volume, PLO-319). `sqlite_omit_load_extension` is a
# mattn/go-sqlite3 tag that compiles the amalgamation with
# -DSQLITE_OMIT_LOAD_EXTENSION, removing `sqlite3_enable_load_extension` and the
# `load_extension()` SQL function, so a metadata DB cannot load a shared object.
PLORI_TAGS := plori,sqlite_omit_load_extension,nogateway,nowebdav,nocos,nobos,nohdfs,noibmcos,noobs,nooss,noqingstor,nosftp,noswift,noazure,nogs,noufile,nob2,nonfs,nodragonfly,nomysql,nopg,notikv,nobadger,noetcd,nocifs,nostorj,noqiniu,notos,noks3
# SQLite is cgo. Set it explicitly so a toolchain that defaults CGO_ENABLED to 0
# fails the build instead of silently producing a binary without SQLite.
PLORI_CGO := CGO_ENABLED=1
GCFLAGS =
LDFLAGS =
BUILD ?= release
ifneq ($(strip $(REVISION)),) # Use git clone
	LDFLAGS += -X $(PKG).revision=$(REVISION) \
		   -X $(PKG).revisionDate=$(REVISIONDATE)
endif
ifneq ($(strip $(VERSION)),)
	LDFLAGS += -X $(PKG).version=$(VERSION)
endif

ifeq ($(BUILD),release)
	LDFLAGS += -s -w
else ifeq ($(BUILD),debug)
	GCFLAGS := all=-N -l
endif

SHELL = /bin/sh

ifdef STATIC
	LDFLAGS += -linkmode external -extldflags '-static'
	CC = /usr/bin/musl-gcc
	export CC
endif

juicefs: Makefile cmd/*.go pkg/*/*.go go.*
	go version
	go build -gcflags="$(GCFLAGS)" -ldflags="$(LDFLAGS)" -o juicefs .

juicefs.cover: Makefile cmd/*.go pkg/*/*.go go.*
	go version
	go build -gcflags="$(GCFLAGS)" -ldflags="$(LDFLAGS)" -cover -o juicefs .

juicefs.lite: Makefile cmd/*.go pkg/*/*.go
	go build -tags nogateway,nowebdav,nocos,nobos,nohdfs,noibmcos,noobs,nooss,noqingstor,nosftp,noswift,noazure,nogs,noufile,nob2,nonfs,nodragonfly,nosqlite,nomysql,nopg,notikv,nobadger,noetcd,nocifs,nostorj,noqiniu,notos,noks3 \
		-gcflags="$(GCFLAGS)" -ldflags="$(LDFLAGS)" -o juicefs.lite .

juicefs.plori: Makefile cmd/*.go pkg/*/*.go go.*
	go version
	$(PLORI_CGO) go build -trimpath -buildvcs=true -tags "$(PLORI_TAGS)" \
		-gcflags="$(GCFLAGS)" -ldflags="$(LDFLAGS)" -o juicefs.plori .

juicefs.plori.scan: Makefile cmd/*.go pkg/*/*.go go.*
	$(PLORI_CGO) go build -trimpath -buildvcs=true -tags "$(PLORI_TAGS)" \
		-gcflags="$(GCFLAGS)" -ldflags="$(filter-out -s -w,$(LDFLAGS))" -o juicefs.plori.scan .

test.plori.profile:
	$(PLORI_CGO) go run -tags "$(PLORI_TAGS)" ./hack/plori-profile

test.plori.benchmark:
	python3 -m unittest discover -s hack/plori-benchmark -p 'test_*.py'

test.plori.backup:
	$(PLORI_CGO) go test -count=1 -v -tags "$(PLORI_TAGS)" ./pkg/vfs/ -run TestBackupPloriProfile

# The SQLite PRAGMA contract on the default build. test.plori.meta below runs
# the same tests, and the rest of ./pkg/meta, under the release tag set.
#
# It is also ./pkg/meta's only DEFAULT-build compile gate, and PLO-429 made that
# load-bearing: pkg/meta/plori_trash.go (the one trash-usage walk) and its test
# carry no `plori` tag, because plori-runtime's services/storage-worker links
# this fork on a plain build and needs PloriMeasureTrash there. `go test` builds
# the whole package and its test files before -run selects anything, so putting
# that tag back turns this target red instead of quietly removing a symbol the
# other repo compiles against.
test.plori.sqlite:
	$(PLORI_CGO) go test -count=1 -v ./pkg/meta/ -run TestSQLitePragma

# ./pkg/meta under the exact tag set the release binary is built with, so the
# SQLite engine is exercised by the metadata engine's own shared test body
# (testMeta) and not only by the CI lifecycle smoke.
#
# Needs a Redis server on 127.0.0.1:6379 for the Redis half of the suite.
# SKIP_NON_CORE is upstream's own gate for the tests that need KeyDB, a Redis
# cluster or PostgreSQL. The two skipped tests hardcode metadata engines the
# Plori profile removes (badger, tikv); NewClient calls logger.Fatalf on an
# unknown driver, so either one aborts the whole test binary instead of failing
# just itself. Everything else is expected to pass: keep this a skip list, not
# a -run allowlist, so a new upstream test is picked up by default.
#
# ./pkg/plori/... is NOT here. PLO-320 put it in this target because at the time
# it was the only gate that ran anything under the release tag set; PLO-368's
# test.plori.unit below is now that gate for every non-meta package, and listing
# ./pkg/plori in both would run it twice. Keep it in one place -- and this is
# the wrong one: SKIP_NON_CORE and the -skip regex above name ./pkg/meta tests.
test.plori.meta:
	SKIP_NON_CORE=true $(PLORI_CGO) go test -count=1 -timeout 20m \
		-tags "$(PLORI_TAGS)" -skip '^TestLoadDump$$|^TestLoadDumpV2$$' ./pkg/meta/

# ./pkg/chunk and ./pkg/vfs under the release tag set: the writeback store, the
# durability barrier and the VFS the plori-mount supervisor is built on.
#
# Both packages need an object storage and (for the VFS) a metadata engine, and
# the harness used to reach for the two backends the profile excludes: `mem`
# resolved to a nil ObjectStorage and crashed the binary on the first upload,
# and `memkv://` hit logger.Fatalf in meta.NewClient and killed it outright
# (PLO-368). The test doubles now pick `file` and sqlite3, both in the profile.
#
# ./pkg/plori is here for the other half of the same gap. Every file in it is
# `//go:build plori` EXCEPT ./pkg/plori/mountspec, which is the plori-mount wire
# contract and carries no tag on purpose (PLO-395: the other end of that
# contract, plori-runtime's storage-worker, has to decode it on a plain build).
# So this target is the one gate for the tagged half -- the supervisor (PLO-321)
# and the restore/repair code (PLO-320) -- and `...` picks up whatever lands
# under it next; mountspec is gated on the default build by test.plori.upstream
# as well, which is where its own tests have to pass. See the note on
# test.plori.meta above.
#
# Needs the same Redis server on 127.0.0.1:6379 as test.plori.meta for the Redis
# rows of the readdir engine matrix. TestBackupPloriProfile skips itself unless
# PLORI_TEST_META_URL and PLORI_TEST_BLOB_URL are set; test.plori.backup is the
# step that sets them.
test.plori.unit:
	$(PLORI_CGO) go test -count=1 -timeout 20m \
		-tags "$(PLORI_TAGS)" ./pkg/chunk/... ./pkg/vfs/... ./pkg/plori/...
# PLO-322: the object-credential hygiene audit has to live in ./cmd, because the
# surfaces it greps only exist there -- the meta.Format a volume is formatted
# with, the SQLite database that Format is stored in (and therefore every LTX
# frame made out of it), and the plori-mount flag set that becomes the worker's
# argv. The rest of ./cmd's suite needs a Redis and a real object store, so this
# names its tests rather than running the package.
	$(PLORI_CGO) go test -count=1 -timeout 5m -tags "$(PLORI_TAGS)" ./cmd/ \
		-run 'Credential|TestTheInMemoryFormat|TestNoCommandLineFlag|TestTheEnvironmentPath|TestTheTrashIsNotWalked|TestAFailedTrashWalk'

# Upstream's own unit tests on the default build. Nothing in the Plori workflow
# ran a default-build `go test`, which is why pkg/chunk/cached_store_test.go sat
# uncompilable from fork PR #13 until PLO-358 tripped over it (PLO-368).
#
# ./pkg/meta is deliberately absent. Its default-build test package is already
# compiled, and TestSQLitePragma run, by test.plori.sqlite, and its whole body
# is run under the release tag set by test.plori.meta -- so it has the
# compile-rot gate ./pkg/chunk was missing. What the default build adds on top
# is the engines the Plori profile removes, and those want servers this job does
# not run: measured with only Redis up, TestTiKVClient burns ~100s of PD retries
# and fails, TestMySQLClient fails, and TestLoadDump's tikv row reaches
# logger.Fatalf in meta.NewClient (pkg/meta/interface.go:675) and kills the test
# binary outright -- 243s, red, for engines we do not ship.
#
# ./pkg/plori/mountspec is the one package under ./pkg/plori that is NOT
# `//go:build plori`: it is the plori-mount wire contract, and it is untagged
# precisely so the other end -- plori-runtime's services/storage-worker -- can
# decode a control-plane MountSpec without inheriting the release profile
# (PLO-395). That makes the default build its real build, so it is gated here.
# The rest of ./pkg/plori matches no packages at all without the tag and runs in
# test.plori.unit.
#
# SKIP_NON_CORE is upstream's gate for the cases that need a KeyDB or a Redis
# cluster. ./pkg/object's remote backends skip themselves when their credentials
# are absent (38 of them).
test.plori.upstream:
	SKIP_NON_CORE=true $(PLORI_CGO) go test -count=1 -timeout 25m \
		./pkg/chunk/... ./pkg/vfs/... ./pkg/object/... ./pkg/plori/mountspec/...

test.plori.security:
	python3 hack/verify_plori_security_test.py
	python3 hack/verify_plori_scope.py

test.java.security:
	python3 sdk/java/verify_ranger_dependencies_test.py
	python3 sdk/java/verify_ranger_dependencies.py sdk/java/pom.xml

juicefs.ceph: Makefile cmd/*.go pkg/*/*.go
	go build -tags ceph -gcflags="$(GCFLAGS)" -ldflags="$(LDFLAGS)" -o juicefs.ceph .

juicefs.fdb: Makefile cmd/*.go pkg/*/*.go
	go build -tags fdb -gcflags="$(GCFLAGS)" -ldflags="$(LDFLAGS)" -o juicefs.fdb .

juicefs.fdb.cover: Makefile cmd/*.go pkg/*/*.go
	go build -tags fdb -gcflags="$(GCFLAGS)" -ldflags="$(LDFLAGS)" -cover -o juicefs.fdb .

juicefs.gluster: Makefile cmd/*.go pkg/*/*.go
	go build -tags gluster -gcflags="$(GCFLAGS)" -ldflags="$(LDFLAGS)" -o juicefs.gluster .

juicefs.gluster.cover: Makefile cmd/*.go pkg/*/*.go
	go build -tags gluster -gcflags="$(GCFLAGS)" -ldflags="$(LDFLAGS)" -cover -o juicefs.gluster .

juicefs.all: Makefile cmd/*.go pkg/*/*.go
	go build -tags ceph,fdb,gluster -gcflags="$(GCFLAGS)" -ldflags="$(LDFLAGS)" -o juicefs.all .

# This is cross-compiling LoongArch in a Linux environment on x86_64 (amd64) or aarch64 (arm64) architecture.
# 1. Install LoongArch64 cross-compile toolchain from https://github.com/loong64/cross-tools
# 2. Set CC to your toolchain path.
# 3. Run `STATIC=1 make juicefs.loongarch` to build the LoongArch binary.
juicefs.loongarch: Makefile cmd/*.go pkg/*/*.go go.*
	CC=bin/loongarch64-unknown-linux-musl-cc CGO_ENABLED=1 GOARCH=loong64 go build -gcflags="$(GCFLAGS)" -ldflags="$(LDFLAGS)" -o juicefs .

# This is the script for compiling the Linux version on the MacOS platform.
# Please execute the `brew install FiloSottile/musl-cross/musl-cross` command before using it.
juicefs.linux:
	CGO_ENABLED=1 GOOS=linux GOARCH=amd64 CC=x86_64-linux-musl-gcc CGO_LDFLAGS="-static" go build -gcflags="$(GCFLAGS)" -ldflags="$(LDFLAGS)"  -o juicefs .

/usr/local/include/winfsp:
	sudo mkdir -p /usr/local/include/winfsp
	sudo cp hack/winfsp_headers/* /usr/local/include/winfsp

# This is the script for compiling the Windows version on the MacOS platform.
# Please execute the `brew install mingw-w64` command before using it.
juicefs.exe: /usr/local/include/winfsp cmd/*.go pkg/*/*.go
	GOOS=windows CGO_ENABLED=1 CC=x86_64-w64-mingw32-gcc \
	     go build -gcflags="$(GCFLAGS)" -ldflags="$(LDFLAGS)" -buildmode exe -o juicefs.exe .

# This is the script for compiling the Windows version on Windows platform.
# Please ensure mingw64 is in PATH and WinFsp SDK is installed at C:/WinFsp
_juicefs.exe:
	powershell -Command "$$env:PATH+=';C:\mingw64\bin'; $$env:CGO_ENABLED='1'; $$env:CGO_CFLAGS='-IC:/WinFsp/inc/fuse'; go build -ldflags='-s -w' -o juicefs.exe ."

.PHONY: snapshot release debug test test.plori.profile test.plori.benchmark test.plori.backup test.plori.sqlite test.plori.meta test.plori.unit test.plori.upstream test.plori.security test.java.security plori.tags

plori.tags:
	@printf '%s\n' "$(PLORI_TAGS)"

snapshot:
	docker run --rm --privileged \
		-e REVISIONDATE=$(REVISIONDATE) \
		-e PRIVATE_KEY=${PRIVATE_KEY} \
		-v ~/go/pkg/mod:/go/pkg/mod \
		-v `pwd`:/go/src/github.com/juicedata/juicefs \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-w /go/src/github.com/juicedata/juicefs \
		juicedata/golang-cross:v1.25.7-0 release --snapshot --clean

release:
	docker run --rm --privileged \
		-e REVISIONDATE=$(REVISIONDATE) \
		-e PRIVATE_KEY=${PRIVATE_KEY} \
		--env-file .release-env \
		-v ~/go/pkg/mod:/go/pkg/mod \
		-v `pwd`:/go/src/github.com/juicedata/juicefs \
		-v /var/run/docker.sock:/var/run/docker.sock \
		-w /go/src/github.com/juicedata/juicefs \
		juicedata/golang-cross:v1.25.7-0 release --clean

debug:
	$(MAKE) BUILD=debug all

test.meta.core:
	SKIP_NON_CORE=true go test -v -cover -count=1  -failfast -timeout=12m ./pkg/meta/... -args -test.gocoverdir="$(shell realpath cover/)"

test.meta.non-core:
	go test -v -cover -run='TestRedisCluster|TestPostgreSQLClient|TestLoadDumpSlow|TestEtcdClient|TestKeyDB' -count=1  -failfast -timeout=12m ./pkg/meta/... -args -test.gocoverdir="$(shell realpath cover/)"

test.pkg:
	go test -tags gluster -v -cover -count=1  -failfast -timeout=12m $$(go list ./pkg/... | grep -v /meta) -args -test.gocoverdir="$(shell realpath cover/)"

test.cmd:
	sudo JFS_GC_SKIPPEDTIME=1 MINIO_ACCESS_KEY=testUser MINIO_SECRET_KEY=testUserPassword GOMAXPROCS=8 go test -v -count=1 -failfast -cover -timeout=8m ./cmd/... -coverpkg=./pkg/...,./cmd/... -args -test.gocoverdir="$(shell realpath cover/)"

test.fdb:
	go test -v -cover -count=1  -failfast -timeout=4m ./pkg/meta/ -tags fdb -run=TestFdb -args -test.gocoverdir="$(shell realpath cover/)"

unit-random-test:
	echo "Using meta:$(meta), seed: $(seed), checks:${checks}, steps: $(steps)"
	go test ./pkg/meta/... -rapid.meta="$(meta)" -rapid.seed=$(seed) -rapid.checks=$(checks) -rapid.steps=$(steps) -run "TestFSOps" -v -failfast -count=1 -timeout=60m -cover -coverpkg=./pkg/... -args -test.gocoverdir="$(shell realpath cover/)"
