FAKTORY_HOST=127.0.0.1
FAKTORY_PORT=7419
FAKTORY_PORT_UI=7420

.PHONY: check
check:
	cargo fmt --check
	cargo clippy
	cargo d --no-deps --all-features
	cargo +nightly fmt -- --config group_imports=one --check

.PHONY: doc
doc:
	RUSTDOCFLAGS='--cfg docsrs' cargo +nightly d --all-features --open

.PHONY: faktory
faktory:
	docker run --rm -d \
	-v faktory-data:/var/lib/faktory \
	-p ${FAKTORY_HOST}:${FAKTORY_PORT}:7419 \
	-p ${FAKTORY_HOST}:${FAKTORY_PORT_UI}:7420 \
	--name faktory \
	contribsys/faktory:latest \
	/faktory -b :7419 -w :7420

.PHONY: faktory/kill
faktory/kill:
	docker stop faktory

README.md: README.tpl src/lib.rs
	cargo readme > README.md

.PHONY: sort
sort:
	cargo +nightly fmt -- --config group_imports=one

.PHONY: test
test:
	cargo t --locked --all-features --all-targets

.PHONY: test/doc
test/doc:
	cargo test --locked --all-features --doc

.PHONY: test/e2e
test/e2e:
	FAKTORY_URL=tcp://${FAKTORY_HOST}:${FAKTORY_PORT} cargo test --locked --all-features --all-targets

.PHONY: test/load
test/load:
	cargo run --release --features binaries

.PHONY: test/perf
test/perf:
	CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph -o perf.flamegraph.svg -f binaries -b loadtest

.PHONY: test/perf/clean
test/perf/clean:
	rm perf.*
