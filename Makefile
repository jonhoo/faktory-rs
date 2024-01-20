.PHONY: check doc faktory faktory/* test test/*

FAKTORY_HOST=127.0.0.1
FAKTORY_PORT=7419
FAKTORY_PORT_UI=7420

check:
	cargo fmt --check
	cargo clippy
	cargo d --no-deps --all-features

doc:
	RUSTDOCFLAGS='--cfg docsrs' cargo +nightly d --all-features --open

faktory:
	docker run --rm -d \
	-v faktory-data:/var/lib/faktory \
	-p ${FAKTORY_HOST}:${FAKTORY_PORT}:7419 \
	-p ${FAKTORY_HOST}:${FAKTORY_PORT_UI}:7420 \
	--name faktory \
	contribsys/faktory:latest \
	/faktory -b :7419 -w :7420

faktory/kill:
	docker stop faktory

README.md: README.tpl src/lib.rs
	cargo readme > README.md

test:
	cargo t --locked --all-features --all-targets

test/doc:
	cargo test --locked --all-features --doc

test/e2e:
	FAKTORY_URL=tcp://${FAKTORY_HOST}:${FAKTORY_PORT} cargo test --locked --all-features --all-targets

test/load:
	cargo run --release --features binaries

test/perf:
	cargo build --release --features binaries
	perf record -o perf_loadtest.data --call-graph dwarf target/release/loadtest
	perf script -i perf_loadtest.data | inferno-collapse-perf > perf_loadtest_stacks.folded
	cat perf_loadtest_stacks.folded | inferno-flamegraph > perf_loadtest.svg

test/perf/clean:
	rm perf_loadtest*
