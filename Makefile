FAKTORY_HOST=localhost
FAKTORY_PORT=7419
FAKTORY_PORT_SECURE=17419
FAKTORY_PORT_UI=7420

.PHONY: check
check:
	cargo fmt --check
	cargo clippy
	cargo d --no-deps --all-features

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

.PHONY: faktory/tls
faktory/tls:
	docker compose -f docker/compose.yml up -d

.PHONY: faktory/tls/kill
faktory/tls/kill:
	docker compose -f docker/compose.yml down

.PHONY: test
test:
	cargo t --locked --all-features --all-targets

.PHONY: test/doc
test/doc:
	cargo test --locked --all-features --doc

.PHONY: test/e2e
test/e2e:
	FAKTORY_URL=tcp://${FAKTORY_HOST}:${FAKTORY_PORT} cargo test --locked --all-features --all-targets

.PHONY: test/e2e/tls
test/e2e/tls:
	FAKTORY_URL_SECURE=tcp://${FAKTORY_HOST}:${FAKTORY_PORT_SECURE} \
	cargo test --locked --features tls --test tls

.PHONY: test/load
test/load:
	cargo run --release --features binaries

.PHONY: test/perf
test/perf:
	CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph -o perf.flamegraph.svg -f binaries -b loadtest

.PHONY: test/perf/clean
test/perf/clean:
	rm perf.*
