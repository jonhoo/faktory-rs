FAKTORY_IP=127.0.0.1
FAKTORY_HOST=localhost
FAKTORY_PORT=7419
FAKTORY_PORT_SECURE=17419
FAKTORY_PORT_UI=7420
FAKTORY_PASSWORD=uredinales

# parse Faktory version out of the Dockerfile; we are having that "no-op"
# Dockerfile so that we can receive dependabot notifications and automated RPs
# when new versions of the image are available
FAKTORY_VERSION=$(shell awk '/FROM contribsys\/faktory/ {print $$2}' docker/faktory.Dockerfile | awk -F ':' '{print $$2}' | head -n1)

.PHONY: precommit
precommit: fmt check test/doc test/e2e test/e2e/tls

.PHONY: fmt
fmt:
	cargo fmt

.PHONY: check
check:
	cargo fmt --check
	cargo clippy --all-features
	cargo d --no-deps --all-features

.PHONY: doc
doc:
	RUSTDOCFLAGS='--cfg docsrs' cargo +nightly d --all-features --open

.PHONY: faktory
faktory:
	docker run --rm -d \
	-v faktory-data:/var/lib/faktory \
	-p ${FAKTORY_IP}:${FAKTORY_PORT}:7419 \
	-p ${FAKTORY_IP}:${FAKTORY_PORT_UI}:7420 \
	--name faktory \
	contribsys/faktory:${FAKTORY_VERSION} \
	/faktory -b :7419 -w :7420

.PHONY: faktory/kill
faktory/kill:
	docker stop faktory
	docker volume rm faktory-data

.PHONY: faktory/tls
faktory/tls:
	docker compose -f docker/compose.yml up -d --build

.PHONY: faktory/tls/kill
faktory/tls/kill:
	docker compose -f docker/compose.yml down -v

.PHONY: test
test:
	cargo t --locked --all-features --all-targets -- $(pattern)

.PHONY: test/doc
test/doc:
	cargo test --locked --all-features --doc

.PHONY: test/e2e
test/e2e:
	TESTCONTAINERS_ENABLED=1 \
	FAKTORY_URL=tcp://${FAKTORY_HOST}:${FAKTORY_PORT} \
	cargo test --locked --all-features --all-targets -- \
	--nocapture $(pattern)

.PHONY: test/e2e/ignored
test/e2e/ignored:
	FAKTORY_URL=tcp://${FAKTORY_HOST}:${FAKTORY_PORT} \
	cargo test --locked --all-features --all-targets -- \
	--nocapture --include-ignored queue_control_actions_wildcard

.PHONY: test/e2e/tls
test/e2e/tls:
	FAKTORY_URL_SECURE=tcp://:${FAKTORY_PASSWORD}@${FAKTORY_HOST}:${FAKTORY_PORT_SECURE} \
	FAKTORY_URL=tcp://:${FAKTORY_PASSWORD}@${FAKTORY_HOST}:${FAKTORY_PORT} \
	cargo test --locked --features native_tls,rustls --test tls -- \
	--nocapture $(pattern)

.PHONY: test/load
test/load:
	cargo run --release --features binaries $(jobs) $(threads)

.PHONY: test/perf
test/perf:
	CARGO_PROFILE_RELEASE_DEBUG=true cargo flamegraph -o perf.flamegraph.svg -f binaries -b loadtest

.PHONY: test/perf/clean
test/perf/clean:
	rm perf.*
