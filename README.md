- `pyproject.toml` — dependency declarations and locked versions (via uv/poetry)
- `docker-compose.yml` — defines and starts the local Kafka broker (and optionally a Kafka UI container)
- `.env.example` — documents required environment variables without committing real secrets
- `.gitignore` — excludes .env, virtual envs, caches, etc. from version control
- `README.md` — setup and run instructions
- `src/kafka_pipeline/` — the installable Python package; all reusable library code lives here  
<br>
- `config/settings.py` — single pydantic BaseSettings class reading all env vars (broker address, topic names, consumer group id)  
<br>
- `producer/`
- `base_producer.py` — shared producer plumbing: connection setup, logging, graceful shutdown
- `market_event_producer.py` — specific producer logic: what topic to write to, what the message looks like  
<br>
- `consumer/`
- `base_consumer.py` — shared consumer plumbing: connection, polling loop, offset handling
- `market_event_consumer.py` — specific consumer logic: what to do with each consumed message  
<br>
- `schemas/models.py` — pydantic models defining the message contract/shape
- `serializers/json_serializer.py` — converts pydantic models to/from bytes for the wire  
<br>
- `utils/`
- `logger.py` — consistent structured logging setup
- `retry.py` — retry/backoff decorators for transient broker failures  
<br>
- `tests/`
- `unit/` — tests pure logic in isolation (serializer round-trips, settings parsing), no real broker
- `integration/test_producer_consumer.py` — spins up a real ephemeral Kafka broVker (via testcontainers-python) and verifies end-to-end message flow  
<br>
- `scripts/`
- `create_topics.py` — one-time script to create topics with the right partition count
- `run_local_demo.py` — convenience entrypoint to run producer + consumer together for manual testing  
<br>
- `.github/workflows/ci.yml` — automated linting, unit tests, and (optionally) integration tests on every push/PR