"""Live trading through Symphony XTS ("Open XTS" brokers).

Two processes share this package:

* the API (`src.main`) uses `service.py` / `broker_store.py` to store broker
  credentials, log in to XTS, save execution settings and create
  deployments -- then forwards each command to the live worker;
* the live worker (`uvicorn src.live.worker:app`) owns the market data feed,
  the broker sockets and one `DeploymentRunner` task per active deployment,
  and streams LTP/MTM to browsers over `/ws/live`.

All state that must survive a restart lives in PostgreSQL
(scripts/sql/live_trade_migration.sql); everything else is in-memory in the
worker.
"""
