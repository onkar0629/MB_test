"""PostgreSQL client for extracting Action OS (Twenty CRM) records.

Supports direct TCP connection via pg8000 with local docker execution fallback.
Extracts records formatted as JSON for raw landing volumes.
"""
from __future__ import annotations

import json
import logging
import shutil
import subprocess
from collections.abc import Iterator

logger = logging.getLogger(__name__)


class PostgresClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "postgres",
        database: str = "default",
        workspace_schema: str | None = None,
        docker_container: str = "twenty-db-1",
    ) -> None:
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self._workspace_schema = workspace_schema
        self.docker_container = docker_container

    def get_workspace_schema(self) -> str:
        """Discover the active Twenty CRM workspace schema name."""
        if self._workspace_schema:
            return self._workspace_schema

        query = (
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name LIKE 'workspace_%' ORDER BY schema_name LIMIT 1;"
        )
        rows = self._execute_query(query)
        if rows and len(rows) > 0 and len(rows[0]) > 0 and rows[0][0]:
            self._workspace_schema = str(rows[0][0])
            return self._workspace_schema
        return "public"

    def _execute_query(self, sql: str) -> list[tuple]:
        """Execute query using pg8000 or fallback to docker exec only if on localhost."""
        try:
            import pg8000.dbapi

            conn = pg8000.dbapi.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database,
                timeout=15,
            )
            try:
                cursor = conn.cursor()
                cursor.execute(sql)
                rows = cursor.fetchall()
                return rows
            finally:
                conn.close()
        except Exception as exc:
            # Only fallback to docker exec on localhost developer environments
            if self.host in ("localhost", "127.0.0.1") and shutil.which("docker"):
                logger.info("Direct TCP failed (%s); trying local docker exec fallback...", exc)
                return self._execute_docker_query(sql)
            raise ConnectionError(
                f"Failed to connect to PostgreSQL at {self.host}:{self.port} "
                f"(user: {self.user}, db: {self.database}): {exc}"
            ) from exc

    def _execute_docker_query(self, sql: str) -> list[tuple]:
        """Execute psql command inside local docker container."""
        cmd = [
            "docker", "exec", self.docker_container,
            "psql", "-U", self.user, "-d", self.database,
            "-t", "-A", "-F", "\t", "-c", sql,
        ]
        res = subprocess.run(cmd, capture_output=True, text=True, check=True)
        lines = [line.strip() for line in res.stdout.strip().split("\n") if line.strip()]
        return [tuple(line.split("\t")) for line in lines]

    def fetch_entity_json(self, table_name: str) -> list[dict]:
        """Extract all table rows formatted as a list of dicts."""
        schema = self.get_workspace_schema()
        json_query = (
            f"SELECT json_agg(row_to_json(t)) FROM "
            f'(SELECT * FROM "{schema}"."{table_name}") t;'
        )

        # Primary: direct database query via pg8000
        try:
            rows = self._execute_query(json_query)
            if rows and len(rows) > 0 and rows[0][0]:
                raw = rows[0][0]
                return json.loads(raw) if isinstance(raw, str) else list(raw)
            return []
        except Exception as exc:
            # Secondary fallback for local docker only
            if self.host in ("localhost", "127.0.0.1") and shutil.which("docker"):
                cmd = [
                    "docker", "exec", self.docker_container,
                    "psql", "-U", self.user, "-d", self.database,
                    "-t", "-A", "-c", json_query,
                ]
                res = subprocess.run(cmd, capture_output=True, text=True)
                out = res.stdout.strip()
                if out and out != "" and out != "null":
                    data = json.loads(out)
                    if isinstance(data, list):
                        return data
            raise exc

    def iter_entity(
        self,
        entity: str,
        page_size: int = 500,
    ) -> Iterator[tuple[int, dict]]:
        """Yield (page_number, payload) for an entity (people, members, tasks)."""
        table_map = {
            "people": "person",
            "workspace_members": "workspaceMember",
            "tasks": "task",
        }
        table_name = table_map.get(entity, entity)
        records = self.fetch_entity_json(table_name)

        if not records:
            yield 1, {"records": [], "entity": entity, "total": 0}
            return

        for page_idx, start_pos in enumerate(range(0, len(records), page_size), start=1):
            chunk = records[start_pos : start_pos + page_size]
            payload = {
                "records": chunk,
                "entity": entity,
                "total": len(records),
            }
            yield page_idx, payload
