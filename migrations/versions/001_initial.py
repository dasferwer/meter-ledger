from pathlib import Path

from alembic import op

revision = "001"
down_revision = None


def upgrade():
    # asyncpg исполняет несколько SQL-команд через сырой драйвер внутри транзакции миграции.
    sql = Path("infra/schema.sql").read_text()
    op.get_bind().connection.run_async(lambda conn: conn.execute(sql))


def downgrade():
    raise RuntimeError("Billing history cannot be removed with an automatic downgrade")
