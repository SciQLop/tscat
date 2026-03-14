"""migrate tags and products to JSON arrays

Revision ID: df61d8f30e7c
Revises: 9a655086fb51
Create Date: 2026-03-14 21:25:01.498322

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import json

# revision identifiers, used by Alembic.
revision: str = 'df61d8f30e7c'
down_revision: Union[str, None] = '9a655086fb51'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _csv_to_json(csv_value):
    """Convert ScalarListType comma-separated string to JSON array.

    ScalarListType stores lists as comma-separated strings:
      - empty list -> '' (empty string)
      - ['a', 'b'] -> 'a,b'
      - ['a', '', 'b'] -> 'a,,b'  (empty string elements produce double commas)
      - None -> NULL

    Preserves empty string elements -- they are valid tag values in existing DBs.
    Skips conversion for values that are already valid JSON arrays (idempotency).
    """
    if csv_value is None or csv_value == '':
        return '[]'
    if csv_value.startswith('['):
        return csv_value
    return json.dumps(csv_value.split(','))


def upgrade() -> None:
    conn = op.get_bind()

    for table, columns in [('events', ['tags', 'products']), ('catalogues', ['tags'])]:
        for col in columns:
            rows = conn.execute(sa.text(f"SELECT id, {col} FROM {table}")).fetchall()
            for row_id, csv_val in rows:
                conn.execute(
                    sa.text(f"UPDATE {table} SET {col} = :val WHERE id = :id"),
                    {"val": _csv_to_json(csv_val), "id": row_id}
                )


def downgrade() -> None:
    raise NotImplementedError("Downgrade is not supported.")
