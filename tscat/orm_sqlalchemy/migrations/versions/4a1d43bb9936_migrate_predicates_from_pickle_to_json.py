"""migrate predicates from pickle to JSON

Revision ID: 4a1d43bb9936
Revises: df61d8f30e7c
Create Date: 2026-03-14 21:32:10.884164

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
import pickle
import json

# revision identifiers, used by Alembic.
revision: str = '4a1d43bb9936'
down_revision: Union[str, None] = 'df61d8f30e7c'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _predicate_pickle_to_dict(pickled_bytes):
    """Convert a pickled Predicate to its dict form."""
    pred = pickle.loads(pickled_bytes)
    return pred.to_dict()


def upgrade() -> None:
    conn = op.get_bind()

    rows = conn.execute(sa.text("SELECT id, predicate FROM catalogues WHERE predicate IS NOT NULL")).fetchall()
    for row_id, pred_blob in rows:
        if isinstance(pred_blob, bytes):
            pred_dict = _predicate_pickle_to_dict(pred_blob)
            pred_json = json.dumps(pred_dict)
            conn.execute(
                sa.text("UPDATE catalogues SET predicate = :pred WHERE id = :id"),
                {"pred": pred_json, "id": row_id}
            )


def downgrade() -> None:
    raise NotImplementedError("Downgrade is not supported.")
