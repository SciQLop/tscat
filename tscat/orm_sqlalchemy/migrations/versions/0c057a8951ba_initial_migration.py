"""Initial migration

Revision ID: 0c057a8951ba
Revises:
Create Date: 2024-03-18 18:48:34.051832

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = '0c057a8951ba'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    pass


def downgrade() -> None:
    raise NotImplementedError("Downgrade is not supported")
