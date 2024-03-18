"""init

Revision ID: 526fb088c3f0
Revises:
Create Date: 2024-03-19 17:29:54.371885

"""
from typing import Sequence, Union

import sqlalchemy as sa
import sqlalchemy_utils
from alembic import op
from sqlalchemy.engine.reflection import Inspector

# revision identifiers, used by Alembic.
revision: str = '0c057a8951ba'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # special case for initial migration script: as we start using migration while production is already active
    # we need to check if the tables already exist but only in this initial migration script

    conn = op.get_bind()
    inspector = Inspector.from_engine(conn)
    tables = inspector.get_table_names()
    if 'catalogues' in tables:
        return

    op.create_table('catalogues',
                    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
                    sa.Column('uuid', sa.String(length=36), nullable=False),
                    sa.Column('name', sa.UnicodeText(), nullable=False),
                    sa.Column('author', sa.UnicodeText(), nullable=False),
                    sa.Column('predicate', sa.LargeBinary(), nullable=True),
                    sa.Column('tags', sqlalchemy_utils.types.scalar_list.ScalarListType(), nullable=True),
                    sa.Column('removed', sa.Boolean(), nullable=False),
                    sa.Column('attributes', sa.JSON(), nullable=True),
                    sa.PrimaryKeyConstraint('id')
                    )
    op.create_index(op.f('ix_catalogues_uuid'), 'catalogues', ['uuid'], unique=True)
    op.create_table('events',
                    sa.Column('id', sa.Integer(), autoincrement=True, nullable=False),
                    sa.Column('uuid', sa.String(length=36), nullable=False),
                    sa.Column('start', sa.DateTime(), nullable=False),
                    sa.Column('stop', sa.DateTime(), nullable=False),
                    sa.Column('author', sa.UnicodeText(), nullable=False),
                    sa.Column('tags', sqlalchemy_utils.types.scalar_list.ScalarListType(), nullable=True),
                    sa.Column('products', sqlalchemy_utils.types.scalar_list.ScalarListType(), nullable=True),
                    sa.Column('removed', sa.Boolean(), nullable=False),
                    sa.Column('attributes', sa.JSON(), nullable=True),
                    sa.PrimaryKeyConstraint('id')
                    )
    op.create_index(op.f('ix_events_uuid'), 'events', ['uuid'], unique=True)
    op.create_table('event_in_catalogue',
                    sa.Column('event_id', sa.Integer(), nullable=True),
                    sa.Column('catalogue_id', sa.Integer(), nullable=True),
                    sa.ForeignKeyConstraint(['catalogue_id'], ['catalogues.id'], ),
                    sa.ForeignKeyConstraint(['event_id'], ['events.id'], )
                    )
    op.create_index('e_in_c_index', 'event_in_catalogue', ['event_id', 'catalogue_id'], unique=True)


def downgrade() -> None:  # pragma: no cover
    raise NotImplementedError("Downgrade is not implemented")
