#  Copyright 2025 Collate
#  Licensed under the Collate Community License, Version 1.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  https://github.com/open-metadata/OpenMetadata/blob/main/ingestion/LICENSE
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
#  pylint: disable=protected-access

"""
Greenplum SQLAlchemy util methods
"""
import re
from collections import defaultdict
from typing import Dict, Tuple

from sqlalchemy import sql, util
from sqlalchemy.dialects.postgresql.base import ENUM
from sqlalchemy.engine import reflection
from sqlalchemy.sql import sqltypes

from metadata.ingestion.source.database.greenplum.queries import (
    GREENPLUM_COL_IDENTITY,
    GREENPLUM_TABLE_COMMENTS,
    GREENPLUM_VIEW_DEFINITIONS,
    GREENPLUM_SCHEMA_COLUMNS,
)
from metadata.utils.sqlalchemy_utils import (
    get_table_comment_wrapper,
    get_view_definition_wrapper,
)


@reflection.cache
def get_table_comment(
    self, connection, table_name, schema=None, **kw
):  # pylint: disable=unused-argument
    return get_table_comment_wrapper(
        self,
        connection,
        table_name=table_name,
        schema=schema,
        query=GREENPLUM_TABLE_COMMENTS,
    )


@reflection.cache
def get_columns(  # pylint: disable=too-many-locals
    self, connection, table_name, schema=None, **kw
):
    """
    Overriding the dialect method to add raw_data_type in response
    """
    if schema is None:
        return []

    if (
        not hasattr(self, "_all_schema_columns")
        or self._all_schema_columns_db != connection.engine.url.database
    ):
        self._all_schema_columns = {}
        self._all_schema_columns_db = connection.engine.url.database

    if schema not in self._all_schema_columns:
        self._all_schema_columns[schema] = self._get_schema_columns(
            connection, schema
        )

    return self._all_schema_columns.get(schema, {}).get((table_name, schema), [])


def _get_schema_columns(self, connection, schema):  # pylint: disable=too-many-locals
    generated = (
        "a.attgenerated as generated"
        if self.server_version_info >= (12,)
        else "NULL as generated"
    )
    if self.server_version_info >= (10,):
        # a.attidentity != '' is required or it will reflect also
        # serial columns as identity.
        identity = GREENPLUM_COL_IDENTITY
    else:
        identity = "NULL as identity_options"

    sql_col_query = GREENPLUM_SCHEMA_COLUMNS.format(
        generated=generated,
        identity=identity,
    )
    sql_col_query = (
        sql.text(sql_col_query)
        .bindparams(sql.bindparam("schema", type_=sqltypes.Unicode))
        .columns(attname=sqltypes.Unicode, default=sqltypes.Unicode)
    )
    conn = connection.execute(sql_col_query, {"schema": schema})
    rows = conn.fetchall()

    # dictionary with (name, ) if default search path or (schema, name)
    # as keys
    domains = self._load_domains(connection)

    # dictionary with (name, ) if default search path or (schema, name)
    # as keys
    enums = dict(
        ((rec["name"],), rec) if rec["visible"] else ((rec["schema"], rec["name"]), rec)
        for rec in self._load_enums(connection, schema="*")
    )

    all_columns = defaultdict(list)
    for row in rows:
        column_info = self._get_column_info(
            row.attname,
            row.format_type,
            row.default,
            row.attnotnull,
            domains,
            enums,
            row.schema_name,
            row.comment,
            row.generated,
            row.identity_options,
        )
        column_info["system_data_type"] = row.format_type
        column_info["ordinal_position"] = row.attnum
        all_columns[(row.table_name, row.schema_name)].append(column_info)
    return dict(all_columns)


def _get_numeric_args(charlen):
    if charlen:
        prec, scale = charlen.split(",")
        return (int(prec), int(scale))
    return ()


def _get_interval_args(charlen, attype, kwargs: Dict):
    field_match = re.match(r"interval (.+)", attype, re.I)
    if charlen:
        kwargs["precision"] = int(charlen)
    if field_match:
        kwargs["fields"] = field_match.group(1)
    attype = "interval"
    return (), attype, kwargs


def _get_bit_var_args(charlen, kwargs):
    kwargs["varying"] = True
    if charlen:
        return (int(charlen),), kwargs

    return (), kwargs


def get_column_args(
    charlen: str, args: Tuple, kwargs: Dict, attype: str
) -> Tuple[Tuple, Dict]:
    """
    Method to determine the args and kwargs
    """
    if attype == "numeric":
        args = _get_numeric_args(charlen)
    elif attype == "double precision":
        args = (53,)
    elif attype == "integer":
        args = ()
    elif attype in ("timestamp with time zone", "time with time zone"):
        kwargs["timezone"] = True
        if charlen:
            kwargs["precision"] = int(charlen)
        args = ()
    elif attype in (
        "timestamp without time zone",
        "time without time zone",
        "time",
    ):
        kwargs["timezone"] = False
        if charlen:
            kwargs["precision"] = int(charlen)
        args = ()
    elif attype == "bit varying":
        args, kwargs = _get_bit_var_args(charlen, kwargs)
    elif attype == "geometry":
        args = ()
    elif attype.startswith("interval"):
        args, attype, kwargs = _get_interval_args(charlen, attype, kwargs)
    elif charlen:
        args = (int(charlen),)

    return args, kwargs, attype


def get_column_default(coltype, schema, default, generated):
    """
    Method to determine the default of column
    """
    autoincrement = False
    # If a zero byte or blank string depending on driver (is also absent
    # for older PG versions), then not a generated column. Otherwise, s =
    # stored. (Other values might be added in the future.)
    if generated not in (None, "", b"\x00"):
        computed = {"sqltext": default, "persisted": generated in ("s", b"s")}
        default = None
    else:
        computed = None
    if default is not None:
        match = re.search(r"""(nextval\(')([^']+)('.*$)""", default)
        if match is not None:
            if issubclass(coltype._type_affinity, sqltypes.Integer):
                autoincrement = True
            # the default is related to a Sequence
            sch = schema
            if "." not in match.group(2) and sch is not None:
                # unconditionally quote the schema name.  this could
                # later be enhanced to obey quoting rules /
                # "quote schema"
                default = (
                    match.group(1)
                    + (f'"{sch}"')
                    + "."
                    + match.group(2)
                    + match.group(3)
                )
    return default, autoincrement, computed


def _handle_array_type(attype):
    return (
        # strip '[]' from integer[], etc.
        re.sub(r"\[\]$", "", attype),
        attype.endswith("[]"),
    )


# pylint: disable=too-many-statements,too-many-branches,too-many-locals,too-many-arguments
def get_column_info(
    self,
    name,
    format_type,
    default,
    notnull,
    domains,
    enums,
    schema,
    comment,
    generated,
    identity,
):
    """
    Method to return column info
    """

    if format_type is None:
        no_format_type = True
        attype = format_type = "no format_type()"
        is_array = False
    else:
        no_format_type = False

        # strip (*) from character varying(5), timestamp(5)
        # with time zone, geometry(POLYGON), etc.
        attype = re.sub(r"\(.*\)", "", format_type)

        # strip '[]' from integer[], etc. and check if an array
        attype, is_array = _handle_array_type(attype)

    # strip quotes from case sensitive enum or domain names
    enum_or_domain_key = tuple(util.quoted_token_parser(attype))

    nullable = not notnull

    charlen = re.search(r"\(([\d,]+)\)", format_type)
    if charlen:
        charlen = charlen.group(1)
    args = re.search(r"\((.*)\)", format_type)
    if args and args.group(1):
        args = tuple(re.split(r"\s*,\s*", args.group(1)))
    else:
        args = ()
    kwargs = {}

    args, kwargs, attype = get_column_args(charlen, args, kwargs, attype)

    while True:
        # looping here to suit nested domains
        if attype in self.ischema_names:
            coltype = self.ischema_names[attype]
            break
        if enum_or_domain_key in enums:
            enum = enums[enum_or_domain_key]
            coltype = ENUM
            kwargs["name"] = enum["name"]
            if not enum["visible"]:
                kwargs["schema"] = enum["schema"]
            args = tuple(enum["labels"])
            break
        if enum_or_domain_key in domains:
            domain = domains[enum_or_domain_key]
            attype = domain["attype"]
            attype, is_array = _handle_array_type(attype)
            # strip quotes from case sensitive enum or domain names
            enum_or_domain_key = tuple(util.quoted_token_parser(attype))
            # A table can't override a not null on the domain,
            # but can override nullable
            nullable = nullable and domain["nullable"]
            if domain["default"] and not default:
                # It can, however, override the default
                # value, but can't set it to null.
                default = domain["default"]
            continue
        coltype = None
        break

    if coltype:
        coltype = coltype(*args, **kwargs)
        if is_array:
            coltype = self.ischema_names["_array"](coltype)
    elif no_format_type:
        util.warn(f"PostgreSQL format_type() returned NULL for column '{name}'")
        coltype = sqltypes.NULLTYPE
    else:
        util.warn(f"Did not recognize type '{attype}' of column '{name}'")
        coltype = sqltypes.NULLTYPE

    default, autoincrement, computed = get_column_default(
        coltype=coltype, schema=schema, default=default, generated=generated
    )
    column_info = {
        "name": name,
        "type": coltype,
        "nullable": nullable,
        "default": default,
        "autoincrement": autoincrement or identity is not None,
        "comment": comment,
    }
    if computed is not None:
        column_info["computed"] = computed
    if identity is not None:
        column_info["identity"] = identity
    return column_info


@reflection.cache
def get_view_definition(
    self, connection, table_name, schema=None, **kw
):  # pylint: disable=unused-argument
    return get_view_definition_wrapper(
        self,
        connection,
        table_name=table_name,
        schema=schema,
        query=GREENPLUM_VIEW_DEFINITIONS,
    )
