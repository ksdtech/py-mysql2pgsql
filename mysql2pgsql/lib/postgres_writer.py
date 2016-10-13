import re
import sys
from cStringIO import StringIO
from datetime import datetime, date, timedelta

from psycopg2.extensions import QuotedString, Binary, AsIs
from pytz import timezone

def pg_timeformat(dt, with_tz=True, time_only=False):
    fmt = '%H:%M:%S' if time_only else '%Y-%m-%d %H:%M:%S'
    s = dt.strftime(fmt)
    millis = '.' + dt.strftime('%f')[0:3]
    utcoffset = dt.strftime('%z')
    if utcoffset == '' and with_tz:
        utcoffset = '+00'
    elif len(utcoffset) == 5:
        if utcoffset[3:5] == '00':
            utcoffset = utcoffset[0:3]
        else:
            utcoffset = utcoffset[0:3] + ':' + utcoffset[3:5]
    if millis != '.000':
        s += millis
    if with_tz:
        s += utcoffset
    return s

UNIX_EPOCH     = '1970-01-01 00:00:00'
UNIX_EPOCH_UTC = '1970-01-01 00:00:00+00'


class PostgresWriter(object):
    """Base class for :py:class:`mysql2pgsql.lib.postgres_file_writer.PostgresFileWriter`
    and :py:class:`mysql2pgsql.lib.postgres_db_writer.PostgresDbWriter`.
    """
    def __init__(self, verbose=False, options={}):

        # TODO: options for table and column cases: lower', 'preserve'
        # Database, table, field and columns names in PostgreSQL are
        # case-independent, unless you created them with double-quotes around
        # their name, in which case they are case-sensitive.
        # In MySQL, table names can be case-sensitive or not

        # TODO: options for sequence naming: 'table', 'table_and_column'
        # 'table' will only work if tables have zero or one sequenced fields

        self.column_types = {}
        self.name_conversion = options.get('name_conversion', 'lower')
        self.sequence_naming = options.get('sequence_naming', 'table')
        self.index_prefix = options.get('index_prefix', '')
        self.src_tz = options.get('source_timezone', '')
        if self.src_tz != '':
            self.src_tz = timezone(self.src_tz)
        else:
            self.src_tz = None
        self.dst_tz = options.get('timezone', False)
        self.utc = timezone('UTC')
        self.verbose = verbose

    def sequence_name(self, table_name, column_name):
        if self.sequence_naming == 'table':
            return "%s_seq" % table_name.encode('utf8').lower()
        return "%s_%s_seq" % (table_name.encode('utf8').lower(), column_name.encode('utf8').lower())

    def pgsql_case(self, name, quoted=False):
        s = name.encode('utf8')
        if self.name_conversion == 'preserve':
            s = '"%s"' % s
        else:
            s = s.lower()
        if quoted:
            return QuotedString(s).getquoted()
        return s

    def column_description(self, column):
        return '%s %s' % (self.pgsql_case(column['name']), self.column_type_info(column))

    def column_type(self, column):
        hash_key = hash(frozenset(column.items()))
        self.column_types[hash_key] = self.column_type_info(column).split(" ")[0]
        return self.column_types[hash_key]

    def get_type(self, column):
        """This in conjunction with :py:class:`mysql2pgsql.lib.mysql_reader.MysqlReader._convert_type`
        determines the PostgreSQL data type. In my opinion this is way too fugly, will need
        to refactor one day.
        """
        t = lambda v: not v == None
        default = (' DEFAULT %s' % QuotedString(column['default']).getquoted()) if t(column['default']) else None

        if column['type'] == 'char':
            default = ('%s::char' % default) if t(default) else None
            return default, 'character(%s)' % column['length']
        elif column['type'] == 'varchar':
            default = ('%s::character varying' % default) if t(default) else None
            return default, 'character varying(%s)' % column['length']
        elif column['type'] == 'integer':
            default = (" DEFAULT %s" % (column['default'] if t(column['default']) else 'NULL')) if t(default) else None
            return default, 'integer'
        elif column['type'] == 'bigint':
            default = (" DEFAULT %s" % (column['default'] if t(column['default']) else 'NULL')) if t(default) else None
            return default, 'bigint'
        elif column['type'] == 'tinyint':
            default = (" DEFAULT %s" % (column['default'] if t(column['default']) else 'NULL')) if t(default) else None
            return default, 'smallint'
        elif column['type'] == 'boolean':
            default = (" DEFAULT %s" % ('true' if int(column['default']) == 1 else 'false')) if t(default) else None
            return default, 'boolean'
        elif column['type'] == 'float':
            default = (" DEFAULT %s" % (column['default'] if t(column['default']) else 'NULL')) if t(default) else None
            return default, 'real'
        elif column['type'] == 'float unsigned':
            default = (" DEFAULT %s" % (column['default'] if t(column['default']) else 'NULL')) if t(default) else None
            return default, 'real'
        elif column['type'] in ('numeric', 'decimal'):
            default = (" DEFAULT %s" % (column['default'] if t(column['default']) else 'NULL')) if t(default) else None
            return default, 'numeric(%s, %s)' % (column['length'] or 20, column['decimals'] or 0)
        elif column['type'] == 'double precision':
            default = (" DEFAULT %s" % (column['default'] if t(column['default']) else 'NULL')) if t(default) else None
            return default, 'double precision'
        elif column['type'] == 'datetime':
            default = None
            if self.dst_tz:
                return default, 'timestamp with time zone'
            else:
                return default, 'timestamp without time zone'
        elif column['type'] == 'date':
            default = None
            return default, 'date'
        elif column['type'] == 'timestamp':
            if column['default'] == None:
                default = None
            elif "CURRENT_TIMESTAMP" in column['default']:
                default = ' DEFAULT CURRENT_TIMESTAMP'
            elif "0000-00-00 00:00" in column['default']:
                if self.dst_tz:
                    default = " DEFAULT '%s'" % UNIX_EPOCH_UTC
                else:
                    default = " DEFAULT '%s'" % UNIX_EPOCH
            if self.dst_tz:
                return default, 'timestamp with time zone'
            else:
                return default, 'timestamp without time zone'
        elif column['type'] == 'time':
            default = " DEFAULT NOW()" if t(default) else None
            if self.dst_tz:
                return default, 'time with time zone'
            else:
                return default, 'time without time zone'
        elif column['type'] in ('blob', 'binary', 'longblob', 'mediumblob', 'tinyblob', 'varbinary'):
            return default, 'bytea'
        elif column['type'] in ('tinytext', 'mediumtext', 'longtext', 'text'):
            return default, 'text'
        elif column['type'].startswith('enum'):
            default = (' %s::character varying' % default) if t(default) else None
            enum = re.sub(r'^enum\(|\)$', '', column['type'])
            # TODO: will work for "'.',',',''''" but will fail for "'.'',','.'"
            max_enum_size = max([len(e.replace("''", "'")) for e in enum.split("','")])
            return default, ' character varying(%s) check(%s in (%s))' % (
                max_enum_size, self.pgsql_case(column['name'], True), enum)
        elif column['type'].startswith('bit('):
            return ' DEFAULT %s' % column['default'].upper() if column['default'] else column['default'], 'varbit(%s)' % re.search(r'\((\d+)\)', column['type']).group(1)
        elif column['type'].startswith('set('):
            if default:
                default = ' DEFAULT ARRAY[%s]::text[]' % ','.join(QuotedString(v).getquoted() for v in re.search(r"'(.*)'", default).group(1).split(','))
            return default, 'text[]'
        else:
            raise Exception('unknown %s' % column['type'])

    def column_type_info(self, column):
        """
        """
        null = "" if column['null'] else " NOT NULL"

        default, column_type = self.get_type(column)

        if column.get('auto_increment', None):
            return '%s DEFAULT nextval(\'%s\'::regclass) NOT NULL' % (
                   column_type, self.sequence_name(column['table_name'], column['name']))

        return '%s%s%s' % (column_type, (default if not default == None else ''), null)

    def table_comments(self, table):
        comments = StringIO()
        if table.comment:
          comments.write(self.table_comment(table.name, table.comment))
        for column in table.columns:
          comments.write(self.column_comment(table.name, column))
        return comments.getvalue()

    def column_comment(self, tablename, column):
      if column['comment']:
        return (' COMMENT ON COLUMN %s.%s is %s;' % (
            self.pgsql_case(tablename), self.pgsql_case(column['name']), QuotedString(column['comment']).getquoted()))
      else:
        return ''

    def table_comment(self, tablename, comment):
        return (' COMMENT ON TABLE %s is %s;' % ( tablename, QuotedString(comment).getquoted()))

    def process_row(self, table, row):
        """Examines row data from MySQL and alters
        the values when necessary to be compatible with
        sending to PostgreSQL via the copy command
        """
        for index, column in enumerate(table.columns):
            hash_key = hash(frozenset(column.items()))
            column_type = self.column_types[hash_key] if hash_key in self.column_types else self.column_type(column)
            if row[index] == None and ('timestamp' not in column_type or not column['default']):
                row[index] = '\N'
            elif row[index] == None and column['default']:
                if self.dst_tz:
                    row[index] = UNIX_EPOCH_UTC
                else:
                    row[index] = UNIX_EPOCH
            elif 'bit' in column_type:
                row[index] = bin(ord(row[index]))[2:]
            elif isinstance(row[index], (str, unicode, basestring)):
                if column_type == 'bytea':
                    row[index] = Binary(row[index]).getquoted()[1:-8] if row[index] else row[index]
                elif 'text[' in column_type:
                    row[index] = '{%s}' % ','.join('"%s"' % v.replace('"', r'\"') for v in row[index].split(','))
                else:
                    row[index] = row[index].replace('\\', r'\\').replace('\n', r'\n').replace('\t', r'\t').replace('\r', r'\r').replace('\0', '')
            elif column_type == 'boolean':
                # We got here because you used a tinyint(1), if you didn't want a bool, don't use that type
                row[index] = 't' if row[index] not in (None, 0) else 'f' if row[index] == 0 else row[index]
            elif isinstance(row[index], datetime):
                dt = row[index]
                if self.src_tz and not dt.tzinfo:
                    dt = self.src_tz.localize(dt)
                dt = dt.astimezone(self.utc)
                row[index] = pg_timeformat(dt, self.dst_tz, False)
            elif isinstance(row[index], date):
                row[index] = pg_timeformat(row[index], self.dst_tz, False)
            elif isinstance(row[index], timedelta):
                # MySQL time values range from      '-838:59:59' to '838:59:59'
                # PossgreSQL time values range from   '00:00:00' to  '24:00:00'
                # Do a "modulo" 2000-01-01 00:00:00 calculation
                dt = datetime(2000, 1, 1) + row[index]
                if self.src_tz:
                    dt = self.src_tz.localize(dt)
                    dt = dt.astimezone(self.utc)
                else:
                    dt = self.utc.localize(dt)
                row[index] = pg_timeformat(dt, self.dst_tz, True)
            else:
                row[index] = AsIs(row[index]).getquoted()

    def table_attributes(self, table):
        primary_keys = []
        serial_key = None
        maxval = None
        columns = StringIO()

        for column in table.columns:
            if column['auto_increment']:
                serial_key = column['name']
                maxval = 1 if column['maxval'] < 1 else column['maxval'] + 1
            if column['primary_key']:
                primary_keys.append(column['name'])
            columns.write('  %s,\n' % self.column_description(column))
        return primary_keys, serial_key, maxval, columns.getvalue()[:-2]

    def truncate(self, table):
        serial_key = None
        maxval = None

        for column in table.columns:
            if column['auto_increment']:
                serial_key = column['name']
                maxval = 1 if column['maxval'] < 1 else column['maxval'] + 1

        truncate_sql = 'TRUNCATE %s CASCADE;' % self.pgsql_case(table.name, True)
        serial_key_sql = None

        if serial_key:
            serial_key_sql = "SELECT pg_catalog.setval(pg_get_serial_sequence(%(table_name)s, %(serial_key)s), %(maxval)s, true);" % {
                'table_name': self.pgsql_case(table.name, True),
                'serial_key': self.pgsql_case(serial_key, True),
                'maxval': maxval }

        return (truncate_sql, serial_key_sql)

    def write_table(self, table):
        primary_keys, serial_key, maxval, columns = self.table_attributes(table)
        serial_key_sql = []
        table_sql = []
        if serial_key:
            serial_key_seq = self.sequence_name(table.name, serial_key)
            serial_key_sql.append("DROP SEQUENCE IF EXISTS %s CASCADE;" % serial_key_seq)
            serial_key_sql.append("""CREATE SEQUENCE %s INCREMENT BY 1
NO MAXVALUE NO MINVALUE CACHE 1;""" % serial_key_seq)
            serial_key_sql.append("SELECT pg_catalog.setval('%s', %s, true);" % (serial_key_seq, maxval))

        table_sql.append('DROP TABLE IF EXISTS %s CASCADE;' % self.pgsql_case(table.name, False))
        table_sql.append('CREATE TABLE %s (\n%s\n)\nWITHOUT OIDS;' % (
            self.pgsql_case(table.name, False), columns))
        table_sql.append( self.table_comments(table))
        return (table_sql, serial_key_sql)

    def write_indexes(self, table):
        index_sql = []
        primary_index = [idx for idx in table.indexes if idx.get('primary', None)]
        index_prefix = self.index_prefix
        if primary_index:
            index_name = self.pgsql_case('%s%s_%s' % (index_prefix, table.name,
                                '_'.join(primary_index[0]['columns'])), True)
            index_sql.append('ALTER TABLE %(table_name)s ADD CONSTRAINT %(index_name)s_pkey PRIMARY KEY(%(column_names)s);' % {
                    'table_name': self.pgsql_case(table.name, True),
                    'index_name': index_name,
                    'column_names': ', '.join(self.pgsql_case(col, True) for col in primary_index[0]['columns']),
                    })
        for index in table.indexes:
            if 'primary' in index:
                continue
            unique = 'UNIQUE ' if index.get('unique', None) else ''
            index_name = self.pgsql_case('%s%s_%s' % (index_prefix, table.name, '_'.join(index['columns'])), True)
            index_sql.append('DROP INDEX IF EXISTS %s CASCADE;' % index_name)
            index_sql.append('CREATE %(unique)sINDEX %(index_name)s ON %(table_name)s (%(column_names)s);' % {
                    'unique': unique,
                    'index_name': index_name,
                    'table_name': self.pgsql_case(table.name),
                    'column_names': ', '.join(self.pgsql_case(col, True) for col in index['columns']),
                    })

        return index_sql

    def write_constraints(self, table):
        constraint_sql = []
        for key in table.foreign_keys:
            constraint_sql.append("""ALTER TABLE %(table_name)s ADD FOREIGN KEY (%(column_name)s)
            REFERENCES %(ref_table_name)s (%(ref_column_name)s);""" % {
                'table_name': self.pgsql_case(table.name, True),
                'column_name': self.pgsql_case(key['column'], True),
                'ref_table_name': self.pgsql_case(key['ref_table'], True),
                'ref_column_name': self.pgsql_case(key['ref_column'], True) })
        return constraint_sql

    def write_triggers(self, table):
        trigger_sql = []
        for key in table.triggers:
            trigger_sql.append("""CREATE OR REPLACE FUNCTION %(fn_trigger_name)s RETURNS TRIGGER AS $%(trigger_name)s$
            BEGIN
                %(trigger_statement)s
            RETURN NULL;
            END;
            $%(trigger_name)s$ LANGUAGE plpgsql;""" % {
                'trigger_time': key['timing'],
                'trigger_event': key['event'],
                'trigger_name': key['name'],
                'fn_trigger_name': 'fn_' + key['name'] + '()',
                'trigger_statement': key['statement']})

            trigger_sql.append("""CREATE TRIGGER %(trigger_name)s %(trigger_time)s %(trigger_event)s ON %(table_name)s
            FOR EACH ROW
            EXECUTE PROCEDURE fn_%(trigger_name)s();""" % {
                'table_name': self.pgsql_case(table.name, True),
                'trigger_time': key['timing'],
                'trigger_event': key['event'],
                'trigger_name': key['name']})

        return trigger_sql

    def close(self):
        raise NotImplementedError

    def write_contents(self, table, reader):
        raise NotImplementedError
