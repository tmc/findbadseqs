"""
Script to generate psql input needed to fix sequence issues for a django db
"""
import os
import re
import logging
from django.conf import settings
from django.db import connection, transaction
from django.db.models.loading import get_models
from django.db.models.fields import AutoField
from django.db.utils import DatabaseError


class Sequence(object):
    def __init__(self, name, cursor):
        self.name = name
        self.cursor = cursor

    def __eq__(self, other):
        return self.name == other.name

    def __str__(self):
        return '<Sequence: %s>' % self.name

    @property
    def exists(self):
        query = "SELECT relname from pg_class where relkind='S' AND relname = %s"
        params = (self.name,)
        cursor.execute(query, params)
        return bool(self.cursor.fetchone())

    def get_similar_sequence_names(self):
        seq_parts = self.name.split('_')
        similar_seqs = '%_' + '_'.join(seq_parts[-3:])
        query = "SELECT relname from pg_class where relkind='S' AND relname LIKE %s"
        params = (similar_seqs,)
        self.cursor.execute(query, params)
        return [row[0] for row in self.cursor.fetchall()]


class ColumnInformation(object):

    def __init__(self, cursor, table_name, column_name):
        self.cursor, self.table, self.column = cursor, table_name, column_name

    def sequence_expected_by_django(self):
        name = '%s_%s_seq' % (self.table, self.column)
        return Sequence(name, self.cursor)

    def sequence_according_to_postgres(self):
        self.cursor.execute("SELECT pg_get_serial_sequence(%s, %s)", (self.table, self.column))
        name = self.cursor.fetchone()[0]
        if name:
            return Sequence(name, self.cursor)

    def sequence_currently_used(self):
        query = "select distinct(adrelid), attname, adsrc, relname from pg_catalog.pg_attrdef, pg_catalog.pg_class, pg_attribute WHERE pg_class.oid=adrelid AND pg_attribute.attnum = adnum AND relname=%s AND attname=%s AND adsrc ILIKE '%%nextval%%'"
        #query = "select distinct(adrelid), attname, adsrc, relname from pg_catalog.pg_attrdef, pg_catalog.pg_class, pg_attribute WHERE pg_class.oid=adrelid AND pg_attribute.attnum = adnum AND relname=%s AND attname=%s"
        params = (self.table, self.column)

        #debug
        #query = "select distinct(adrelid), attname, adsrc, relname from pg_catalog.pg_attrdef, pg_catalog.pg_class, pg_attribute WHERE pg_class.oid=adrelid AND pg_attribute.attnum = adnum AND attname=%s AND adsrc ILIKE '%%nextval%%'"
        #params = (self.column,)
        self.cursor.execute(query, params)
        rows = self.cursor.fetchall()
        #print '\n'.join('%s.%s\t%s' % (row[3], row[1], row[2]) for row in rows)
        if len(rows) != 1:
            #logging.error('Unexpected number of results for %s.%s: %s', self.table, self.column, rows)
            return
        row = rows[0]
        adsrc = row[2]
        seq_name_match = re.match("nextval\(+'(?:public.)?([^']+)'", adsrc)

        if not seq_name_match:
            raise ValueError('nextval regex failed. %s.%s adsrc: %s' % (self.table, self.column, adsrc))
        return Sequence(seq_name_match.group(1), self.cursor)


    def get_sequence_permission_fixing_sql(self, sequence):
        return "ALTER SEQUENCE %s OWNED BY %s.%s;" % (sequence.name, self.table, self.column)

    def suggest_sequence_repair_sql(self):
        result = []
        dj_seq = self.sequence_expected_by_django()
        cur_seq = self.sequence_currently_used()

        if not dj_seq or not cur_seq:
            logging.warn('There was a problem getting sequence information for %s.%s - table or column missing?', self.table, self.column)
            return result

        # First we check if the current sequence is the sequence expected by Django
        # if so we can move own to verify sequence ownership.

        if not dj_seq.exists and not cur_seq.exists:
            logging.error('Neither the current sequence %s nor the expected sequence %s exists!', cur_seq, dj_seq)
            return result

        if dj_seq == cur_seq:
            logging.debug('Current sequence %s is what is expected by Django.', cur_seq)
            if not cur_seq.exists:
                logging.error('Current sequence %s DOES NOT EXIST', cur_seq)
        else:
            logging.warn('Current sequence %s for %s.%s is what NOT is expected by Django which is %s', cur_seq, self.table, self.column, dj_seq)
            if not dj_seq.exists:
                logging.error('Expected sequence %s does not exist, suggesting renaming current to expected', dj_seq)
                result.append('ALTER SEQUENCE %s RENAME TO %s;' % (cur_seq.name, dj_seq.name))
                result.append("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT nextval('%s');" % (self.table, self.column, dj_seq.name))
            else:
                logging.error('Expected sequence %s exists, but is not the same as the the current sequence %s, suggesting changing to expected and updating sequence value', dj_seq, cur_seq)
                result.append("ALTER TABLE %s ALTER COLUMN %s SET DEFAULT nextval('%s');" % (self.table, self.column, dj_seq.name))
                result.append("""SELECT setval('%s', coalesce(max("%s"), 1), max("%s") IS NOT null) FROM %s;""" % (dj_seq.name, self.column, self.column, self.table))

        pg_seq = self.sequence_according_to_postgres()
        if not pg_seq:
            logging.warn('Bad sequence ownership: %s.%s, suggesting fixing sql' % (self.table, self.column))
            result.append(self.get_sequence_permission_fixing_sql(self.sequence_currently_used()))

        return result


def check_pk_field(cursor, field):
    output = []
    table_and_column = (model._meta.db_table, field.column)
    transaction.enter_transaction_management()
    try:
        if 'taggit_tag' in table_and_column[0]:
            import ipdb; ipdb.set_trace()

        ci = ColumnInformation(cursor, *table_and_column)
        suggested_repair_sql = ci.suggest_sequence_repair_sql()
        if suggested_repair_sql:
            output = ['BEGIN;'] + suggested_repair_sql + ['COMMIT;']

    except DatabaseError, e:
        print os.getenv('DJANGO_SETTINGS_MODULE'), '\tdb error:\t', str(e).strip()
        pass

    transaction.rollback()

    return output

if __name__ == '__main__':
    cursor = connection.cursor()
    #shortname = os.getenv('DJANGO_SETTINGS_MODULE').split('.')[-2]
    shortname = os.getenv('DJANGO_SETTINGS_MODULE')

    output = []
    for model in [model for model in get_models() if model._meta.installed]:
        auto_fields = [f for f in model._meta.fields if type(f) == AutoField and f.primary_key]
        if auto_fields:
            assert len(auto_fields) == 1
            output.extend(check_pk_field(cursor, auto_fields[0]))

    if output:
        print '\c', settings.DATABASES['default']['NAME']
        print '\n'.join(output)
