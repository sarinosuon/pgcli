#!/usr/bin/env python
from __future__ import unicode_literals
from __future__ import print_function

import os
import sys
import traceback
from io import StringIO
import logging
import atexit
import re

from time import time

import click
import sqlparse
from prompt_toolkit import CommandLineInterface, AbortAction
from prompt_toolkit.shortcuts import create_default_layout, create_eventloop
from prompt_toolkit.document import Document
from prompt_toolkit.filters import Always
from prompt_toolkit.layout.processors import HighlightMatchingBracketProcessor
from prompt_toolkit.layout.prompt import DefaultPrompt
from prompt_toolkit.history import FileHistory
from pygments.lexers.sql import PostgresLexer
from pygments import highlight
#from pygments.lexers import PostgresLexer
from pygments.formatters import Terminal256Formatter

from pygments.token import Token

from .packages.tabulate import tabulate
from .packages.expanded import expanded_table
from .packages.pgspecial import (CASE_SENSITIVE_COMMANDS,
        NON_CASE_SENSITIVE_COMMANDS, is_expanded_output)

from .packages.pgspecial import save_macros
from .packages.pgspecial import load_macros
from .packages.pgspecial import get_lambda
from .packages.pgspecial import simple_invoke
from .packages.pgspecial import check_invoke_macro

import pgcli.packages.pgspecial as pgspecial
import pgcli.packages.iospecial as iospecial
from .pgcompleter import PGCompleter
from .pgtoolbar import create_toolbar_tokens_func
from .pgstyle import style_factory
from .pgexecute import PGExecute
from .pgbuffer import PGBuffer
from .config import write_default_config, load_config
from .key_bindings import pgcli_bindings
from .encodingutils import utf8tounicode
from .__init__ import __version__

import shared

try:
    from urlparse import urlparse
except ImportError:
    from urllib.parse import urlparse
from getpass import getuser
from psycopg2 import OperationalError

from collections import namedtuple

# Query tuples are used for maintaining history
Query = namedtuple('Query', ['query', 'successful', 'mutating'])

class PGCli(object):
    def __init__(self, force_passwd_prompt=False, never_passwd_prompt=False,
                 pgexecute=None):

        self.force_passwd_prompt = force_passwd_prompt
        self.never_passwd_prompt = never_passwd_prompt
        self.pgexecute = pgexecute

        load_macros(None,None,None)

        from pgcli import __file__ as package_root
        package_root = os.path.dirname(package_root)

        default_config = os.path.join(package_root, 'pgclirc')
        write_default_config(default_config, '~/.pgclirc')

        # Load config.
        c = self.config = load_config('~/.pgclirc', default_config)
        self.multi_line = c.getboolean('main', 'multi_line')
        self.vi_mode = c.getboolean('main', 'vi')
        pgspecial.TIMING_ENABLED = c.getboolean('main', 'timing')
        self.table_format = c.get('main', 'table_format')
        self.syntax_style = c.get('main', 'syntax_style')

        self.logger = logging.getLogger(__name__)
        self.initialize_logging()

        self.query_history = []

        # Initialize completer
        smart_completion = c.getboolean('main', 'smart_completion')
        completer = PGCompleter(smart_completion)
        completer.extend_special_commands(CASE_SENSITIVE_COMMANDS.keys())
        completer.extend_special_commands(NON_CASE_SENSITIVE_COMMANDS.keys())
        self.completer = completer

    def initialize_logging(self):

        log_file = self.config.get('main', 'log_file')
        log_level = self.config.get('main', 'log_level')

        level_map = {'CRITICAL': logging.CRITICAL,
                     'ERROR': logging.ERROR,
                     'WARNING': logging.WARNING,
                     'INFO': logging.INFO,
                     'DEBUG': logging.DEBUG
                     }

        handler = logging.FileHandler(os.path.expanduser(log_file))

        formatter = logging.Formatter(
            '%(asctime)s (%(process)d/%(threadName)s) '
            '%(name)s %(levelname)s - %(message)s')

        handler.setFormatter(formatter)

        root_logger = logging.getLogger('pgcli')
        root_logger.addHandler(handler)
        root_logger.setLevel(level_map[log_level.upper()])

        root_logger.debug('Initializing pgcli logging.')
        root_logger.debug('Log file %r.', log_file)

    def connect_uri(self, uri):
        uri = urlparse(uri)
        database = uri.path[1:]  # ignore the leading fwd slash
        self.connect(database, uri.hostname, uri.username,
                     uri.port, uri.password)

    # return None if format was not correct
    def extract_user_input(self, sql):
        p = sql.find('# Type your query below this line')
        if p >= 0:
            p2 = sql.find('# Type your query above this line')
            if p2 >= 0:
                lines = sql[p:p2].split("\n")[1:]  # ignore the first line
                return "\n".join(lines).rstrip()

        return None

    def connect(self, database='', host='', user='', port='', passwd=''):
        # Connect to the database.

        if not database:
            if user:
                database = user
            else:
                # default to current OS username just like psql
                database = user = getuser()

        # Prompt for a password immediately if requested via the -W flag. This
        # avoids wasting time trying to connect to the database and catching a
        # no-password exception.
        # If we successfully parsed a password from a URI, there's no need to
        # prompt for it, even with the -W flag
        if self.force_passwd_prompt and not passwd:
            passwd = click.prompt('Password', hide_input=True,
                                  show_default=False, type=str)

        # Prompt for a password after 1st attempt to connect without a password
        # fails. Don't prompt if the -w flag is supplied
        auto_passwd_prompt = not passwd and not self.never_passwd_prompt

        # Attempt to connect to the database.
        # Note that passwd may be empty on the first attempt. If connection
        # fails because of a missing password, but we're allowed to prompt for
        # a password (no -w flag), prompt for a passwd and try again.
        try:
            try:
                pgexecute = PGExecute(database, user, passwd, host, port)
            except OperationalError as e:
                if ('no password supplied' in utf8tounicode(e.args[0]) and
                        auto_passwd_prompt):
                    passwd = click.prompt('Password', hide_input=True,
                                          show_default=False, type=str)
                    pgexecute = PGExecute(database, user, passwd, host, port)
                else:
                    raise e

        except Exception as e:  # Connecting to a database could fail.
            self.logger.debug('Database connection failed: %r.', e)
            self.logger.error("traceback: %r", traceback.format_exc())
            click.secho(str(e), err=True, fg='red')
            sys.exit(1)

        self.pgexecute = pgexecute

    def handle_editor_command(self, cli, document):
        """
        Editor command is any query that is prefixed or suffixed
        by a '\e'. The reason for a while loop is because a user
        might edit a query multiple times.
        For eg:
        "select * from \e"<enter> to edit it in vim, then come
        back to the prompt with the edited query "select * from
        blah where q = 'abc'\e" to edit it again.
        :param cli: CommandLineInterface
        :param document: Document
        :return: Document
        """
        the_text = document.text
        the_text = "\n".join(cli.current_buffer._history.strings)
        while iospecial.editor_command(document.text):
            filename = iospecial.get_filename(document.text)
            sql, message = iospecial.open_external_editor(filename,
                                                          sql=the_text)


            if message:
                # Something went wrong. Raise an exception and bail.
                raise RuntimeError(message)

            extract = self.extract_user_input(sql)
            if extract is None:
                continue
            else:
                sql = extract

            cli.current_buffer.document = Document(sql, cursor_position=len(sql))
            document = cli.read_input(False)
            continue
        return document

    def display_colorized_sql(self, sql):
        print(highlight(sql, PostgresLexer(), Terminal256Formatter()))

    def get_color_specs_helper(self, main):
        colors = ['red', 'green', 'yellow', 'blue', 'purple', 'cyan', 'white']
        col_pat = re.compile(r'(?P<col_name>[^:/]+):(?P<subspecs>[^/]+)')

        full = []

        col_specs = col_pat.findall(main)
        for col_name, col_spec_main in col_specs:
            col_specs = [each.strip() for each in col_spec_main.split("~")]
            color = 0
            for col_spec in col_specs:
                if col_spec.strip():
                    cases = [each.strip() for each in col_spec.split("|")]
                    for case in cases:
                        full.append((col_name, case, colors[color]))
                color += 1

        return full

    # returns (array of specs (otherwise []),  True/False to add to default colors)
    def get_color_specs(self, text):
        full = []
        add_to_default = False

        ref_pat = re.compile(r"--'\s*(?P<add_maybe>\+)?color\s*=>\s*(?P<rest>.+)'")
        main_pat = re.compile(r"--'\s*(?P<add_maybe>\+)?color\s*=\s*(?P<rest>.+)'")

        r = ref_pat.search(text)
        if r:
            if r.group('add_maybe'):
                add_to_default = True
            refs_str = r.group('rest').strip()
            refs = [ref.strip() for ref in refs_str.split(",")]
            for ref in refs:
                if ref in shared.color_specs:
                    main = shared.color_specs[ref]
                    full += self.get_color_specs_helper(main)
                elif ('+' + ref) in shared.color_specs:
                    main = shared.color_specs['+' + ref]
                    full += self.get_color_specs_helper(main)
                else:
                    raise Exception("Cannot find color_spec '%s'" % ref)
        else:
            r = main_pat.search(text)
            #---'color=name@cash on hand|Office supplies~VAT input~Custom tax//ucode~~115020';
            if r:
                main = r.group('rest').strip()
                if r.group('add_maybe'):
                    add_to_default = True

                full += self.get_color_specs_helper(main)

        return full, add_to_default

    def run_cli(self, say_bye = True):
        pgexecute = self.pgexecute
        logger = self.logger
        original_less_opts = self.adjust_less_opts()

        completer = self.completer
        self.refresh_completions()
        key_binding_manager = pgcli_bindings(self.vi_mode)
        print('Version:', __version__)
        print('Chat: https://gitter.im/dbcli/pgcli')
        print('Mail: https://groups.google.com/forum/#!forum/pgcli')
        print('Home: http://pgcli.com')

        # ============== preparation ================================
        for name, spec_str in shared.color_specs.items():
            specs = self.get_color_specs_helper(spec_str)
            if name.startswith('+'):
                shared.default_color_specs += specs

        for mod in "random string sys os glob time uuid re".split():
            shared.info[mod] = __import__(mod)

        import sbox
        reload(sbox)
        shared.info["sbox"] = sbox

        try:
            shared.info["shared"] = shared
            from hsdl.common import general
            shared.info["general"] = general
        except:
            pass
        # ==========================================================

        def prompt_tokens(cli):
            return [(Token.Prompt,  '%s> ' % pgexecute.dbname)]

        get_toolbar_tokens = create_toolbar_tokens_func(key_binding_manager)
        layout = create_default_layout(lexer=PostgresLexer,
                                       reserve_space_for_menu=True,
                                       get_prompt_tokens=prompt_tokens,
                                       get_bottom_toolbar_tokens=get_toolbar_tokens,
                                       extra_input_processors=[
                                           HighlightMatchingBracketProcessor(),
                                       ])
        buf = PGBuffer(always_multiline=self.multi_line, completer=completer,
                history=FileHistory(os.path.expanduser('~/.pgcli-history')),
                complete_while_typing=Always())
        cli = CommandLineInterface(create_eventloop(),
                style=style_factory(self.syntax_style),
                layout=layout, buffer=buf,
                key_bindings_registry=key_binding_manager.registry,
                on_exit=AbortAction.RAISE_EXCEPTION)

        try:
            while True:
                document = cli.read_input()

                # The reason we check here instead of inside the pgexecute is
                # because we want to raise the Exit exception which will be
                # caught by the try/except block that wraps the pgexecute.run()
                # statement.
                if quit_command(document.text):
                    raise EOFError

                try:
                    document = self.handle_editor_command(cli, document)
                except RuntimeError as e:
                    logger.error("sql: %r, error: %r", document.text, e)
                    logger.error("traceback: %r", traceback.format_exc())
                    click.secho(str(e), err=True, fg='red')
                    continue

                # Keep track of whether or not the query is mutating. In case
                # of a multi-statement query, the overall query is considered
                # mutating if any one of the component statements is mutating
                mutating = False

                try:
                    logger.debug('sql: %r', document.text)
                    successful = False
                    # Initialized to [] because res might never get initialized
                    # if an exception occurs in pgexecute.run(). Which causes
                    # finally clause to fail.
                    res = []
                    orig_start = time()

                    the_text = document.text

                    shared.entered_code = ''      # this gets filled in
                    shared.executed_sql = ''      # this gets filled in

                    # load file
                    if the_text.strip().startswith("?!"):
                        path = the_text.strip()[2:].strip()
                        path = path.replace(";", "").strip()
                        if os.path.isfile(path):
                            fin = open(path, 'rb')
                            the_text = fin.read()
                            fin.close()
                            self.display_colorized_sql(the_text)
                        else:
                            print("File does not exist")
                            continue


                    elif the_text.strip().startswith("??"):
                        the_text = the_text.strip()
                        if the_text.endswith(';'):
                            the_text = the_text[:-1]
                        parts = the_text.split()
                        if len(parts) > 1:
                            the_text = ";\n".join(["select * from " + tab for tab in parts[1:]]) + ";"
                            self.display_colorized_sql(the_text)
                        else:
                            print("Invalid ?? command; please table name(s)")
                            continue

                    elif the_text.strip().startswith("?#"):
                        the_text = the_text.strip()
                        if the_text.endswith(';'):
                            the_text = the_text[:-1]
                        parts = the_text.split()
                        if len(parts) > 1:
                            the_text = ";\n".join([("\\d " + tab + "; select count(*) from " + tab) for tab in parts[1:]]) + ";"
                            self.display_colorized_sql(the_text)
                        else:
                            print("Invalid ?? command; please table name(s)")
                            continue


                        path = the_text.strip()[2:].strip()
                        path = path.replace(";", "").strip()
                        if os.path.isfile(path):
                            fin = open(path, 'rb')
                            the_text = fin.read()
                            fin.close()
                            self.display_colorized_sql(the_text)


                    local_color_specs = []
                    add_to_default = False

                    try:
                        local_color_specs, add_to_default = self.get_color_specs(the_text)
                    except Exception as e:
                        traceback.print_exc()
                        raise e

                    shared.local_color_specs = local_color_specs

                    # if no color specs were provided, leave the decision to add to defaults alone
                    if local_color_specs:
                        shared.local_color_specs_add_to_default = add_to_default

                    macro_invoke_pat = re.compile(r'@(?P<name>[a-zA-Z0-9_]+)\((?P<args>[^)]+)?\)')
                    assign_pat = re.compile(r'^(?P<name>[a-zA-Z0-9_]+!?)\s*=\s*', re.DOTALL)

                    # Run the query.
                    if the_text.strip().startswith('def '):

                        name, fun, lambda_code = get_lambda(the_text, shared.info)
                        if name:
                            shared.macros[name] = (fun, lambda_code, the_text)
                            #shared.macros[name] = (fun, code, the_text)
                            #save_macros(None,None,None)    # TODO
                            #print("???????????", the_text)
                            #for name, (fun, code) in sorted(shared.macros.items()):
                            #    print(name,":",code)
                            continue
                    else:
                        the_text = the_text.strip()

                        r = assign_pat.search(the_text)
                        shared._it_var_name = ''

                        if r:
                            shared._it_var_name = r.group('name')
                            the_text = the_text[r.end():].strip()

                        # see if we can do some {{ }} interpolation

                        text_after_interpolation = pgspecial.eval_python_slots(the_text, shared.info)
                        shared.entered_code = the_text

                        if text_after_interpolation != the_text:
                            self.display_colorized_sql(text_after_interpolation)
                            the_text = text_after_interpolation

                        try:
                            shared.info['macros'] = shared.macros
                            shared.info['subinterp'] = pgspecial.subinterp
                            shared.info['info'] = shared.info
                            shared.info['_clean'] = pgspecial.clean_for_sql_insertion
                            shared.info['_OR'] = pgspecial.sql_helper_or
                            shared.info['_AND'] = pgspecial.sql_helper_and
                            shared.info['_BETWEEN'] = pgspecial.sql_helper_between
                            r = macro_invoke_pat.search(the_text)
                            if r:
                                res = eval(simple_invoke(the_text), shared.info, shared.info)
                                self.display_colorized_sql(res)
                                #name, fun, code = get_lambda(input, shared.info)
                                #res = check_invoke_macro(the_text.strip(), shared.macros)
                                if res:
                                    the_text = res

                            if the_text.startswith('?'):
                                the_text = ''

                        except Exception as e:
                            traceback.print_exc()


                    # squirrel this away for later
                    shared.executed_sql = the_text

                    res = pgexecute.run(the_text)
                    successful = True
                    output = []
                    total = 0
                    for title, cur, headers, status in res:
                        logger.debug("headers: %r", headers)
                        logger.debug("table:", self.table_format)
                        logger.debug("status: %r", status)
                        start = time()
                        threshold = 1000
                        if (is_select(status) and
                                cur and cur.rowcount > threshold):
                            click.secho('The result set has more than %s rows.'
                                    % threshold, fg='red')
                            if not click.confirm('Do you want to continue?'):
                                click.secho("Aborted!", err=True, fg='red')
                                break

                        output.extend(format_output(title, cur, headers,
                            status, self.table_format))
                        end = time()
                        total += end - start
                        mutating = mutating or is_mutating(status)

                except KeyboardInterrupt:
                    # Restart connection to the database
                    pgexecute.connect()
                    logger.debug("cancelled query, sql: %r", document.text)
                    click.secho("cancelled query", err=True, fg='red')
                except NotImplementedError:
                    click.secho('Not Yet Implemented.', fg="yellow")
                except OperationalError as e:
                    reconnect = True
                    if ('server closed the connection' in utf8tounicode(e.args[0])):
                        reconnect = click.prompt('Connection reset. Reconnect (Y/n)',
                                show_default=False, type=bool, default=True)
                        if reconnect:
                            try:
                                pgexecute.connect()
                                click.secho('Reconnected!\nTry the command again.', fg='green')
                            except OperationalError as e:
                                click.secho(str(e), err=True, fg='red')
                    else:
                        logger.error("sql: %r, error: %r", document.text, e)
                        logger.error("traceback: %r", traceback.format_exc())
                        click.secho(str(e), err=True, fg='red')
                except Exception as e:
                    logger.error("sql: %r, error: %r", document.text, e)
                    logger.error("traceback: %r", traceback.format_exc())
                    click.secho(str(e), err=True, fg='red')
                else:
                    click.echo_via_pager('\n'.join(output))
                    if pgspecial.TIMING_ENABLED:
                        duration = time() - orig_start
                        print('Command Time:', duration)
                        print('Format Time:', total)

                # Refresh the table names and column names if necessary.
                if need_completion_refresh(document.text):
                    self.refresh_completions()

                # Refresh search_path to set default schema.
                if need_search_path_refresh(document.text):
                    logger.debug('Refreshing search path')
                    completer.set_search_path(pgexecute.search_path())
                    logger.debug('Search path: %r', completer.search_path)

                query = Query(document.text, successful, mutating)
                self.query_history.append(query)

        except EOFError:
            if say_bye:
                print ('Goodbye!')
        finally:  # Reset the less opts back to original.
            logger.debug('Restoring env var LESS to %r.', original_less_opts)
            os.environ['LESS'] = original_less_opts

    def adjust_less_opts(self):
        less_opts = os.environ.get('LESS', '')
        self.logger.debug('Original value for LESS env var: %r', less_opts)
        os.environ['LESS'] = '-RXF'

        #if 'X' not in less_opts:
            #os.environ['LESS'] += 'X'
        #if 'F' not in less_opts:
            #os.environ['LESS'] += 'F'

        return less_opts

    def refresh_completions(self):
        completer = self.completer
        completer.reset_completions()

        pgexecute = self.pgexecute

        # schemata
        completer.set_search_path(pgexecute.search_path())
        completer.extend_schemata(pgexecute.schemata())

        # tables
        completer.extend_relations(pgexecute.tables(), kind='tables')
        completer.extend_columns(pgexecute.table_columns(), kind='tables')

        # views
        completer.extend_relations(pgexecute.views(), kind='views')
        completer.extend_columns(pgexecute.view_columns(), kind='views')

        # functions
        completer.extend_functions(pgexecute.functions())

        # types
        completer.extend_datatypes(pgexecute.datatypes())

        # databases
        completer.extend_database_names(pgexecute.databases())

    def get_completions(self, text, cursor_positition):
        return self.completer.get_completions(
            Document(text=text, cursor_position=cursor_positition), None)

@click.command()
# Default host is '' so psycopg2 can default to either localhost or unix socket
@click.option('-h', '--host', default='', envvar='PGHOST',
        help='Host address of the postgres database.')
@click.option('-p', '--port', default=5432, help='Port number at which the '
        'postgres instance is listening.', envvar='PGPORT')
@click.option('-U', '--user', envvar='PGUSER', help='User name to '
        'connect to the postgres database.')
@click.option('-W', '--password', 'prompt_passwd', is_flag=True, default=False,
        help='Force password prompt.')
@click.option('-w', '--no-password', 'never_prompt', is_flag=True,
        default=False, help='Never prompt for password.')
@click.option('-v', '--version', is_flag=True, help='Version of pgcli.')
@click.option('-d', '--dbname', default='', envvar='PGDATABASE',
        help='database name to connect to.')
@click.argument('database', default=lambda: None, envvar='PGDATABASE', nargs=1)
@click.argument('username', default=lambda: None, envvar='PGUSER', nargs=1)
def cli(database, user, host, port, prompt_passwd, never_prompt, dbname,
        username, version):

    if version:
        print('Version:', __version__)
        sys.exit(0)

    pgcli = PGCli(prompt_passwd, never_prompt)

    # Choose which ever one has a valid value.
    database = database or dbname
    user = username or user

    if '://' in database:
        pgcli.connect_uri(database)
    else:
        pgcli.connect(database, host, user, port)

    pgcli.logger.debug('Launch Params: \n'
            '\tdatabase: %r'
            '\tuser: %r'
            '\thost: %r'
            '\tport: %r', database, user, host, port)

    pgcli.run_cli()

def pg(username, password, server, port, db):
    pgcli = PGCli(False, False)
    pgcli.connect_uri("postgresql://%s:%s@%s:%s/%s" % (username, password, server, port, db))
    pgcli.run_cli(say_bye = False)

# Utility for quick development of pgcli interpreter. Start in python repl, run rehash().
# Try new code in pgcli repl. Press ctl-D, re-enter python repl, re-run rehash() to try
# new code.
def rehash():
    from pgcli.packages import pgspecial
    reload(pgspecial)
    from pgcli import shared
    reload(shared)
    from pgcli import main
    reload(main)
    main.pg('server', 'username','password', 5432, 'khtaxes')


def format_output(title, cur, headers, status, table_format):
    output = []
    if title:  # Only print the title if it's not None.
        output.append(title)
    if cur:
        headers = [utf8tounicode(x) for x in headers]
        if is_expanded_output():
            output.append(expanded_table(cur, headers))
        else:
            output.append(tabulate(cur, headers, tablefmt=table_format,
                missingval='<null>'))
    if status:  # Only print the status if it's not None.
        output.append(status)
    return output

def need_completion_refresh(queries):
    """Determines if the completion needs a refresh by checking if the sql
    statement is an alter, create, drop or change db."""
    for query in sqlparse.split(queries):
        try:
            first_token = query.split()[0]
            return first_token.lower() in ('alter', 'create', 'use', '\\c',
                    '\\connect', 'drop')
        except Exception:
            return False

def need_search_path_refresh(sql):
    """Determines if the search_path should be refreshed by checking if the
    sql has 'set search_path'."""
    return 'set search_path' in sql.lower()

def is_mutating(status):
    """Determines if the statement is mutating based on the status."""
    if not status:
        return False

    mutating = set(['insert', 'update', 'delete', 'alter', 'create', 'drop'])
    return status.split(None, 1)[0].lower() in mutating

def is_select(status):
    """Returns true if the first word in status is 'select'."""
    if not status:
        return False
    return status.split(None, 1)[0].lower() == 'select'

def quit_command(sql):
    return (sql.strip().lower() == 'exit'
            or sql.strip().lower() == 'quit'
            or sql.strip() == '\q'
            or sql.strip() == ':q')

if __name__ == "__main__":
    cli()
