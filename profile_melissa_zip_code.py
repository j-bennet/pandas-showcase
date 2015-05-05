import pandas as pd

from cProfile import Profile
from pstats import Stats
from memory_profiler import LineProfiler
from memory_profiler import show_results

from gocept.recordserialize import FixedWidthRecord
from argparse import ArgumentParser


class ZipCodeRecord(FixedWidthRecord):
    """
    Stores specifications of the fixed width ZIP code file to read.
    """

    encoding = 'utf-8'
    lineterminator = '\r\n'

    fields = [
        ('zip_code', 5, ' ', FixedWidthRecord.LEFT),
        ('state_code', 2, ' ', FixedWidthRecord.LEFT),
        ('city_name', 28, ' ', FixedWidthRecord.LEFT),
        ('zip_type', 1, ' ', FixedWidthRecord.LEFT),
        ('county_code', 5, ' ', FixedWidthRecord.LEFT),
        ('latitude', 7, ' ', FixedWidthRecord.LEFT),
        ('longitude', 8, ' ', FixedWidthRecord.LEFT),
        ('area_code', 3, ' ', FixedWidthRecord.LEFT),
        ('finance_code', 6, ' ', FixedWidthRecord.LEFT),
        ('city_official', 1, ' ', FixedWidthRecord.LEFT),
        ('facility', 1, ' ', FixedWidthRecord.LEFT),
        ('msa_code', 4, ' ', FixedWidthRecord.LEFT),
        ('pmsa_code', 4, ' ', FixedWidthRecord.LEFT),
        ('filler', 3, ' ', FixedWidthRecord.LEFT),
    ]


p = ArgumentParser()

p.add_argument('importer',
               choices=['pandas', 'gocept'],
               help='Which library to profile.')

p.add_argument('top_n',
               metavar='N',
               type=int,
               nargs='?',
               help='How many top lines from profiler output to display.')

p.add_argument('--memory',
               action='store_true',
               default=False,
               help='Whether to run the memory profiler.')

parsed_args = p.parse_args()


def main():
    """
    Init the profilers and run the tests.
    """

    global parsed_args

    fun = choose_func_to_run()
    pr = enable_time_profiler()
    mem = enable_mem_profiler(fun)

    fun()

    show_time_profiler_results(pr, parsed_args.top_n)
    show_mem_profiler_results(mem)


def choose_func_to_run():
    """
    Select a function to test according to provided arguments.
    :return: function
    """
    global parsed_args

    if parsed_args.importer == 'gocept':
        return run_gocept
    elif parsed_args.importer == 'pandas':
        return run_pandas


def enable_mem_profiler(fun):
    """
    Enable memory profiler if specified in arguments.
    :param fun: function to wrap
    :return: LineProfiler instance
    """
    global parsed_args

    mem = None

    if parsed_args.memory:
        mem = LineProfiler()
        mem.add_function(fun)
        mem.enable()

    return mem


def enable_time_profiler():
    """
    Enable time profiler.
    :return: cProfile.Profile
    """
    pr = Profile()
    pr.enable()
    return pr


def show_time_profiler_results(pr, top_records):
    """
    Show results of timed profiling.
    :param pr: profiler instance
    :param top_records: how many top function calls to show.
    """
    st = Stats(pr)
    st.strip_dirs()
    st.sort_stats('cumulative')
    st.print_stats(top_records)


def show_mem_profiler_results(mem):
    """
    Show results of memory profiling if enabled.
    :param mem: profiler instance
    """
    if mem:
        show_results(mem)
        mem.disable()


def run_gocept():
    """
    Load records with gocept.recordserialize.

    * Can specify column data types: no
    * Can read in chunks: no
    * Can skip columns: no
    * Can stream: no
    * Return type: array
    """
    zr = ZipCodeRecord()
    with open('data/ZIP.DAT', 'r') as f:
        records = zr.parse_file(f)
    print 'Records:', len(records)


def run_pandas():
    """
    Load records into pandas data frame.

    * Can specify column data types: yes
    * Can read in chunks: yes
    * Can skip columns: yes
    * Can stream: no
    * Return type: DataFrame
    """
    zp = pd.read_fwf(
        'data/ZIP.DAT',
        widths=[5, 2, 28, 1, 5, 7, 8, 3, 6, 1, 1, 4, 4, 3],
        names=['zip_code', 'state_code', 'city_name', 'type', 'county_fips',
               'lat', 'lon', 'area_code', 'fin_code', 'last_line',
               'facility', 'msa_code', 'pmsa_code', 'filler'],
        usecols=[0, 1, 2, 4, 5, 6, 7, 11, 12],
        converters={'zip_code': str, 'county_fips': str, 'area_code': str,
                    'msa_code': str, 'pmsa_code': str},
        header=None,
        skiprows=2
    )
    print 'Records:', len(zp)


if __name__ == '__main__':
    main()
