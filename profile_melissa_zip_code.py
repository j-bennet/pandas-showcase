import pandas as pd

from cProfile import Profile
from pstats import Stats
from memory_profiler import LineProfiler
from memory_profiler import show_results

from argparse import ArgumentParser


p = ArgumentParser()

p.add_argument('importer',
               choices=['pandas', 'gocept', 'fixedwidth'],
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
    elif parsed_args.importer == 'fixedwidth':
        return run_fixedwidth


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


def run_fixedwidth():
    """
    Load records with fixedwidth.FixedWidth.

    * Independent: yes
    * Small: yes
    * Can specify column data types: yes
    * Can read in chunks: manually
    * Can skip columns: no
    * Can stream: manually
    * Return type: parses every line as dict
    * Memory usage: minimal
    * Timing: around 2.5 sec
    """
    from fixedwidth.fixedwidth import FixedWidth
    fields = {
        'zip_code': {
            'required': True, 'type': 'string', 'length': 5,
            'padding': ' ', 'alignment': 'left', 'start_pos': 1
        },
        'state_code': {
            'required': True, 'type': 'string', 'length': 2,
            'padding': ' ', 'alignment': 'left', 'start_pos': 6
        },
        'city_name': {
            'required': True, 'type': 'string', 'length': 28,
            'padding': ' ', 'alignment': 'left', 'start_pos': 8
        },
        'zip_type': {
            'required': True, 'type': 'string', 'length': 1,
            'padding': ' ', 'alignment': 'left', 'start_pos': 36
        },
        'county_code': {
            'required': True, 'type': 'string', 'length': 5,
            'padding': ' ', 'alignment': 'left', 'start_pos': 37
        },
        'latitude': {
            'required': False, 'type': 'decimal', 'length': 7,
            'padding': ' ', 'alignment': 'left', 'start_pos': 42
        },
        'longitude': {
            'required': False, 'type': 'decimal', 'length': 8,
            'padding': ' ', 'alignment': 'left', 'start_pos': 49
        },
        'area_code': {
            'required': True, 'type': 'string', 'length': 3,
            'padding': ' ', 'alignment': 'left', 'start_pos': 57
        },
        'finance_code': {
            'required': True, 'type': 'string', 'length': 6,
            'padding': ' ', 'alignment': 'left', 'start_pos': 60
        },
        'city_official': {
            'required': False, 'type': 'string', 'length': 1,
            'padding': ' ', 'alignment': 'left', 'start_pos': 66
        },
        'facility': {
            'required': False, 'type': 'string', 'length': 1,
            'padding': ' ', 'alignment': 'left', 'start_pos': 67
        },
        'msa_code': {
            'required': True, 'type': 'string', 'length': 4,
            'padding': ' ', 'alignment': 'left', 'start_pos': 68
        },
        'pmsa_code': {
            'required': False, 'type': 'string', 'length': 4,
            'padding': ' ', 'alignment': 'left', 'start_pos': 72
        },
        'filler': {
            'required': False, 'type': 'string', 'length': 3,
            'padding': ' ', 'alignment': 'left', 'start_pos': 76
        }
    }

    zp = FixedWidth(config=fields)
    records = 0
    with open('data/ZIP.DAT', 'r') as f:
        for line in f:
            zp.line = line
            records += 1
            #print "{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}\t{7}\t{8}\t{9}".format(
            #    zp.data['zip_code'],
            #    zp.data['state_code'],
            #    zp.data['city_name'],
            #    zp.data['zip_type'],
            #    zp.data['county_code'],
            #    zp.data['latitude'],
            #    zp.data['longitude'],
            #    zp.data['area_code'],
            #    zp.data['msa_code'],
            #    zp.data['pmsa_code'])
            #if records == 100:
            #    break

    print 'Records:', records


def run_gocept():
    """
    Load records with gocept.recordserialize.

    * Independent: yes
    * Small: yes
    * Can specify column data types: no
    * Can read in chunks: no
    * Can skip columns: no
    * Can stream: no
    * Return type: array
    * Memory usage: about 170Mb (bad because it stores the whole list in memory)
    * Timing: around 2.5 sec
    """

    from gocept.recordserialize import FixedWidthRecord

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

    zr = ZipCodeRecord()
    with open('data/ZIP.DAT', 'r') as f:
        records = zr.parse_file(f)
    print 'Records:', len(records)


def run_pandas():
    """
    Load records into pandas data frame.

    * Independent: no
    * Small: no
    * Can specify column data types: yes
    * Can read in chunks: yes
    * Can skip columns: yes
    * Can stream: no
    * Return type: DataFrame
    * Memory usage: about 60Mb
    * Timing: around 0.7 sec
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
