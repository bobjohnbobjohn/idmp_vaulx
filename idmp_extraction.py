#!/usr/bin/env python
# author : bobjohn
# date   : 20150101


"""IDMP Vaulx-en-Velin station data extraction

extract data from the IDMP of Vaul-en-Velin, near Lyon (FR).

more info at : {EN} http://idmp.entpe.fr/
data can be downloaded here : {FR} http://idmp.entpe.fr/mesfr.htm
"""


import sys
import os
import getopt

#
# Usage and information about the script
#
PROGRAM = os.path.basename(sys.argv[0])
EMAIL = "bobjohn@fouinot.fr"
VERSION = "1.0"
USAGE = """{program} - extract IDMP station data. By {email}, version {version}.

Usage:
    {program} [options] file

Options:
    -m month    month = [1..12]. Use "-m m1,m2" for a range, m1<m2
    -d day      day = [1..31]. Use "-d d1,d2" for a range, d1<d2
    -h hour     hour = [0..23]. Use "-h h1,h2" for a range, h1<h2

    -p params   comma separated values (see -l option) case insentive

    -o file     output file, TSV format

    -s          script output: header is not printed, only values
    -l          list all allowed parameters (see any IDMP data file header)
    -u          displays this usage information

    file        Vaulx-en-Velin IDMP station data file (see http://idmp.entpe.fr/vaulx/mesfr.htm)
                the file can contain:
                    - 1 year  (e.g. vlx03.txt         = 2003)
                    - 1 month (e.g. vlx98QC_3.txt     = march 1998)
                    - 1 day   (e.g. vlx14QC_12_30.txt = 30th December 2014)

Examples:
    extract all parameters for the month of march of the given file:

        {program} -m 3 -o march.tsv vlx03.txt

    extract "dry bulb temperature" and "wind speed" for every working hours, for an entire month in this case:

        {program} -h 8,17 -p dbt,ws -o meteo_during_work.tsv vlx98QC_3.txt

    extract "relative humidity" for all the available period, one day only on this case:

        {program} -p rh -o humidity_lastDayOf2014.tsv vlx14QC_12_31.txt
""".format(program=PROGRAM, email=EMAIL, version=VERSION)


#
# Default settings
#
EXIT_SUCCESS = 0
EXIT_FAILURE = 1
HEADER_LENGTH = 33 # lines
PRINT_HEADER = True

MONTH, DAY, HOUR = 0, 1, 2
ALLOWED_VALUES = [[1,12],[1,31],[0,23]]
SLOT_NAME = ["Month","Day","Hour"]
PARAMS = {
    "alts" : [2, "altitude of the sun"],
    "azis" : [3, "azimuth of the sun (from North to East)"],
    "evg"  : [4, "global horizontal illuminance"],
    "evd"  : [5, "diffuse horizontal illuminance"],
    "evgn" : [6, "global vertical north illuminance"],
    "evge" : [7, "global vertical east illuminance"],
    "evgs" : [8, "global vertical south illuminance"],
    "evgw" : [9, "global vertical west illuminance"],
    "eeg"  : [10, "global horizontal irradiance"],
    "eed"  : [11, "diffuse horizontal irradiance"],
    "lvz"  : [12, "zenith luminance (11 degree aperture)"],
    "rh"   : [13, "relative humidity"],
    "wd"   : [14, "wind direction (from North to East)"],
    "ws"   : [15, "wind speed"],
    "dbt"  : [16, "dry bulb temperature"],
    "cvf"  : [17, "illuminance shadow band correction factor"],
    "cef"  : [18, "irradiance shadow band correction factor"],
    "ees"  : [19, "direct horizontal irradiance"],
    "uva"  : [20, "global horizontal UVA irradiance"],
    "uvb"  : [21, "global horizontal UVB irradiance"]
}


def print_usage():
    """Print usage information about the script"""
    print USAGE


def print_params():
    """Print the list of allowed parameters"""
    print "List of allowed parameters : "
    for param, descr in sorted(PARAMS.items()):
        print "\t", param, ":", descr[1]


def parse_options():
    """Deal with command line parameters"""
    try:
        opts, args = getopt.getopt(sys.argv[1:], "ulsm:d:h:p:o:")
    except getopt.GetoptError as err:
        sys.stderr.write(err)
        sys.stderr.write("\nUse {program} -u to have usage information.\n".format(program=PROGRAM))
        sys.exit(EXIT_FAILURE)

    month = None
    day = None
    hour = None
    params = None
    output = None
    global PRINT_HEADER

    for o, a in opts:
        if o == "-u":
            print_usage()
            sys.exit(EXIT_SUCCESS)
        elif o == "-l":
            print_params()
            sys.exit(EXIT_SUCCESS)
        elif o == "-s":
            PRINT_HEADER = False
        elif o == "-m":
            month = check_values(MONTH, a.split(','))
        elif o == "-d":
            day = check_values(DAY, a.split(','))
        elif o == "-h":
            hour = check_values(HOUR, a.split(','))
        elif o == "-p":
            params = check_params(a.split(','))
        elif o == "-o":
            output = a
        else:
            assert False, "Unhandled option"

    if len(args) != 1:
        sys.stderr.write(
            "Missing file name.\nUse {program} -u to have usage information.\n"
            .format(program=PROGRAM))
        sys.exit(EXIT_FAILURE)

    if not params:
        sys.stderr.write("No parameter selected, use -p option.\n")
        sys.exit(EXIT_FAILURE)


    if len(opts) < 1:
        sys.stderr.write(
            "You must apply a filter.\nUse {program} -u to have usage information.\n"
            .format(program=PROGRAM))
        sys.exit(EXIT_FAILURE)
    else:
        input = args[0]

    return month, day, hour, params, input, output


def extract_data(month, day, hour, params, input, output):
    """Print or save in output file the extraction of parameters:
    - for a range of month or/and day or/and hour (each can also be a single value)
    - from an input IDMP station data file
    """
    src = open(input, "r")
    if output:
        dst = open(output, "w")
    else:
        dst = None

    if PRINT_HEADER:
        write_header(["MM/DD/YYYY","hh:mm"] + params, dst)

    nb_filter = len(filter(None, [month, day, hour]))

    i = 1
    for line in src:
        if i < HEADER_LENGTH:       # skip header lines
            i = i + 1
        else:
            fields = line.split("\t")
            datetime = fields[0].split(" ")
            if selected_slot(datetime, nb_filter, month, day, hour):
                extract_params(datetime, params, fields, dst)

    src.close()
    if dst:
        dst.close()


def extract_params(datetime, params, fields, dst):
    """Write values of requested parameters in TSV format"""
    out(datetime + [fields[i] for i in [PARAMS[p][0] for p in params]], dst)


def write_header(params, dst):
    """The first line contains the name of the requested fields"""
    out(params, dst)


def check_params(raw_params):
    """Check if parameters are valid"""
    params = map(str.lower, raw_params)

    try:
        [PARAMS[p] for p in params]
    except:
        sys.stderr.write(
            "Wrong parameter name.\nUse {program} -l to see all allowed parameters.\n"
            .format(program=PROGRAM))
        sys.exit(EXIT_FAILURE)
    
    return params


def check_values(slot, v_range):
    """Verify if month/day/hour option is valid"""

    if len(v_range) > 2:
        sys.stderr.write(
            "{slot_name} can have only 1 or 2 values.\nUse {program} -u to have usage information.\n"
            .format(slot_name=SLOT_NAME[slot], program=PROGRAM))
        sys.exit(EXIT_FAILURE)

    for v in v_range:
        try:
            v = float(v)        # v must be a number
        except:
            sys.stderr.write(
                "{slot_name}(s) must be an number"
                .format(slot_name=SLOT_NAME[slot]))
            sys.exit(EXIT_FAILURE)

        # floating values are not allowed
        # mandatory code, since int(floatNumber) would work and return a truncated value.
        # Hence, the user could believe that the script works with floats, which is not the case
        if not float(v).is_integer():
            sys.stderr.write(
                "{slot_name}(s) value must be an integer, not a float"
                .format(slot_name=SLOT_NAME[slot]))
            sys.exit(EXIT_FAILURE)

        if int(v) < ALLOWED_VALUES[slot][0] or ALLOWED_VALUES[slot][1] < int(v):
            sys.stderr.write(
                "{slot_name}(s) value must be between {min} and {max}.\n"
                .format(slot_name=SLOT_NAME[slot], min=ALLOWED_VALUES[slot][0], max=ALLOWED_VALUES[slot][1]))
            sys.exit(EXIT_FAILURE)

    if len(v_range) == 2:
        if int(v_range[0]) >= int(v_range[1]):
            sys.stderr.write(
                "{slot_name} error : when using ranges, first value must be lower.\n"
                .format(slot_name=SLOT_NAME[slot]))
            sys.exit(EXIT_FAILURE)

    return map(int, v_range)      # convert values into int, once for all
        

def selected_slot(slot, nb_filter, month, day, hour):
    """check whether the date/time is within the selected range
    slot format : ["MM/DD/YYYY", "hh:mm"]
    month/day/hour are user specified filters, none is mandatory
    """
    date = map(int, slot[0].split("/"))
    time = map(int, slot[1].split(":"))
    
    score = 0
    score += inside(date[0], month)
    score += inside(date[1], day)
    score += inside(time[0], hour)

    return score == nb_filter


def inside(date_or_time, m_d_h):
    """Check whether the month/day/hour (m_d_h) is in the passed date or time"""
    try:
        if len(m_d_h) == 1:     # value
            return date_or_time == m_d_h[0]
        else:                # range
            return m_d_h[0] <= date_or_time <= m_d_h[1]
    except:
        return False


def out(data, dst):
    """Write or print data depending on output value"""
    if dst:
        dst.write("\t".join([str(d) for d in data]))
        dst.write("\n")
    else:
        print "\t".join([str(d) for d in data])


def main():
    options = parse_options()
    extract_data(*options)
    sys.exit(EXIT_SUCCESS)


if __name__ == "__main__":
    main()
