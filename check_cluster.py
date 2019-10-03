#!/usr/bin/python
import copy
import errno
import logging
import math
from optparse import OptionParser
import os
import pkgutil
import re
import shutil
import subprocess
import sys
import time
import traceback
from threading import Timer

isi_accountant = None
uconfig = None

try:
    import isilon.accountant.isi_accountant as isi_accountant
    import isilon.accountant.config as uconfig
except ImportError:
    # (no Accountant implementation available; skip)
    pass

import checkcluster
from checkcluster import *

sys.path.append(os.path.abspath(os.path.dirname(__file__)))

import sendmail

# List of checks that we don't want to raise error code.
# Each check result will be defered if DEFER_LIST[k] is true.
# e.g.: 'node_fw': ['bug 12345'], or
#       'node_fw': ['any valid comment'],...
DEFER_LIST = {
        'node_fw': [],
        'drive_fw': [],
        'event_message_expand': ['Deferred until after pipeline release']
        }

# Process the command line
"""
Description:
This tool will check a series of conditions on a node or nodes in a cluster,
reporting to the user as it runs.  Examples of conditions that are checked are
device status, logfile errors, ecc errors, etc.

Logging:
There are 4 logging options that are called to play when with --ld and --mail:
1. By default a log file is created in /ifs/data/Isilon_Support/cc_logs.
2. Use the --ld <dir name> option to save the log to a different
local directory.
3. Use --ld <dir name on qalogserver> and --mail to save a local copy
of the logs in the default location on the node AND a copy to a designated
directory on the qalogserver. The copy on the logserver will be accessible
via HTTP. --mail sends the user a summary of the monitor results
which includes a link to the full report.
If you use the --mail flag, the --ld <log dir> flag is MANDATORY.

Data files:
In addition, CSV files are created in the same directory that tracks
data points from some of the checks.
The 3 files are:
check_cluster_data.csv
check_cluster_fstat_tracking.csv
check_cluster_mem_tracking.csv

The format for these file is:
timestamp,node_id,check_tag,value

Modes:
A node_id of 0 is used when the data applies to the entire cluster
rather than a single node.
Run modes available are single report mode, and continuous mode.
By default,a single pass of the chosen checks is run, and
the script exits.
The --monitor mode lets the script run in the background as a daemon,
running the set of checks at each interval.

Tuning checks:
By default, all checks are run at each pass. Use the --show option to see the
see all available checks. Individual checks can be run with the --check
option. Defined subsets of checks can be run by selecting an
available 'set.' Sets can be created by editing the 'Script Data' section
of this script.

Message parsing:
The 'message' checks searches the /var/log/messages* files on all nodes
for errors and warnings. Specify the patterns to search for and define
custom error levels in the \$QA_dir/etc/check_cluster.filters file.
Read the comments found in that file for details.
In continuous mode, only messages from the last interval will be matched.

Usage: /usr/local_qa/bin/check_cluster.py [options]

==> Basic Options:
--batch num_nodes
        Query nodes in batches of num_nodes to reduce the load on the
        host node (default is to query all nodes at once)

--l logfilename
        Save logfile as 'logfilename'
        (defaults to check_cluster_timestamp.log)

--lld logdir
        Save timestamped logfile to logdir
        (defaults to /ifs/data/Isilon_Support/cc_logs)

--rld logdir
        Save timestamped logfile to mounted log server (qalogserver.west.isilon.com)
	under /usr/local/qa/log/<user determined direcory>
	Also provides URL link to the log location

--mail email_address[,email_address,...]
        Email the results to email_address (should be valid \"user\@domain\").
        If this is set, but --from is not, the first email address will
        be used for the From: portion of the email header

--mem_samples=N
        Save N memory samples for analyzation (default is 100)

--show
        Show list of available checks, and sets of checks

-h
        Print script usage

--debug
        Print debug output

--version
        Print the revision number of the script and exit; this number will
        also be included in each report

--verbose, --noverbose
        Control verbose mode (by default, script will run quietly when an
        --interval is specified, and verbosely if not)

-n
        No color codes in output to screen.

==> Specific to the 'messages' check:

--add_filter extra_filter1[,ex_filter2,...]
        Use filter files in addition to the default filters file;
        extra_filter1 is simply the filename, not the full
        pathname; filter files must be located in \$QA/etc

--all_messages
        Applies to the 'messages' check only; search all /var/log/messages*
        files; default is to only search the latest /var/log/messages file

--sort [t*|n|0]
        Provide sorted results at the end of the script rather than
        inline results; default is on and by totals; use
        n to sort by node; use 0 to turn off the sorted report

==> Specific to the memory leak checks:

--min_samples N
        Only calculate trend line if N samples are found

--max_samples M
        Limit trend line to the most recent M data points

--slope M
        Filter lines using a minimum slope of M (default is $nice_min_slope)

--cc M
        Filter lines using a minimum correlation coefficient of M
        (default is $min_cc)

--vmlogs M
        Copy M+2 vmlogs for memory processing
        (default is $min_vmlogs, max limit 19)

==> Default mode (single report) options

--check check_name[,check_name,...]
        Run the specified checks only; defaults to running all checks

==> Monitor mode options
--monitor
        Run as a daemon. In daemon mode, two runs are available and
        configurable. The first runs a set of checks at a given interval.
        By default, the 'interval' set is run every 15 minutes. Use the
        --interval and --set options to override these values.
        A second run is hourly (from the start of the script), and runs
        the 'hourly' set. Use the --set option to change the set.
        Note: When using this option, the 'messages' check will only match
        messages occurring since the previous run (either interval or hourly).

--interval N
        If --monitor is used, change the interval from the default of
        15 minutes to N minutes for the first set of checks

==> Examples (single report):
Run the tool in default mode:
    /usr/local_qa/bin/check_cluster.py
---> Run individual checks or sets of checks

Show all available checks and sets
    /usr/local_qa/bin/check_cluster.py --show

Run individual checks
    /usr/local_qa/bin/check_cluster.py --check=messages
    /usr/local_qa/bin/check_cluster.py --check="messages,cpu,mbufs"

---> Running in monitor mode
Run the script as a daemon, with default checks
    /usr/local_qa/bin/check_cluster.py --monitor

Run checks every 15 minutes, sending an email report at each interval
    /usr/local_qa/bin/check_cluster.py --monitor --interval 15 --mail "user\@domain"

"""

########################################
# Sub class

class StreamToLogger(object):
   """
   Fake file-like stream object that redirects writes to a logger instance.
   """
   def __init__(self, logger, logdir, log_level=logging.INFO):
      self.logger = logger
      self.log_level = log_level
      self.linebuf = ''
      self.logpath = '%s/output.log' % logdir

   def write(self, buf):
      for line in buf.rstrip().splitlines():
         self.logger.log(self.log_level, line.rstrip())


class Cluster(object):
    def __init__(self, check_state, cluster_info, opt_info):
        self.check_state = check_state
        self.cluster_info = cluster_info
        self.opt_info = opt_info

    def output(self, string = None, status = None, node = None, caller = None):
        print_to_stderr = False
        coff = "\e[0m"
        con = ''
        beg = ""
        end = "\n"
        tmp_stag = ""
        tmp_etag = ""
        log_stag = ""
        log_etag = ""
        div = "------------------------------------"
        # Verify incoming data
        monitor = self.opt_info['monitor']

        if status is None:
            status = ""
            con = "\e[37m"       # gray
            tmp_stag = '<h5>'
            tmp_etag = "</h5>"
            log_stag = '<h5>'
            log_etag = "</h5>"

        elif status == -1:
            status = ""
            con = "\e[131m"     # bright red
            tmp_stag = '''<h5><font color="#800080" style="font-weight: bold margin: 0px"> WARN: '''
            tmp_etag = "</font></h5>"
            log_stag = '''<h5><font color="#800080" style="font-weight: bold margin: 0px"> WARN: '''
            log_etag = "</font></h5>"

        elif status == -2:
            check_timestamp, timestamp_in_seconds = timestamp()
            string = "\n%s\n%s\n Started at %s\n%s\n" % (caller, div, check_timestamp, string)
            status = ""
            tmp_stag = '<h5><font color="#0000FF">'
            tmp_etag = "</font></h5>"
            log_stag = '<h5><font color="#0000FF">'
            log_etag = "</font></h5>"

        elif status == -3:
            check_timestamp, timestamp_in_seconds = timestamp()
            string = "Completed at %s\n%s\n%s" % (check_timestamp, string, div)
            status = ""
            tmp_stag = '<h5>'
            tmp_etag = "</h5>"
            log_stag = '<h5>'
            log_etag = "</h5>"

        elif status == -4:    # Current Mem Leaks
            print_to_stderr = True
            if node and self.opt_info['sort']:
                if 'CRNT_LEAK' not in self.cluster_info['cluster_events'][node]:
                    self.cluster_info['cluster_events'][node]['CRNT_LEAK'] = ""
                self.cluster_info['cluster_events'][node]['CRNT_LEAK'] += string + '\n'
            status = " CRNT_LEAK:  "
            con = "\e[32m"        # green
            tmp_stag = '''<pre><font color="#FF8040" style="font-weight: bold margin: 0px">'''
            tmp_etag = "</font></pre>"
            log_stag = '<pre><font color="#FF8040" style="font-weight: bold margin: 0px">'
            log_etag = "</font></pre>"

        elif status == -5:    # Past Mem Leaks
            print_to_stderr = True
            if node and self.opt_info['sort']:
                if 'PAST_LEAK' not in self.cluster_info['cluster_events'][node]:
                    self.cluster_info['cluster_events'][node]['PAST_LEAK'] = ""
                self.cluster_info['cluster_events'][node]['PAST_LEAK'] += string + '\n'
            status = " PAST_LEAK:  "
            con = "\e[33m"        # yellow
            tmp_stag = '''<pre><font color="#FF8040" style="font-weight: bold margin: 0px">'''
            tmp_etag = "</font></pre>"
            log_stag = '''<pre><font color="#FF8040" style="font-weight: bold margin: 0px">'''
            log_etag = "</font></pre>"

        elif status == 0:
            status = "PASS:  "
            con = "\e[132m"      # bright green
            tmp_stag = '''<font color="#00FF00" style="font-weight: bold margin: 0px">'''
            tmp_etag = "</font>"
            log_stag = '''<font color="#00FF00" style="font-weight: bold margin: 0px">'''
            log_etag = "</font>"

        elif status == 1:    # Failure Errors
            print_to_stderr = True
            if node and self.opt_info['sort']:
                if 'FAIL' not in self.cluster_info['cluster_events'][node]:
                    self.cluster_info['cluster_events'][node]['FAIL'] = ""
                self.cluster_info['cluster_events'][node]['FAIL'] += string + '\n'
            status = " FAIL:  "
            con = "\e[131m"      # bright red
            tmp_stag = '''<pre><font color="#FF0000" style="font-weight: bold margin: 0px">'''
            tmp_etag = "</font></pre>"
            log_stag = '''<pre><font color="#FF0000" style="font-weight: bold margin: 0px">'''
            log_etag = "</font></pre>"

        elif status == 2:    # Execution Errors
            print_to_stderr = True
            if node and self.opt_info['sort']:
                if 'EXEC' not in self.cluster_info['cluster_events'][node]:
                    self.cluster_info['cluster_events'][node]['EXEC'] = ""
                self.cluster_info['cluster_events'][node]['EXEC'] += string + '\n'
            status = " EXEC:  "
            con = "\e[131m"     # bright red
            tmp_stag = '''<pre><font color="#800080" style="font-weight: bold margin: 0px">'''
            tmp_etag = "</font></pre>"
            log_stag = '''<pre><font color="#800080" style="font-weight: bold margin: 0px">'''
            log_etag = "</font></pre>"

        elif status == 3: 	# For summary outptut
            print_to_stderr = True
            status = ""
            con = "\e[131m"     # bright red
            tmp_stag = '''<font color="#800080" style="font-weight: bold margin: 0px">'''
            tmp_etag = "</font>"
            log_stag = '''<font color="#800080" style="font-weight: bold margin: 0px">'''
            log_etag = "</font>"

        elif status == 4:    # Failure Block Errors
            print_to_stderr = True
            if node and self.opt_info['sort']:
                if 'FAIL' not in self.cluster_info['cluster_events'][node]:
                    self.cluster_info['cluster_events'][node]['FAIL'] = ""
                self.cluster_info['cluster_events'][node]['FAIL'] += string
            status = " FAIL:  "
            con = "\e[131m"      # bright red
            tmp_stag = '''<pre><font color="#FF0000" style="font-weight: bold margin: 0px">'''
            tmp_etag = "</font></pre>"
            log_stag = '''<pre><font color="#FF0000" style="font-weight: bold margin: 0px">'''
            log_etag = "</font></pre>"
            out_string = 'Node {node}: {log_stag}{status}{lstring}{log_etag}\n'.\
                format(
                node = node, log_stag = log_stag, status = status,\
                lstring = string, log_etag = log_etag
            )
            self.check_state['log_buffer'] += out_string
            self.check_state['summary_buffer'] += out_string
            self.check_state['tmp_buffer'] += out_string

            return  

        # Finally print the results out
        if self.opt_info['verbose']:
            if not self.opt_info['nocolor']:
                coff, con = ('', '')
            if print_to_stderr:
                if node:
                    out_string = 'Node {node}: {con}{status}{coff}{coff}{string}{end}'.\
                    format(
                        node = node, con = con, status = status,\
                        coff = coff, string = string, end = end
                    )
                    sys.stderr.write(out_string)
                else:
                    out_string = '{con}{status}{coff}{coff}{string}{end}'.\
                    format(
                        con = con, status = status, coff = coff,\
                        string = string, end = end
                    )
                    sys.stderr.write(out_string)
            else:
                if node:
                    out_string = 'Node {node}: {con}{status}{coff}{coff}{string}{end}'.\
                    format(
                        node = node, con = con, status = status,\
                        coff = coff, string = string, end = end
                    )
                    sys.stderr.write(out_string)
                else:
                    out_string = '{con}{status}{coff}{coff}{string}{end}'.\
                    format(
                        con = con, status = status, coff = coff,\
                        string = string, end = end
                    )
                    sys.stderr.write(out_string)

        # replace any <> with valid html strings
        lstrings = string.split('\n')
        # Print the line to the log
        for lstring in lstrings:
            if self.check_state['logfile_open']:
                if node:
                    out_string = 'Node {node}: {log_stag}{status}{lstring}{log_etag}\n'.\
                    format(
                        node = node, log_stag = log_stag, status = status,\
                        lstring = lstring, log_etag = log_etag
                    )
                    self.check_state['log_buffer'] += out_string
                else:
                    out_string = '{log_stag}{status}{lstring}{log_etag}\n'.\
                    format(
                        log_stag = log_stag, status = status,\
                        lstring = lstring, log_etag = log_etag
                        )
                    self.check_state['log_buffer'] += out_string

            if node:
                out_string = '{tmp_stag}Node {node}: {status}{lstring}{tmp_etag}\n'.\
                format(
                    tmp_stag = tmp_stag, node = node, status = status,\
                    lstring = lstring, tmp_etag = tmp_etag
                )
            else:
                out_string = '{tmp_stag}{status}{lstring}{tmp_etag}\n'.\
                format(
                    tmp_stag = tmp_stag, status = status,\
                    lstring = lstring, tmp_etag = tmp_etag
                )

            if self.check_state['summaryfile_open']:
                self.check_state['summary_buffer'] += out_string

            if self.check_state['tmpfile_open']:
                self.check_state['tmp_buffer'] += out_string

            if self.check_state['mail_open']:
                self.check_state['mail_buffer'] += out_string

            if self.check_state['summary_open']:
                self.check_state['summary_buffer'] += out_string

        return

    def check_complete(self, name, th_errors, exec_errors, ttime, th_leaks=None):
        tmin = "%.2f min" % ttime
        add_string = ""
        if th_leaks:
            self.output("(%s) check \'%s\' found %s execution errors, %s threshold errors, %s" % (tmin, name, exec_errors, th_errors, th_leaks), -3)
        else:
            self.output("(%s) check \'%s\' found %s execution errors, %s threshold errors" % (tmin, name, exec_errors, th_errors), -3)

    def sysctl_exists(self, sysctl_name):
        exists = 0
        cmd = "%s -d %s" % (self.check_state['sysctl'], sysctl_name)
        result, out = self.run(cmd, None, 1)
        #  if re.search(r'detected', out):
        if not result:
            exists = 1
        return exists

    def open_logfile(self, timestamp):
        log_dir = self.opt_info['log_dir']
        tmp_dir = self.opt_info['tmp_dir']
        log_server_dir = self.check_state['log_server_dir']
        tmp_server_dir = self.check_state['tmp_server_dir']
        make_dirs(log_dir)

        # Create log server dir
        if log_server_dir:
            make_dirs(log_server_dir)

        # Define the current log file
        # my $logfile
        ext = ".html"
        if self.opt_info['user_logfilename']:
            log_file = os.path.join(log_dir, '%s' % self.opt_info['user_logfilename'])
        else:
            log_file = os.path.join(log_dir, 'check_cluster_%s%s' % (timestamp, ext))

        try:
            self.check_state['LOG'] = open(log_file, 'w')
            self.check_state['logfile_open'] = 1
            self.check_state['mail_open'] = 1
            self.output("Check cluster report starting at %s" % timestamp)
            self.check_state['mail_open'] = 0
        except IOError:
            print >> sys.stderr, "Failed to create the log file %s .  Exiting.\n" % log_file
            sys.exit(1)

        make_dirs(tmp_dir)
        if tmp_server_dir:
            make_dirs(tmp_server_dir)

        return log_file

    def open_tmpfile(self, timestamp):
        # Define a tmpfile
        log_dir = self.opt_info['log_dir']
        tmp_file = os.path.join(log_dir, 'check_cluster_%s.tmp' % timestamp)

        try:
            self.check_state['TMP'] = open(tmp_file, 'w')
            self.check_state['tmpfile_open'] = 1
        except IOError:
            print >> sys.stderr, "Failed to create the tmp file %s .  Exiting.\n" % tmp_file
            sys.exit(1)

        return tmp_file

    def open_summaryfile(self, timestamp):
        # Define a summaryfile
        log_dir = self.opt_info['log_dir']
        summary_file = os.path.join(log_dir, 'check_cluster_%s.summary' % timestamp)

        try:
            self.check_state['SUMMARY'] = open(summary_file, 'w')
            self.check_state['summaryfile_open'] = 1
        except IOError:
            print >> sys.stderr, "Failed to create the summary file %s .  Exiting.\n" % summary_file
            sys.exit(1)

        return summary_file

    def is_numeric(self, in_file):
        is_numeric = 0
        if re.search(r'^[0-9]+\.*[0-9]*$', in_file):
            is_numeric = 1
        return is_numeric

    def run_fa(self, cmd = None, in_node = None, ignore = None ):
        # First part of the command
        fa_transport = ""
        self.check_state['fa'] = "/usr/bin/isi_for_array -d \"\" %s" % fa_transport
        cluster_name = self.cluster_info['cluster_name']
        # Execute the command
        excludes = ""
        if self.cluster_info['offline_lnns']:
            excludes = ' -x ' + ','.join(self.cluster_info['offline_lnns'])
        out_dict = {}
        result = None
        if ignore:   # There is acclerator node
            nodes = list()
            storage_nodes_cmd = "sysctl -n efs.gmp.up_storage_nodes"
            result, storage_nodes_string = run_command(storage_nodes_cmd)
            storage_nodes_string = re.sub(r'[{}\s+]', r'', storage_nodes_string)
            storage_nodes = storage_nodes_string.split(',')
            node_infos_cmd = "isi_nodes %{lnn} %{id} %{name} %{ip_address}"
            result, node_infos = run_command(node_infos_cmd)
            node_infos = node_infos.split('\n')
            for node_info in node_infos:
                lnn, id_info, hostname, ip_address = node_info.split(" ")[:4]
                if id_info in storage_nodes:
                    nodes.append(lnn)
        else:
            nodes = copy.copy(self.cluster_info['online_lnns'])
        fa_str = ""
        while nodes:
            if self.opt_info['batch']:
                includes = nodes.pop()
                for i in range(self.opt_info['batch'] - 1):
                    if nodes:
                        includes=includes + "," + nodes.pop()
                fa_str = "%s -n %s \"%s\"" % (self.check_state['fa'], includes, cmd)
            else:
                fa_str = "%s -n %s \"%s\"" % (self.check_state['fa'], excludes, cmd)
                nodes = list()

            if self.opt_info['debug']:
                self.output("Running \"%s\"" % fa_str)
            (result, out, err) = run_command_full_output(
                                    'nice -n 20 %s' % fa_str)
            #  nice -n 20 /usr/bin/isi_for_array -d  "" "nfsstat"
            if result and ignore:
                if out == "":
                    if self.opt_info['debug']:
                        print "Ignoring empty output\n"
                else:
                    self.output("error running \"%s\" =>  %s  <=" % (cmd, out), 2)
            if err != "":
                self.output('==============================================='
                            '=========')
                self.output('Logging stderr output: BEGIN')
                self.output('{}'.format(err))
                self.output('Logging stderr output: END')
                self.output('==============================================='
                            '=========')

            # Parse results into a data structure if $fa
            for line in out.splitlines(True):
                line = line.rstrip("\r\n")
                # next if $line =~ m/^\s*$/  if match then next skip this line
                if re.search(r'^\s*$', line):
                    continue
                match = re.search(r'^%s-([0-9]+):\s*(.*)$' % cluster_name, line)
                if match:
                    node_id = match.group(1)
                    data = match.group(2)
                    # Re scan the rest of the line, looking for repeats of the
                    # node name.  <something>@<cluster_name>-<number>, is a
                    # build location and is not a repeat of the node name.
                    # Bug 246835
                    re_scan = r'^(.*[^@])?%s-([0-9]+):\s*(.*)$' % cluster_name
                    match = re.search(re_scan, data)
                    if match and not ('isi_rdo' in data):
                        self.output("FOUND DOUBLED LINE\n", 2)
                        prev_value = match.group(1)
                        doublenode_id = match.group(2)
                        doubledata = match.group(3)

                        if self.opt_info['debug']:
                            self.output("FOUND DOUBLED LINE\n", 1)
                            print "line: ->%s<-\n" % line
                            print "node_id: ->%s<-\n" % node_id
                            print "prev_value: ->%s<-\n" % prev_value
                            print "doublenode_id: ->%s<-\n" % doublenode_id
                            print "doubledata: ->%s<-\n" % doubledata

                        match = re.search(re_scan, doubledata)
                        if match:
                            self.output("FOUND TRIPLE LINE\n", 2)
                            prev_value = match.group(1)
                            triplenode_id = match.group(2)
                            tripledata = match.group(3)

                            if self.opt_info['debug']:
                                self.output("FOUND TRIPLE LINE!\n", 2)
                                print "line: ->%s<-\n" % line
                                print "node_id: ->%s<-\n" % node_id
                                print "prev_value: ->%s<-\n" % prev_value
                                print "triplenode_id: ->%s<-\n" % triplenode_id
                                print "tripledata: ->%s<-\n" % tripledata
                                if node_id not in out_dict:
                                    out_dict[node_id] = ""
                                out_dict[node_id] += "%s\n" % prev_value

                                if triplenode_id not in out_dict:
                                    out_dict[triplenode_id] = ""
                                out_dict[triplenode_id] += "%s\n" % tripledata
                        else:
                            if node_id not in out_dict:
                                out_dict[node_id] = ""
                            out_dict[node_id] += "%s\n" % prev_value
                            if doublenode_id not in out_dict:
                                out_dict[doublenode_id] = ""
                            out_dict[doublenode_id] += "%s\n" % doubledata
                    else:
                        if node_id not in out_dict:
                            out_dict[node_id] = ""
                        out_dict[node_id] += "%s\n" % data
                else:
                    self.output("UNMATCHED LINE: ->%s<-\n" % line, 2)
        return result, out_dict

    def run(self, cmd = None, node = None, ignore = None):
        if self.opt_info['debug']:
            self.output("Running %s" % cmd)

        (result, out) = run_command('%s' % cmd)
        if (result != 0 and not ignore):
            self.output("Caller: error running %s:" % cmd + ''.join(out), 2, node)

        return result, out

    def update_tracking_file(self, fh, lnn, pid, test_label, value, unit):
        delim = ","
        string = "%s%s%s%s%s%s%s%s%s%s%s \n" % (self.check_state['timestamp'], delim, pid, delim, lnn, delim, test_label, delim, value, delim, unit)
        fh.write(string)
        if self.opt_info['debug']:
            print string
        return

    def cluster_summary(self, nodes):
        my_nodeid = self.cluster_info['my_nodeid']
        self.output("Cluster name        :  %s" % self.cluster_info['cluster_name'])
        self.output("Coordinator node    :  %s (%s)" % (nodes[my_nodeid]['hostname'], nodes[my_nodeid]['ip_address']))
        num_nodes = nodes.keys()
        num_nodes.sort(key=int)
        self.output("Number of Nodes     :  %s" % num_nodes)
        self.output("Nodes Online        :  %s" % self.cluster_info['online_lnns'])
        self.output("Software version    :  %s" % self.cluster_info['software_version'])
        self.output("Software build      :  %s" % self.cluster_info['cluster_build'])
        self.output("Hardware platform   :%s" % self.cluster_info['hardware_version'])

    def regress_data(self, x, y):  # Put into sub class
        sumx = 0.0
        sumx2 = 0.0
        sumxy = 0.0
        sumy = 0.0
        sumy2 = 0.0
        n = len(x)
        for i in range(len(x)):
            # data should be numeric
            if not self.is_numeric(x[i]) or not self.is_numeric(y[i]):
                #print "skipping non-numeric data: x: x, y: y\n"
                continue
            # compute sum of x
            sumx += float(x[i])
            # compute the sum of x**2
            sumx2 += float(x[i]) * float(x[i])
            # compute the sum of x * y
            sumxy += float(x[i]) * float(y[i])
            # compute the sum of y
            sumy += float(y[i])
            # compute sum of y**2
            sumy2 += float(y[i]) * float(y[i])

        if self.opt_info['debug']:
            print "n: %s" % n
            print "sumx2: %s" % sumx2
            print "sumx: %s" % sumx
            print "sumy2: %s" % sumy2
            print "sumy: %s" % sumy
            print "bottom: %s " % (n * sumx2 - sqr(sumx))
        # compute slope
        slope = 0
        y_int = 0
        cc = 0
        sumx = float(str(sumx))
        sumx2 = float(str(sumx2))
        sumxy = float(str(sumxy))
        sumy = float(str(sumy))
        sumy2 = float(str(sumy2))
        if (n * sumx2 - sqr(sumx)) and (sumy2 - sqr(sumy)/n):
            m = ((n * sumxy) - sumx * sumy) / ((n * sumx2) - sqr(sumx))
            # compute y-intercept
            b = (sumy * sumx2 - sumx * sumxy) / ((n * sumx2) - sqr(sumx))
            # compute correlation coefficient
            r = 0
            sqrt = (sumx2 - sqr(sumx)/n) * (sumy2 - sqr(sumy)/n)
            if sqrt <= 0:
                return (0, 0, 0)
            denom = math.sqrt(float(sqrt))

            if denom:
                r = (sumxy - sumx*sumy/n) / denom
            slope = "%15.6e" % m
            y_int = "%15.6e" % b
            cc = "%f" % r

        slope_display = "%.7f" % float(slope)
        y_int_display = "%.7f" % float(y_int)
        cc_display = "%.7f" % float(cc)

        return (slope_display, y_int_display, cc_display)

    def gplot_trend(self, arry):
        (datafile, lnn, cmd, slope, y_int, cc, samples, timerange_sec, unit, delta, rate) = arry
        exec_errors = 0
        out = datafile
        out = out.rstrip("\r\n")
        out = re.sub(r'.csv', r'.png', out)
        timerange = "%.2f days" % (float(timerange_sec) / float((60*60*24)))
        # Is gnuplot available?
        ret, gnuplot = run_command('which gnuplot')
        if ret:
            self.output("Didn't find gnuplot installed. No graphs for you.")
        else:
            gp = os.popen('%s' % gnuplot, 'w')
            try:
                command = '''set terminal png
set output "{out}"
set xlabel "Time"
set ylabel "Memory Used {unit}"
set xdata time
set timefmt "%Y_%m_%d_%H:%M:%S"
set border
unset grid
unset key
set label 1 "LNN {lnn}: {cmd}" at graph .05, graph .9
set label 2 "Slope: {slope}" at graph .05, graph .875
set label 3 "Corr. Coef: {cc}" at graph .05, graph .85
set label 4 "{samples} samples over {timerange}" at graph .05, graph .825
set label 5 "Delta: {delta}" at graph .05, graph .8
set label 6 "Rate: {rate}" at graph .05, graph .775
plot "{datafile}" using 1:2 with lines
'''.format(out=out, unit = unit, lnn = lnn, cmd = cmd, slope = slope, cc = cc, samples = samples, timerange = timerange, delta = delta, rate = rate, datafile = datafile)
                gp.write(command)
                # gp.flush()
                gp.close()

            except OSError:
                self.output("Error running gnuplot on %s" % datafile, -2, None, caller = 'check_for_leaks')
                exec_errors += 1

        return exec_errors

########################################
# Script Subroutines


def make_dirs(dirs):
    if not os.path.isdir(dirs):
        try:
            os.makedirs(dirs)
        except OSError, (err, errstr):
            if err != errno.EEXIST:
                print >> sys.stderr, "Error: %s" % errstr
                sys.exit(2)

def sqr(a):
    return a*a

def run_command_full_output(cmd, timeout_sec=3600):
    stdout = ""
    p = subprocess.Popen(cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            bufsize = -1, shell = True )
    timer = Timer(timeout_sec, p.kill)
    try:
        timer.start()
        stdout, stderr = p.communicate()
        result = p.returncode
        stdout = stdout.rstrip("\r\n")
    finally:
        timer.cancel()

    return result, stdout, stderr

def run_command(cmd, timeout_sec=3600):
    result, stdout, _ = run_command_full_output(cmd, timeout_sec)
    return result, stdout


def create_data_tracking_file(data_type, log_dir):
    # Create the  directory if it doesn't exist
    make_dirs(log_dir)
    # Define the datafile, and back it up if it already exists
    data_file = os.path.join(log_dir, '%s_check_cluster_tracking.csv' % data_type)
    if os.path.isfile(data_file):
        shutil.copy(data_file, data_file + '.bak')
    else:
        try:
            DATA = open(data_file, 'a')
            DATA.close()
        except IOError:
            print "Failed to create the log file %s.  Exiting.\n" % data_file
            sys.exit(1)

    return data_file


def show_checks(checks):
    print "The following individual checks are available:\n"
    check_keys = checks.keys()
    check_keys.sort()
    for check in check_keys:
        print "\t%s" % check
    return


def define_checks(options, checks):  # Running all the check sub routines
    # If the --show option was called, print out a list of checks and sets
    checks_keys = checks.keys()
    checks_keys.sort()
    if options['show']:
        show_checks(checks)
        sys.exit(0)
    ### parse the --check option
    int_checks_to_run = []
    hourly_checks_to_run = []
    if options['checks']:
        names = [x.strip() for x in options['checks'].split(',')]
        for name in names:
            user_check_name = 'check_' + name
            if user_check_name in checks.keys():
                int_checks_to_run.append(user_check_name)
                hourly_checks_to_run.append(user_check_name)
            else:
                show_checks(checks)
                print "Check %s does not exist. Exiting. \n" % user_check_name
                sys.exit(1)

    ### --set option
    elif options['user_set']:
        if options['user_set'] in sets.keys():
            for check_name in sets[options['user_set']]:
                if check_name in checks.keys():
                    int_checks_to_run.append(check_name)
                else:
                    show_checks(checks)
                    print "Check %s does not exist. Exiting. \n" % check_name
                    sys.exit(1)
        else:
            show_checks(checks)
            print "Set %s does not exist. Exiting. \n" % options['user_set']
            sys.exit(1)
    # If the list of checks is still empty, select all of them
    else:
        for check_name in checks_keys:
            int_checks_to_run.append(check_name)
            hourly_checks_to_run.append(check_name)

    ### --hourly option
    '''
    hourly_checks_to_run = []
    if options['hourly_set']:
        if options['hourly_set'] == 'all': # sets.keys():
    for check_name in checks_keys:
        if check_name in checks.keys():
            hourly_checks_to_run.append(check_name)
        else:
            show_checks(checks)
            print "Check %s does not exist. Exiting. \n" % check_name
            sys.exit(1)
    '''
    if (options['debug']):
        print "\nThe following interval checks will be run:\n"
        for check in int_checks_to_run:
            print "check: %s\n" % check
        print "\nThe following hourly checks will be run:\n"
        for check in hourly_checks_to_run:
            print "check: %s\n" % check

    return hourly_checks_to_run, int_checks_to_run


# Report general info about the cluster
def get_cluster_info(sysctl):
    nodes = {}
    # Report the cluster name
    node_name = run_command('%s -n kern.hostname' % sysctl)[1]
    match = re.search(r'^(.*)-([0-9]+)$', node_name)
    cluster_name = match.group(1)
    cluster_name = cluster_name.rstrip("\r\n")
    my_nodelnn = match.group(2)
    # Report the version of software
    uname = run_command('uname -v')[1]
    type_info, fs, software_version, build, other = uname.split(' ', 4)
    build = re.sub(r':', r'', build)

    # Report the version of hardware
    cmd = "/usr/bin/isi_hwtools/isi_hw_status | grep HWGen"
    hardware_version = run_command(cmd)[1]
    if hardware_version == "":
        hardware_version = ": unsure (Cheddar or Mozz)"
    cluster_info = {}
    cluster_info.update({'cluster_name': cluster_name})
    cluster_info.update({'software_version': software_version})
    cluster_info.update({'cluster_build': build})
    cluster_info.update({'hardware_version': hardware_version})
    cluster_info.update({'my_nodelnn': my_nodelnn})

    return cluster_info

# Set the env from the node or nodes in cluster.
# Determine which nodes are online and mounted
# Create a global data structure to record events for each node


def get_node_info(debug):
    nodes = {}
    # Get the list of nodes in the cluster
    node_infos_cmd = "isi_nodes %{lnn} %{id} %{name} %{ip_address}"
    result, node_infos = run_command(node_infos_cmd)
    if result:
        print("Couldn't get a list of nodes using %s. Exiting." % node_infos_cmd)
        sys.exit(1)

    # Create a hash of hashes containing node info
    # with a hash key of node id

    # Get the list of online nodes
    up_nodes_cmd = "sysctl -n efs.gmp.up_nodes"
    result, up_nodes_string = run_command(up_nodes_cmd)
    up_nodes_string = re.sub(r'[{}\s+]', r'', up_nodes_string)
    up_nodes = up_nodes_string.split(',')

    node_infos = node_infos.split('\n')
    for node_info in node_infos:
        lnn, id_info, hostname, ip_address = node_info.split(" ")[:4]
        ip_address = ip_address.rstrip("\r\n")    # chomp()
        node = dict()
        node['lnn'] = lnn
        node['id'] = id_info
        node['hostname'] = hostname
        node['ip_address'] = ip_address
        if id_info in up_nodes:
            node['online'] = 1
        else:
            node['online'] = 0
        node['ifs_unmounted'] = 0
        node['nfs_mounted'] = 0

        # Add the hash to the parent hash
        nodes[lnn] = node

    nodes_keys = nodes.keys()
    nodes_keys.sort(key=int)
    # Check the offline nodes to see if they are just up but unmounted
    for node_id in nodes_keys:
        if nodes[node_id] is not 'online':
            status_cmd = ("isi status -n %s" % node_id)
            error, out = run_command(status_cmd)
            if not error:
                match = re.search('Node\s+Health:\s*(?P<health>[a-zA-Z\-]*)', out, re.M)
                if not match:
                    print('Unable to parse Node Health from isi status')
                else:
                    stat_re = re.compile('.*D.*')
                    health = match.group('health')
                    if stat_re.match(health):
                        print("isi status reported down node: %s" % health)
                        print("Found /ifs unmounted on node %s" % node)
                        nodes[node_id]['ifs_unmounted'] = 1
            else:
                print("Failed to query node id %s: %s" % (node_id, out))
        else:
            nodes[node]['ifs_unmounted'] = 0
    if debug:
        for node_id in nodes_keys:
            print "id: %s\n" % node_id
            print "\tlnn: %s\n" % nodes[node_id]['lnn']
            print "\thostname: %s\n" % nodes[node_id]['hostname']
            print "\tip_address: %s\n" % nodes[node_id]['ip_address']
            print "\tonline: %s\n" % nodes[node_id]['online']
            print "\tifs_unmounted: %s\n" % nodes[node_id]['ifs_unmounted']
            print "\tnfs_mounted: %s\n" % nodes[node_id]['nfs_mounted']

        # Print out the node mapping
        for node_id in nodes_keys:
            map_string = "LNN:    {0}   ID:    {1}   EXT_IP: {2}   ONLINE:   {3}   IFS_UNMOUNTED:   {4}\n". \
            format(
                nodes[node_id]['lnn'], node_id, \
                nodes[node_id]['ip_address'], nodes[node_id]['online'], \
                nodes[node_id]['ifs_unmounted']
            )
            print map_string
    return nodes


def timestamp(esec = None):
    if esec is None:
        time_in_seconds = time.time()
    else:
        time_in_seconds = esec
    year, mon, mday, hour, mininute, sec, wday, yday, isdst = time.localtime(time_in_seconds)
    date_time = "%02d_%02d_%02d_%02d:%02d:%02d" % (year, mon, mday, hour, mininute, sec)
    # date_time = time.strftime('%x_%X')
    date = "%02d_%02d_%02d" % (year, mon, mday)
    return date_time, time_in_seconds


def manage_pid_file(mode):
    current_pid = os.getpid()
    pidfile = "/var/run/check_cluster.pid"
    if mode == "check":
        if os.path.isfile(pidfile):
            cmd = "cat %s" % pidfile
            result, pid = run_command(cmd)
            pid = pid.rstrip("\r\n")
            cmd = "ps -j %s" % pid
            pid_not_found, out = run_command(cmd)
            if pid_not_found:
                print "\nDeleting invalid pid file.\n"
                os.unlink(pidfile)
            else:
                print "\n%s\nFound existing process. Exiting.\n" % out
                sys.exit(0)

    elif mode == "start":
        if os.path.isfile(pidfile):
            print "\nFound existing pid file %s. Exiting.\n" % pidfile
            print "\nFound existing pid file %s. Exiting.\n" % pidfile
            sys.exit(0)

        else:
            cmd = "echo \"%s\" > %s" % (current_pid, pidfile)
            result, out = run_command(cmd)
            if result:
                print "\nERROR: Failed to create pid file %s. Exiting.\n" % pidfile
                sys.exit(1)

    elif mode == "end":
        if os.path.isfile(pidfile):
            cmd = "rm -f " + pidfile
            result, out = run_command(cmd)
            if result:
                print "\nERROR: Failed to remove pid file %s please remove manually.\n" % pidfile
        else:
            print "\nERROR: pid file %s missing!\n" % pidfile

    return


# check_alerts => CheckAlerts, due to python Style
# check_isi_alerts => CheckIsiAlerts
def convert_to_class(name):
    check_list = name.split('_')
    class_name = 'Check'
    length = len(check_list)
    for i in range(1, length):
        sub = check_list[i]
        upper = sub[0].upper()
        sub = sub.replace(sub[0], upper, 1)
        class_name += sub
    return class_name


# CheckAlerts => alerts
# CheckIsiAlerts => isi_alerts
def get_func_name(name):
    if name.startswith('Check'):
        func_name = name[5].lower()
        for i in range(6, len(name)):  # Ignore Check
            if name[i].isupper():
                func_name += '_' + name[i].lower()
            else:
                func_name += name[i]
        return func_name
    else:
        print "Wrong in check Class name"
        sys.exit(2)


def printVersion(exit_on_completion = False):
    program = "check_cluster.py"
    version = "%s: $Revision$ $Date$" % program
    if exit_on_completion:
        print "%s\n" % version
        sys.exit(0)
    return version


def get_class_files(): 	# return like {check_isi_stat:isi_stat}
    "Find the check modulesi and return the check names"
    checks = {}
    package = checkcluster
    pathlist = package.__path__
    #checks are in the checks directory of the package
    path = '%s/checks' % pathlist[0]
    pathlist[0] = path

    for importer,modname,ispkg in pkgutil.iter_modules(pathlist):
	checks.update({modname: modname[6:]})

    return checks


def final_summarize(files, cluster, timestamp_in_seconds, nodes, check_timestamp = None):
    cluster.check_state['mail_open'] = 0
    cluster.check_state['summary_open'] = 1
    # Print final banner
    out_string = 'REPORT SUMMARY:\nResults saved to {logfile}.\n\
Memory status saved to {memfile}.\nFilehandle status saved to {fstatfile}'.\
        format(
            logfile = files['logfile'], memfile = files['memfile'],\
            fstatfile = files['fstatfile']
              )
    cluster.output(out_string)
    # Elapsed Time
    elapsed = "%.2f" % ((time.time() - timestamp_in_seconds)/60)
    cluster.output("Elapsed Time: %s Min" % elapsed)
    version = printVersion()
    cluster.output("%s" % version)
    cluster.check_state['summary_open'] = 0
    cluster.check_state['mail_open'] = 1


def summarize(errors, files, summaryfile, detailed_log, cluster, timestamp_in_seconds, nodes, check_timestamp = None):
    levels = ["FAIL", "EXEC", "CRNT_LEAK", "PAST_LEAK"]
    if detailed_log.endswith('.tmp'):
        file_to_cat = cluster.check_state['tmp_buffer']
    else:
        file_to_cat = ""
        # file_to_cat = cluster.check_state['log_buffer']

    # Different out put in monitor side
    monitor = cluster.opt_info['monitor']
    send_to = cluster.opt_info['send_to']
    pre_check_timestamp = cluster.check_state['pre_check_timestamp']
    cycle_number = cluster.check_state['cycle_number']
    cluster_events = cluster.cluster_info['cluster_events']

    if cluster.check_state['log_server_dir']:    # We have saved log into logserver
        base_dir = cluster.check_state['log_server_dir'].split('/', 5)[-1]
        base_url = "http://durbiox.west.isilon.com/qa/log/" + base_dir
        current_url = base_url + "/%s" % files['logfile'].split('/')[-1]
    else:
        current_url = cluster.opt_info['log_dir'] + "/%s" % files['logfile'].split('/')[-1]

    cluster.check_state['mail_open'] = 1
    # Print final banner
    cluster.output("REPORT SUMMARY:")
    cluster.output("Results saved to %s." % files['logfile'])
    cluster.output("Memory status saved to %s." % files['memfile'])
    cluster.output("Filehandle status saved to %s." % files['fstatfile'])

    # Elapsed Time
    elapsed = "%.2f" % ((time.time() - timestamp_in_seconds)/60)
    cluster.output("Elapsed Time: %s Min" % elapsed)
    version = printVersion()
    cluster.output("%s" % version)

    # If in monitor mode, display the links to the previous logs in the logserver

    if cluster.opt_info['send_to'] and current_url:
        cluster.output("Current link here:", 3)
        current_url = '''<a href= %s>%s</a>''' % (current_url, current_url)
        cluster.output(current_url, 3)

    # Report the total warnings and errors
    multi = "s"
    fail_label = (errors['th_errors'] <= 1) and "error" or "errors"
    leak_label = (errors['th_leaks'] <= 1) and "leak" or "leaks"
    exec_label = (errors['exec_errors'] <= 1) and "execution error" or "execution errors"
    cluster.cluster_summary(nodes)
    cluster.output("Totals:")
    cluster.output("FAIL: %s %s" % (errors['th_errors'], fail_label))
    cluster.output("LEAKS: %s %s" % (errors['th_leaks'], leak_label))
    cluster.output("EXEC: %s %s" % (errors['exec_errors'], exec_label))

    cluster_events_keys = cluster_events.keys()
    cluster_events_keys.sort(key = int)
    # Print the cluster events sorted by node
    if cluster.opt_info['sort'] == "n":
        for node in cluster_events_keys:
            cluster.output("<h5>Node %s events:</h5><pre>" % node)
            node_events = cluster_events[node]
            for level in node_events.keys():
                printlevel = 1
                if level == "CRNT_LEAK":
                    printlevel = -4
                elif level == "PAST_LEAK":
                    printlevel = -5
                elif level == "EXEC":
                    printlevel = 2
                lines = node_events[level].split('\n')
                for line in lines:
                    if line.startswith('BUG'):
                        bug_id, rem = line.split(':', 1)
                        cluster.output("%s : Node %s:  %s" % (bug_id, node, rem),  printlevel)
                    else:
                        cluster.output("Node %s: %s" % (node, line), printlevel)

    # Print the cluster events sorted by total
    elif cluster.opt_info['sort'] == "t":
        for level in levels:
            cluster.output("%s:" % level)
            printlevel = 1  # Fail:
            if level == "CRNT_LEAK":
                printlevel = -4
            elif level == "PAST_LEAK":
                printlevel = -5
            elif level == "EXEC":
                printlevel = 2
            for node in cluster_events_keys:
                node_events = cluster_events[node]
                if level in node_events.keys():
                    cluster.output("Node %s:" % node)
                    lines = node_events[level].splitlines(True)
                    for line in lines:
                        line = line.rstrip("\r\n")
                        if len(line) < 3:    # Incase of '\n' and ' '
                            break
                        if line.startswith('BUG'):
                            (bug_id, rem) = line.split(':', 1)
                            cluster.output("%s : Node %s: %s" % (bug_id, node, rem), printlevel)
                        else:
                            cluster.output(line, printlevel)
                    if '^LEAK'in level:
                        cluster.output("")

    # Append the log or tmp file to the end of the summary file
    if os.path.isfile(detailed_log):
        # cmd = "cat %s >> %s" % (detailed_log, summaryfile)
        # Finish summary and log file writing and re open it.
        # result, out = run(cmd)
        cluster.check_state['summary_buffer'] += file_to_cat
        cluster.check_state['log_buffer'] += file_to_cat
        file_to_cat = ""
        result = 0
        if result:
            cluster.output("Failed to append detailed log to summary", 2)
            errors['exec_errors'] += 1
            cluster.check_state['file_to_mail'] = detailed_log
        else:
            cluster.check_state['file_to_mail'] = summaryfile

    return cluster.check_state['file_to_mail']


MAINTAINER = 'list-eng-automation@isilon.com'
def mail_to(cluster, errors):

    file_to_mail = cluster.check_state['file_to_mail']
    send_to = cluster.opt_info['send_to']
    send_from = cluster.opt_info['send_from']
    addresses = send_to.split(',')
    # If $send_to has more than one address, use the first for From:
    if not send_from:
        send_from = addresses[0]
    fail_label = (errors['th_errors'] <= 1) and "error" or "errors"
    leak_label = (errors['th_leaks'] <= 1) and "leak" or "leaks"
    exec_label = (errors['exec_errors'] <= 1) and "execution error" or "execution errors"

    subject = "C_C: %s %s, %s %s, %s %s (cluster %s)" % (errors['th_errors'], fail_label,
                                                         errors['th_leaks'], leak_label,
                                                         errors['exec_errors'], exec_label,
                                                         cluster.cluster_info['cluster_name'],
                                                         )

    # Check for file definition and existence to troubleshoot bug 57686
    filesize_limit = 100 * 1024
    if file_to_mail:
        try:
            filesize = os.stat(file_to_mail).st_size
            if int(filesize) > int(filesize_limit):
                file_location = file_to_mail.replace('summary', 'html')
                message = "This check_cluster report exceeded the " \
                          "recommended limit to be sent via email (%d " \
                          "kB). The report can be found on your cluster " \
                          "here: %s\n" \
                          % (filesize_limit / 1024, file_location, )
            else:
                message = cluster.check_state['mail_buffer']
        except OSError as ose:
            if ose.errno == errno.ENOENT:
                message = "ERROR: Requested summary file (%s) does not " \
                          "exist! Please report this to %s\n" \
                          % (file_to_mail, MAINTAINER, )
            else:
                message = "ERROR: an unexpected error occurred when trying to " \
                          "access the file (%s): %s\n" % (file_to_mail, str(ose))
    else:
        message = "ERROR: `file_to_mail` is undefined! Please report this to " \
                  "%s\n" \
                  % (MAINTAINER, )
    try:
        sendmail.do_email(addresses, message, subject, sender=send_from, subtype='html')
        cluster.output("Email successfully sent to %s from %s." % (send_to, send_from, ))
    except Exception as exc:
        cluster.output("Warning! Unable to email results to %s: %s."
                       % (send_to, traceback.format_exc(), ))

###########################################################################################
# Intialization


def usage(conffile=None):
    print __doc__
    if conffile:
        print "Reading %s\n" % conffile
        cfh = open(conffile, 'r')
        print cfh.read()
    sys.exit(2)


def get_option_info():
    opt_info = {}
    parser = OptionParser(usage=__doc__)
    parser.add_option('--debug', action="store_true", help = 'Run in debug mode')
    parser.add_option('--warn', action="store_true", help = 'Print out warn message.')
    parser.add_option('--l', help = 'User log file name')
    parser.add_option('--rld', help = 'Remote log directory name')
    parser.add_option('--verbose', action="store_true", help = 'Control Verbose Mode')
    parser.add_option('--add_filters', help = 'Add filters')
    parser.add_option('--all_messages', action="store_true", help = 'Show all messages')
    parser.add_option('--monitor', action="store_true", help = 'Run in Monitor mode')
    parser.add_option('--mail', help = 'Send report by email')
    parser.add_option('--debug_mail', help = 'Send debug report by email')
    parser.add_option('--from', help = 'Sending email from ...')
    parser.add_option('--nocolor', action="store_true", help = 'Run in nocolor mode')
    parser.add_option('--checks', help = 'Check')
    parser.add_option('--start_date', help = 'Start date')
    parser.add_option('--end_date', help = 'End date')
    parser.add_option('--show', action="store_true", help = 'Show')
    parser.add_option('--single', action="store_true", help = 'Run in single mode')
    parser.add_option('--version', action="store_true", help = 'Print the version')
    parser.add_option('--hourly_set', action="store_true", help = 'Hourly set')
    parser.add_option('--set', help = 'Set env')

    parser.add_option('--lld',
    action = "store", dest="lld", default="/ifs/data/Isilon_Support/cc_logs/",
    help = 'Log directory default set to be %default')
    parser.add_option('--leak_days',
    action = "store", dest="leak_days", default = 7,
    help = 'Defautly set to %default days')

    parser.add_option('--interval',
    action = "store", dest = "interval", default = 15,
    help = 'Defaults to %default minutes to run')

    parser.add_option('--min_samples',
    action = "store", dest = "min_samples", default = 20,
    help = 'Minumum samples defaults to be %default')

    parser.add_option('--max_samples',
    action = "store", dest = "max_samples", default = 288,
    help = 'Maximum samples defaults to be %default')

    parser.add_option('--slope',
    action = "store", dest = "slope", default = .000001,
    help = 'Defaults to %default')

    parser.add_option('--vmlogs',
    action = "store", dest = "vmlogs", default = 5,
    help = 'Defaults to %default')

    parser.add_option('--cc',
    action = "store", dest = "cc", default = .95,
    help = 'Defaults to %default')

    parser.add_option('--rf',
    action = "store", dest = "rf", default = 'html',
    help = 'Defaults to %default')

    parser.add_option('--lf',
    action = "store", dest = "lf", default = 'html',
    help = 'Defaults to %default')

    parser.add_option('--sort',
    action = "store", dest = "sort", default = 't',
    help = 'Defaults to %default')

    parser.add_option('--batch',
    action = "store", dest = "batch", default = 24,
    help = 'Defaults to %default')

    (options, args_list) = parser.parse_args()
    args = vars(options)

    # Default Values
    opt_info['debug'] = args['debug']
    opt_info['show'] = args['show']
    opt_info['hourly_set'] = args['hourly_set']
    opt_info['nocolor'] = args['nocolor']
    opt_info['log_dir'] = args['lld']
    opt_info['remote_log_dir'] = args['rld']
    opt_info['user_logfilename'] = args['l']
    opt_info['verbose'] = args['verbose']
    opt_info['warn'] = args['warn']
    opt_info['monitor'] = args['monitor']
    opt_info['added_filters'] = args['add_filters']
    opt_info['all_messages'] = args['all_messages']
    opt_info['leak_days'] = args['leak_days']
    opt_info['interval'] = args['interval']
    opt_info['min_samples'] = args['min_samples']
    opt_info['max_samples'] = args['max_samples']
    opt_info['min_slope'] = args['slope']
    opt_info['nice_min_slope'] = "%.07f" % opt_info['min_slope']
    opt_info['min_vmlogs'] = args['vmlogs']
    opt_info['min_cc'] = args['cc']
    opt_info['send_to'] = args['mail']
    opt_info['debug_mail'] = args['debug_mail']
    opt_info['send_from'] = args['from']
    opt_info['rformat'] = args['rf']
    opt_info['lformat'] = args['lf']
    opt_info['sort'] = args['sort']
    opt_info['user_checks'] = args['checks']
    opt_info['start_date'] = args['start_date']
    opt_info['end_date'] = args['end_date']
    opt_info['single_node'] = args['single']
    opt_info['batch'] = args['batch']
    opt_info['checks'] = args['checks']

    if args['version']:
        printVersion(1)
    opt_info['user_set'] = args['set']

    return opt_info

###########################################################################################
# Main


def main():
    # Check state default values
    check_state = {'sysctl': "/sbin/sysctl",
                   'fa': "isi_for_array -d \"\" -Q ",
                   'pidfile': "/var/run/check_cluster.pid",
                   'total_bugs': 0,
                   'tmpfile_open': 0,
                   'summaryfile_open': 0,
                   'mail_open': 0,
                   'logfile_open': 0,
                   'file_to_mail': None,
                   'LOG': None,
                   'TMP': None,
                   'SUMMARY': None,
                   'DATA': None,
                   'log_buffer': "",
                   'tmp_buffer': "",
                   'summary_buffer': "",
                   'mail_buffer': "",
                   'check_timestamp': None,
                   'timestamp_in_seconds': None,
                   'file_info': None,
                   'hourly': None,    # For houly check in monitor mode
                   'cycle_number': 1,
                   'pre_check_timestamp': None,
                   'pre_cluster_events': None,
                   'log_server_dir': None,
                   'tmp_server_dir': None,
                   'summary_open': 0,
                   'summary_buffer': ""
                  }

    # Option Information
    opt_info = get_option_info()


    # Give a general summary of this cluster
    cluster_info = get_cluster_info(check_state['sysctl'])
    cluster_info.update({'cluster_events': dict()})
    cluster_info.update({'cluster_bugs': dict()})

    # Instantiate cluster obj.
    cluster_instance = Cluster(check_state, cluster_info, opt_info)
    # Stastic information
    error_info = dict()
    file_info = dict()
    cluster_instance.check_state['file_info'] = dict()

    error_info.update({'exec_errors': 0})         # Command execution errors
    error_info.update({'th_errors': 0})       # Threshold failures
    # error_info.update({'th_warns': 0})       # Threshold warnings
    error_info.update({'th_leaks': 0})       # leaks

    # Get dict of check classes
    checks = get_class_files()  # checks = get_class_files

    # Load modules
    for check in checks.keys():
        exec "from checkcluster import %s" % check

    # Define the list of available checks
    hourly_checks_to_run, int_checks_to_run = define_checks(opt_info, checks)

    # Check for an existing pid file
    # If it exits, exit
    manage_pid_file("check")

    # Record the script time
    start_time = time.time()

    # Get the cluster time
    cluster_date = run_command('date')[1]
    day, month, month_day, other = cluster_date.split(' ', 3)
    cluster_monthe_day = month + " " + month_day

    # Create the log file
    # First check if node mount to the logserver
    dir_name = opt_info['log_dir']
    if opt_info['remote_log_dir']:
        if os.path.isdir('/usr/local/qa/log'):    # We have already mounted to log server
            cluster_instance.check_state['log_server_dir'] = "/usr/local/qa/log/%s" % opt_info['remote_log_dir']
            cluster_instance.check_state['tmp_server_dir'] = cluster_instance.check_state['log_server_dir'] + "/tmp"
        else:
        # we are not able to set log server
            cluster_instance.output("Unable to set remote local server")
            sys.exit(2)

    if dir_name == "/ifs/data/Isilon_Support/cc_logs/":
        cluster_instance.opt_info['log_dir'] += "%s" % cluster_info['software_version']

    tmp_dir = cluster_instance.opt_info['log_dir'] + "/tmp"
    cluster_instance.opt_info.update({'tmp_dir': tmp_dir})
    file_time, timestamp_in_seconds = timestamp()
    cluster_instance.check_state['timestamp'] = timestamp_in_seconds
    file_info['logfile'] = cluster_instance.open_logfile(file_time)
    # cluster_instance.check_state['file_info']['logfile'] = file_info['logfile']
    # Open/create the data file

    # Adding ouput log
    logging.basicConfig(
      level=logging.DEBUG,
      format='%(asctime)s:%(levelname)s:%(name)s:%(message)s',
      filename='%s/out.log' % opt_info['log_dir'],
      filemode='a'
    )
    # Open/create the memory tracking file
    file_info['memfile'] = create_data_tracking_file("mem_python", cluster_instance.opt_info['log_dir'])
    file_info['fstatfile'] = create_data_tracking_file("fstat_python", cluster_instance.opt_info['log_dir'])
    cluster_instance.check_state['file_info']['memfile'] = file_info['memfile']
    cluster_instance.check_state['file_info']['fstatfile'] = file_info['fstatfile']

    # Determine which nodes are online and mounted
    # Create a global data structure to record events for each node
    cluster_instance.cluster_info.update({'online_lnns': list()})
    cluster_instance.cluster_info.update({'offline_lnns': list()})
    cluster_instance.cluster_info.update({'unmounted_lnns': list()})
    # Get node information
    nodes = dict()
    nodes = get_node_info(opt_info['debug'])
    nodes_keys = nodes.keys()
    nodes_keys.sort(key=int)
    for node_id in nodes_keys:
        #Add the hash to the parent hash
        node_events = {}
        node_bugs = {}
        cluster_instance.cluster_info['cluster_events'][node_id] = node_events
        cluster_instance.cluster_info['cluster_bugs'][node_id] = node_bugs
        if nodes[node_id]['lnn'] == cluster_instance.cluster_info['my_nodelnn']:
            cluster_instance.cluster_info.update({'my_nodeid': node_id})
            cluster_instance.cluster_info.update({'my_ip': nodes[node_id]['ip_address']})

    for node_id in nodes_keys:
        if cluster_instance.opt_info['single_node'] and (node_id != cluster_instance.cluster_info['my_nodeid']):
            continue
        if nodes[node_id]['online']:
            cluster_instance.cluster_info['online_lnns'].append(nodes[node_id]['lnn'])
        elif nodes[node_id]['ifs_unmounted']:
            cluster_instance.cluster_info['unmounted_lnns'].append(nodes[node_id]['lnn'])
            cluster_instance.output("Node unmounted: %s" % node_id, -1, node_id)
            # error_info['th_warns'] += 1
        else:
            cluster_instance.cluster_info['offline_lnns'].append(nodes[node_id]['lnn'])
            cluster_instance.output("Node offline: %s" % node_id, 1, node_id)
            error_info['th_errors'] += 1

    # Run the selected group of checks
    # Monitor deamon mode

    if cluster_instance.opt_info['monitor']:
        if not cluster_instance.opt_info['verbose']:
            cluster_instance.opt_info['verbose'] = 0

        # Fork in monitor mode
        try:
            pid = os.fork()
            # parent process exits here
            if pid > 0:
                sys.exit(0)
        except OSError, e:
            sys.exit("Failed to fork: $!\n")

        # Decouple from parent environment
        os.umask(0)
        os.setsid()
        os.chdir('/')

        try:
            f = open(os.devnull, 'w')
            sys.stdout = f
        except IOError:
            logging.debug("Cant write stdout to /dev/null: $!")

        stderr_logger = logging.getLogger('STDERR')
        sl = StreamToLogger(stderr_logger, logging.ERROR)
        sys.stderr = sl

        try:
            f = open(os.devnull, 'r')
            sys.stdin = f
        except IOError:
            logging.debug ("Cant read /dev/null: $!")

        # Manage the script PID file if it doesn't exist, create it
        # if it does, exit
        manage_pid_file("start")
        unit = "minutes"
        interval = opt_info['interval']
        if interval == 1:
            unit = "minute"
        hm = 60
        next_hourly_check = timestamp_in_seconds
        while True:
            error_info.update({'exec_errors': 0})     # Command execution errors
            error_info.update({'th_errors': 0})       # Threshold failures
            error_info.update({'th_leaks': 0})       # leaks

            # Determine which nodes are online and mounted
            # Create a global data structure to record events for each node
            nodes = get_node_info(opt_info['debug'])
            # Record the previous cluster events and bugs
            if cluster_instance.check_state['cycle_number'] > 1:
                cluster_instance.check_state['pre_cluster_events'] = cluster_instance.cluster_info['cluster_events']
            cluster_instance.cluster_info['cluster_events'] = {}
            cluster_instance.cluster_info['cluster_bugs'] = {}

            nodes_keys = nodes.keys()
            nodes_keys.sort(key=int)
            for node_id in nodes_keys:
                node_events = {}
                node_bugs = {}
                #Add the has to the parent hash
                cluster_instance.cluster_info['cluster_events'][node_id] = node_events
                cluster_instance.cluster_info['cluster_bugs'][node_id] = node_bugs

            online_lnns = list()
            offline_lnns = list()
            unmounted_lnns = list()

            for node_id in nodes_keys:
                if nodes[node_id]['online']:
                    online_lnns.append(nodes[node_id]['lnn'])
                else:
                    if nodes[node_id]['ifs_unmounted']:
                        unmounted_lnns.append(nodes[node_id]['lnn'])

                    else:
                        offline_lnns.append(nodes[node_id]['lnn'])
                        error_info['th_errors'] += 1

            online_lnns.sort(key=int)
            offline_lnns.sort(key=int)
            unmounted_lnns.sort(key=int)
            cluster_instance.cluster_info['online_lnns'] = online_lnns
            cluster_instance.cluster_info['offline_lnns'] = offline_lnns
            cluster_instance.cluster_info['unmounted_lnns'] = unmounted_lnns

            check_timestamp, timestamp_in_seconds = timestamp()
            cluster_instance.check_state['timestamp_in_seconds'] = timestamp_in_seconds
            tmpfile = cluster_instance.open_tmpfile(check_timestamp)

            # hourly check
            cluster_instance.check_state['hourly'] = 0
            hmin = 60
            send_to = cluster_instance.opt_info['send_to']

            if int(next_hourly_check) <= int(timestamp_in_seconds):
                if int(interval) > int(hm):
                    next_hourly_check = timestamp_in_seconds + float(interval) * hmin
                else:
                    next_hourly_check = timestamp_in_seconds + hm * hmin

                cluster_instance.check_state['hourly'] = 1
                cluster_instance.output("Hourly check at %s (batch=%s)..." % (check_timestamp, opt_info['batch']))
                for check_name in sorted(hourly_checks_to_run):
                    logging.info("Running %s" % check_name)
                    class_name = convert_to_class(check_name)
                    func_name = get_func_name(class_name)
                    class_name = check_name + '.' + class_name
                    check_instance = eval(class_name)(cluster_instance)
                    method = getattr(check_instance, func_name)
                    if not method:
                        raise Exception("Method %s not implemented" % check_name)
                    errors = method()
                    error_info['exec_errors'] += errors[0]
                    error_info['th_errors'] += errors[1]
                    if len(errors) > 2:
                        error_info['th_leaks'] += errors[2]

            # interval check
            else:
                cluster_instance.output("Interval check at %s (batch=%s)..." % (check_timestamp, opt_info['batch']))
                for check_name in sorted(int_checks_to_run):
                    logging.info("Running %s" % check_name)
                    class_name = convert_to_class(check_name)
                    func_name = get_func_name(class_name)
                    class_name = check_name + '.' + class_name
                    check_instance = eval(class_name)(cluster_instance)
                    method = getattr(check_instance, func_name)

                    if not method:
                        raise Exception("Method %s not implemented" % check_name)
                    errors = method()
                    error_info['exec_errors'] += errors[0]
                    error_info['th_errors'] += errors[1]
                    if len(errors) > 2:
                        error_info['th_leaks'] += errors[2]

            # Print this out before the tmpfile is closed
            if send_to:
                cluster_instance.output("Sending report via email to \"%s\"\n" % send_to)

            # Write out a summary of this run
            summaryfile = cluster_instance.open_summaryfile(check_timestamp)

            #Write out a log file to record history logs
            if not cluster_instance.check_state['logfile_open']:
                file_info['logfile'] = cluster_instance.open_logfile(check_timestamp)
            else:
                cluster_instance.check_state['log_buffer'] = ""

            if cluster_instance.check_state['summaryfile_open']:
                cluster_instance.check_state['tmpfile_open'] = 0
                cluster_instance.check_state['logfile_open'] = 0
                summarize(error_info, file_info, summaryfile, tmpfile, cluster_instance, timestamp_in_seconds, nodes, check_timestamp)
                cluster_instance.check_state['tmpfile_open'] = 1
                cluster_instance.check_state['logfile_open'] = 1

                #sh.close()
                cluster_instance.check_state['SUMMARY'].write(cluster_instance.check_state['summary_buffer'])
                cluster_instance.check_state['SUMMARY'].close()
                cluster_instance.check_state['summary_buffer'] = ""
                cluster_instance.check_state['summaryfile_open'] = 0

            # Close the tmp files
            if cluster_instance.check_state['tmpfile_open']:
                cluster_instance.check_state['TMP'].close()
                cluster_instance.check_state['tmp_buffer'] = ""
                cluster_instance.check_state['tmpfile_open'] = 0

            # Close the log files
            if cluster_instance.check_state['logfile_open']:
                cluster_instance.check_state['logfile_open'] = 0
                final_summarize(file_info, cluster_instance, timestamp_in_seconds, nodes)
                cluster_instance.check_state['log_buffer'] = cluster_instance.check_state['summary_buffer'] + cluster_instance.check_state['log_buffer'] + "</body></html>"
                cluster_instance.check_state['logfile_open'] = 1
                cluster_instance.check_state['LOG'].write(cluster_instance.check_state['log_buffer'])
                # copy file
                if cluster_instance.check_state['log_server_dir']:
                    path = cluster_instance.check_state['log_server_dir'] + "/%s" % file_info['logfile'].split('/')[-1]
                    try:
                        fh = open(path, 'w')
                        fh.write(cluster_instance.check_state['log_buffer'])
                    except IOError:
                        logging.info("Failed to copy logfile into logserver directory.")
                        sys.exit(0)
                    fh.close()

                cluster_instance.check_state['log_buffer'] = ""
                cluster_instance.check_state['LOG'].close()
                cluster_instance.check_state['logfile_open'] = 0

            # Send email on any error and always send the hourly report
            if send_to:
                #if (error_info['th_errors'] or cluster_instance.check_state['hourly']):
                mail_to(cluster_instance, error_info)

            if opt_info['debug_mail']:
                mail_to(cluster_instance, error_info)

            if os.path.isfile(tmpfile):
                os.unlink(tmpfile)
            if os.path.isfile(summaryfile):
                os.unlink(summaryfile)

            if cluster_instance.check_state['mail_open']:
                cluster_instance.check_state['mail_buffer'] = ""
                cluster_instance.check_state['mail_open'] = 0

            # Sleep until the next interval
            sleep = interval

            # For updating for next cycle
            cycle_number = cluster_instance.check_state['cycle_number']  # Recode the cycle number
            cluster_instance.output("Sleeping for %s %s (after cycle %s)..." % (sleep, unit, cycle_number))
            time.sleep(float(int(interval) * 60))

    else:
        if not cluster_instance.opt_info['verbose']:
            cluster_instance.opt_info['verbose'] = 1

        # Manage the script PID file if it doesn't exist, create it
        # If it does, exit
        manage_pid_file("start")

        # If Accountant is present, let's consider running it
        ia = None
        if isi_accountant is not None and uconfig is not None:
            tm_log_dir = uconfig.get_log_dir()
            # This log dir check means "are we running on the wheel?"
            if tm_log_dir != uconfig.DEFAULT_CONFIG['logdir']:
                ia = isi_accountant.IsiAccountant(output_path=tm_log_dir,
                                                  task_id=uconfig.get_task_id(),
                                                  capture_std=True)

        try:
            # for check_name in int_checks_to_run:
            # Start running each check_*

            for check_name in sorted(int_checks_to_run):
                # Count result as failed if we can't tell whether it passed
                result = isi_accountant.TestResult.failed
                test_name = "UNKNOWN"
                # check_alerts => CheckAlerts, due to python Style
                class_name = convert_to_class(check_name)
                func_name = get_func_name(class_name)
                class_name = check_name + '.' + class_name
                check_instance = eval(class_name)(cluster_instance)
                method = getattr(check_instance, func_name)

                if not method:
                    raise Exception("Method %s not implemented" % check_name)

                test_ctx = ['check_cluster', class_name, func_name]
                test_name = ":".join(test_ctx)
                if ia is not None:
                    # Tell Accountant that the following events belong to this
                    # case.
                    ia.set_context_push(test_ctx)
                    # Raise event meaning the test case has started
                    ia.start_testcase(test_name, framework="check_cluster")

                errors = method()
                if func_name in DEFER_LIST and DEFER_LIST[func_name]:
                    print "According to deferring list %s, " \
                        "skip counting errors(%s) for %s" % \
                        (DEFER_LIST, errors, func_name)
                else:
                    error_info['exec_errors'] += errors[0]
                    error_info['th_errors'] += errors[1]
                if len(errors) > 2:
                    error_info['th_leaks'] += errors[2]

                if ia is not None:
                    if sum(errors) == 0:
                        result = isi_accountant.TestResult.passed

                    # Finishing events for th ecase
                    ia.add_testcase_result(result)
                    result = None
                    ia.end_testcase(test_name)
                    test_name = None
                    # Pop the context back off the ctx stack.
                    ia.set_context_pop(test_ctx)
        finally:
            if ia is not None:
                if result is not None:
                    ia.add_testcase_result(result)
                if test_name is not None:
                    ia.end_testcase(test_name)
                ia.finalize()


    # Write out a summary of this run to the log file, and a tmp file
    summaryfile = cluster_instance.open_summaryfile(file_time)
    if cluster_instance.check_state['summaryfile_open']:
        cluster_instance.check_state['logfile_open'] = 0
        summarize(error_info, file_info, summaryfile, file_info['logfile'], cluster_instance, timestamp_in_seconds, nodes)
        cluster_instance.check_state['SUMMARY'].write(cluster_instance.check_state['summary_buffer'])
        cluster_instance.check_state['summary_buffer'] = ""
        cluster_instance.check_state['SUMMARY'].close()
        cluster_instance.check_state['summaryfile_open'] = 0

    # Close the log file
    final_summarize(file_info, cluster_instance, timestamp_in_seconds, nodes)
    cluster_instance.check_state['log_buffer'] = cluster_instance.check_state['summary_buffer'] + cluster_instance.check_state['log_buffer'] + "</body></html>"
    cluster_instance.check_state['logfile_open'] = 1
    cluster_instance.check_state['LOG'].write(cluster_instance.check_state['log_buffer'])
    if cluster_instance.check_state['log_server_dir']:
        path = cluster_instance.check_state['log_server_dir'] + "/%s" % file_info['logfile'].split('/')[-1]
        try:
            fh = open(path, 'w')
            fh.write(cluster_instance.check_state['log_buffer'])
        except IOError:
            print "Failed to copy logfile into logserver directory."
            sys.exit(0)
        fh.close()
    cluster_instance.check_state['log_buffer'] = ""
    cluster_instance.check_state['LOG'].close()
    cluster_instance.check_state['logfile_open'] = 0

    # Email the results file to send_to
    if cluster_instance.opt_info['send_to']:
        mail_to(cluster_instance, error_info)

    cluster_instance.check_state['mail_open'] = 0
    cluster_instance.check_state['mail_buffer'] = ""
    # Remove the pid file
    manage_pid_file("end")

    # Return a non-zero status if any errors or flags were encountered
    sys.exit(error_info['exec_errors'] > 0 or error_info['th_errors'] > 0)

if __name__ == "__main__":
    main()

