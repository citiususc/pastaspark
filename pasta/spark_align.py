#!/usr/bin/env python

"""Alignements in SPARK
"""

# This file is part of PASTASPARK and is forked from PASTa

# PASTASPARK like PASTA is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# Tomas F. Pena and Jose M. Abuin, University of Santiago de Compostela (Spain)
import os, stat

from configure_spark import get_sparkcontext, setSpark
from pasta import get_logger, MESSENGER
from scheduler import LightJobForProcess

_LOG = get_logger(__name__)

input_data = dict()


def set_input_data(dest, data):
    global input_data
    input_data[dest] = data

def spark_align(joblist, num_partitions):
    _LOG.debug("SPARK alignment starting")
    sc = get_sparkcontext()
    global input_data
    lightjoblist = list()
    # dir_path = os.path.dirname(os.path.realpath(__file__))
    dir_path = os.getcwd()
    for job in joblist:
        pa = job.start()

        # Create a list of pairs (job, data)
        #MESSENGER.send_info(pa[0])
        lightjoblist.append((LightJobForProcess(pa[0], pa[1], os.environ), input_data[pa[0][-1]].items()))
    # Parallelize the list of pairs (job, data)
    #rdd_joblist = sc.parallelize(lightjoblist, len(lightjoblist))

    MESSENGER.send_info("[JMAbuin] The number of light jobs is: "+str(len(lightjoblist)))

    if (num_partitions == 0):
        rdd_joblist = sc.parallelize(lightjoblist, len(lightjoblist))
    else:
        rdd_joblist = sc.parallelize(lightjoblist, num_partitions)
    # This two lines are for testing purposes only
    # for j in lightjoblist:
    #   do_align(j)

    # For each pair, do alignment
    # as output, get an RDD with pairs (out_filename, out_data)
    out = rdd_joblist.map(do_align)

    # Collect out and write the data to corresponding files in master's disk
    results = out.collect()
    for res in results:
        with open(res[0], mode='w+b') as f:
            f.writelines(res[1])

    # Switch down Spark
    _LOG.debug("SPARK alignment finished")
    _LOG.debug("Deactivating Spark")
    setSpark(False)

    # Finish the jobs
    for job in joblist:
        job.results = job.result_processor()
        job.finished_event.set()
        job.get_results()
        job.postprocess()


def do_align(job):
    global sc
    # Save data in local disc in a temporary file
    from tempfile import NamedTemporaryFile
    try:
        intmp = NamedTemporaryFile(delete=True, prefix='pasta')
        _LOG.debug('Created NamedTemporaryFile %s' % intmp.name)
    except Exception as e:
        _LOG.error('Error creating NamedTemporaryFile')

    # Write the input data in fasta format
    write_fasta_format(job[1], intmp)

    # Change the name of the input file
    job[0]._invocation[-1] = intmp.name
    # If the file is closed, it's deleted, so we flush it
    intmp.flush()
    # Save the name of the output file
    outfile = job[0]._k['stdout']

    # Run the job and Save in the rdd the name and data of the output file
    out = (outfile, job[0].runwithpipes())
    # Close and delete the file
    intmp.close()
    return out


def write_fasta_format(alignment, fdest):
    """Writes the `alignment` in FASTA format to a file"""
    for name, seq in alignment:
        fdest.write('>%s\n%s\n' % (name, seq))
