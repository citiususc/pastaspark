#!/usr/bin/env python

"""SPARK configuration
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

sc = None

FlagSpark = False

def init_spark():
    """
    Init the SPARK context if we are running in a spark cluster
    :return: The SparkContext, if we are using Spark; false otherwise
    """
    try:
        # noinspection PyUnresolvedReferences,PyUnresolvedReferences
        from pyspark import SparkConf, SparkContext
    except ImportError:
        return False
    global sc
    if not sc:
        conf = SparkConf()
        conf.set("spark.app.name", "PASTA Spark")
        sc = SparkContext(conf=conf)
        setSpark(True)
    return sc


def finish_spark():
    """
    Set the SparkContext to None so avoid to use again
    """
    global sc
    sc = None


def get_sparkcontext():
    """
    Get the actual SparkContext
    :return: the SparkContext
    """
    global sc
    return sc


def setSpark(flag):
    global FlagSpark
    FlagSpark = flag


def isSpark():
    global FlagSpark
    return FlagSpark
