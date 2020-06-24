% StfBuilder(1)
% Gvozden Nešković <neskovic@compeng.uni-frankfurt.de>
% September 2018

# NAME

StfBuilder – aggregate readout data into Sub Time Frames (STF)


# DESCRIPTION

**StfBuilder** (StfBuilder) is the O2 device responsible for aggregating readout data into
SubTimeFrame objects on the FLPs. On the input channel, the StfBuilder  receives HBFrame data
from the readout. On the output, the StfBuilder supports the DPL or the native data distribution
chain forwarding (to SubTimeFrameSender). Optionally, the StfBuilder can be used to write STFs
to files, before they are sent out.

# OPTIONS

## General options

**-h**, **--help**
:   Print help

**-v** **--version**
:   Print version

**--severity** level
:   Log severity level: trace, debug, info, state, warn, error, fatal, nolog.
    The default value of this parameter is '*debug*'.

**--verbosity** level
:   Log verbosity level: veryhigh, high, medium, low.
    The default value of this parameter is '*medium*'.

**--color** arg
:   Log color (true/false). The default value of this parameter is '*1*'.

**--print-options** [arg]
:   Print options.  The default value of this parameter is '*1*'.


## FairMQ device options

**--id** arg
:   Device ID (**required**).

**--io-threads** n
:   Number of I/O threads. The default value of this parameter is '*1*'.

**--transport** arg (=zeromq)
:   Transport ('zeromq'/'nanomsg'/'shmem'). The default value of this parameter is '*zeromq*'.

**--network-interface** arg
:   Network interface to bind on (e.g. eth0, ib0..., default will try to detect the interface of
    the default route).
    The default value of this parameter is '*default*'.

**--session** arg
:   Session name. All FairMQ devices in the chain must use the same session parameter.
    The default value of this parameter is '*default*'.


## FairMQ channel parser options

**--mq-config** path
:   JSON input as file.

**--channel-config** conf
:   Configuration of single or multiple channel(s) by comma separated *key=value* list.


## StfBuilder options

**--input-channel-name** name
:   Name of the input readout channel (**required**).

**--stand-alone**
:   Standalone operation. SubTimeFrames will not be forwarded to other processes.

**--max-buffered-stfs** num
:   Maximum number of buffered SubTimeFrames before starting to drop data. Unlimited: -1.
    The default value of this parameter is '*-1*'.

**--output-channel-name** name
:   Name of the output channel for non-DPL deployments (**required**).


## StfBuilder DPL options

**--dpl-channel-name** name
:   Enable DPL workflow: Specify name of the DPL output channel. NOTE: Channel specification
    is given using '*--channel-config*' option.


## SubTimeFrameBuilder data source

**--detector** name
:   Specifies the detector string for SubTimeFrame building. Allowed are: ACO, CPV,
    CTP, EMC, FT0, FV0, FDD, HMP, ITS, MCH, MFT, MID, PHS, TOF, TPC, TRD, ZDC.

**--detector-rdh** ver
:   Specifies the version of RDH. Supported versions of the RDH are: 3, 4, 5, 6.

**--detector-subspec** method
:   Specifies the which RDH fields are used for O2 SubSpecification field: Allowed methods
    are: 'cru_linkid' or 'feeid'.

## Options controlling SubTimeFrame building

**--rdh-data-check** arg (=off)
:   Enable extensive RDH verification. Permitted values: off, print, drop.

**--rdh-filter-empty-trigger**
:   Filter out empty HBFrames sent in triggered mode.

## (Sub)TimeFrame file sink options

**--data-sink-enable**
:   Enable writing of (Sub)TimeFrames to file.

**--data-sink-dir** dir
:   Specifies a root directory where (Sub)TimeFrames are to be written.
    Note: A new directory will be created here for all files of the current run.

**--data-sink-file-name** pattern
:   Specifies file name pattern: %n - file index, %D - date, %T - time.
    The default value of this parameter is '*%i.tf*'.

**--data-sink-max-stfs-per-file** num
:   Limit the number of (Sub)TimeFrames per file.s
    The default value of this parameter is '*1*'.

**--data-sink-max-file-size** arg (=4294967296)
:   Limit the target size for (Sub)TimeFrame files.'
    Note: Actual file size might exceed the limit since the (Sub)TimeFrames are written as a whole.

**--data-sink-sidecar**
:   Write a sidecar file for each (Sub)TimeFrame file containing information about data blocks
    written in the data file. Note: Useful for debugging.
    *Warning: Format of sidecar files is not stable. This option is for debugging only.*

## (Sub)TimeFrame file source options

**--data-source-enable**
:   Enable reading of (Sub)TimeFrames from files.

**--data-source-dir** arg
:   Specifies the source directory where (Sub)TimeFrame files are located. NOTE:
    Only (Sub)TimeFrame data files are allowed in this directory.

**--data-source-rate** arg (=1.0)
:   Rate of injecting new (Sub)TimeFrames (approximate). Use -1 to inject as fast as possible. (float)

**--data-source-repeat**
:   If enabled, repeatedly inject (Sub)TimeFrames into the chain.

**--data-source-regionsize**
:   Size of the memory region for (Sub)TimeFrames data in MiB. Note: make sure the
    region can fit several (Sub)TimeFrames to avoid deadlocks.

# NOTES

To enable zero-copy operation using shared memory, make sure the parameter **--transport** is set
to '*shmem*' and that all input and output channels are of '*shmem*' type as well. Also, consider
setting the **--io-threads** parameter to a value equal to, or lower than, the number of CPU cores
on your system.


