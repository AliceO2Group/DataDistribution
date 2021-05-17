## Standalone run

Standalone run is useful in following scenarios:
 - Running SubTimeFrame building with readout emulators (NO CRU HARDWARE REQUIRED)
 - Replaying recorded (Sub)TimeFrame data for full processing chain (DPL)
 - Receiving SubTimeFrames from StfBuilder or DPL workflows in StfSender

Run the chain with the `datadist_start_standalone.sh` script.
For supported options see `datadist_start_standalone.sh --help`

NOTE: Emulated chain ends with StfSender. Data is never sent to TfBuilder!

### Emulated readout data run

To use `o2-readout-exe` as the CRU emulator:
  - Make sure the Readout module is loaded in the environment (or the `o2-readout-exe` executable exists in the PATH).
  - pass `--readout` parameter

### Replaying recorded (Sub)TimeFrame data and DPL gateway configuration

  - Use `--data-source-dir` parameter to select the directory with (Sub)TimeFrame files previously recorded with StfBuilder or TfBuilder.
  - Use `--dpl-passthrough` to enable sending data to DPL workflows, defined [here](datadist_standalone_chain.json#L52-L55)


FMQDevice channel configuration is in the file `script/datadist_standalone_chain.json`.
If using the CRU emulation mode of the `o2-readout-exe` process, configuration of the emulator equipment is read from `readout_cfg/readout_emu.cfg`.

`StfBuilder` component is used to read and inject previously recorded SubTimeFrames or TimeFrames (same file and data structure).


### Example: running the chain with emulated data

```
# adapt the configuration file path!

o2-readout-exe file:///$HOME/alice/sw/slc7_x86-64/DataDistribution/latest/config/readout_emu.cfg
```

```
StfBuilder --id stf_builder-0 --transport shmem --detector TPC --dpl-channel-name=dpl-chan --channel-config "name=dpl-chan,type=push,method=bind,address=ipc:///tmp/stf-builder-dpl-pipe-0,transport=shmem,rateLogging=1" --channel-config "name=readout,type=pull,method=connect,address=ipc:///tmp/readout-pipe-0,transport=shmem,rateLogging=1"
```

```
o2-dpl-raw-proxy -b --dataspec "B:TPC/RAWDATA" --channel-config "name=readout-proxy,type=pull,method=connect,address=ipc:///tmp/stf-builder-dpl-pipe-0,transport=shmem,rateLogging=1" | o2-dpl-raw-parser -b --input-spec "B:TPC/RAWDATA"
```

Make sure that only a single instance of the proxy is started.



## Environment variables (advanced or temporary options)

  - `DATADIST_FEE_MASK=0xffff`  Apply the mask if StfBuilder is configured to use the FeeID field as a O2::Subspecification (O2::Subspec = (RDH::FeeID & DATADIST_FEE_MASK))

  - `DATADIST_FILE_READ_COUNT=N`     Terminate after injecting set number of TF files. Data set will be repeated if necessary. Number of TimeFrames will be `DATADIST_FILE_READ_COUNT x Number of TFs per file`.

  - `DATADIST_DEBUG_DPL_CHAN` When defined, data sent to DPL will be checked for consistency with the O2 data model. Note: will be slow with larger TimeFrames.