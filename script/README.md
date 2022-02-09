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


## Consul parameter for online runs

### DataDistribution Global
 - `DataDistNetworkTransport` (fmq|ucx) Use select transport for FLP-EPN data transport

### StfBuilder

 - `NumPagesInTopologicalStf` (128) Page aggregation for topological runs. Larger number of pages decreases FLP-EPN interaction rate (better performance)



### StfSender

 - `StfBufferSizeMB` (MegaBytes) Define size of DataDist buffer on FLP. Default is 32768 (MiB)

 - `UcxRdmaGapB` (8192 Bytes) Allowed gap between two messages of the same region when creating RMA txgs.
                              Larger gap creates fewer transactions, but can increase the amount of transferred data.

 - `UcxStfSenderThreadPoolSize` (0) Size of StfSender tread pool. Default 0 (number of cpu cores). Threads are not CPU intensive,
                                    they enable simultaneous transfers.


### TfBuilder

 - `MaxNumStfTransfers` (100) Define maximum number of concurrent STF transfers. Helps with long tails of TCP transfers.

 - `UcxTfBuilderThreadPoolSize` (0) Size of receiver tread pool. Default 0 (number of cpu cores)

 - `UcxNumConcurrentRmaGetOps` (8) Number of concurrent RMA Get operations per ucx thread.


### TfScheduler

 - `MaxNumTfsInBuilding` (25) Define maximum number of concurrent TFs in building per TfBuilder

 - `BuildIncompleteTfs` (true) Decision wether to build or drop incomplete TFs

 - `StaleTfTimeoutMs` (1000 ms) An incomplete TF is considered stale when the following timeout expires after the last STF is reported.

 - `IncompleteTfsMaxCnt` (100) Max number of incomplete TFs to keep before considering them stale
