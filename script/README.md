## Environment variables (advanced or temporary options)

  - `DATADIST_FEE_MASK=0xffff`  Apply the mask if StfBuilder is configured to use the FeeID field as a O2::Subspecification (O2::Subspec = (RDH::FeeID & DATADIST_FEE_MASK))

  - `DATADIST_FILE_READ_COUNT=N`     Terminate after injecting set number of TF files. Data set will be repeated if necessary. Number of TimeFrames will be `DATADIST_FILE_READ_COUNT x Number of TFs per file`.

  - `DATADIST_DEBUG_DPL_CHAN` When defined, data sent to DPL will be checked for consistency with the O2 data model. Note: will be slow with larger TimeFrames.


### Shared memory:

  - `DATADIST_NO_MLOCK` Disable locking of the shared memory segments. Only use in testing and development!

  - `DATADIST_SHM_ZERO` When defined, shared memory segment will be zeroed before use. Because of large performance impact, only use to check for memory corruption, not in production.

  - `DATADIST_SHM_ZERO_CHECK` Define to enable checking for memory corruption in unmanaged region. Each de-allocated message will be checked for write-past-end corruption.



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
