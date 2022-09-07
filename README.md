# Data Distribution


Data Distribution processes (O2 devices) implement SubTimeFrame building on FLPs, and TimeFrame aggregation on EPNs.

## Components

Components of the data distribution chain are:

- `StfBuilder` (FLP): The first process (O2 device) in the chain. It receives readout data and once all HBFrames are received, forwards the STF to the next device in the chain.
- `StfSender` (FLP):  Receives STF data and related results of local processing on FLP and performs the TimeFrame aggregation.
- `TfBuilder` (EPN): Receives STFs from all `StfSender` processes, creates the full TimeFrame and forwards it to global processing.
- `TfScheduler` (service): Service discovery and active TimeFrame steering.


## Use cases

Standalone, testing and development use-cases are discussed [here](script/README.md).

## Contact

Gvozden Nešković <neskovic@compeng.uni-frankfurt.de>
