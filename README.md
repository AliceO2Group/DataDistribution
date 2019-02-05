# Data Distribution


Data Distribution processes (O2 devices) implement SubTimeFrame building on FLPs, and TimeFrame aggregation on EPNs.

## Components

Three components of the data distribution chain are:

- `StfBuilder` (FLP): The first process (O2 device) in the chain. It receives readout data and once all HBFrames are received, forwards the STF to the next device in the chain.
- `StfSender` (FLP):  Receives STF data and related results of local processing on FLP and performs the TimeFrame aggregation.
- `TfBuilder` (EPN): Receives STFs from all `StfSender` processes, creates the full TimeFrame and forwards it to global processing.

## Running with the Readout CRU emulator

Run the chain with the `start_Emulator-3FLP-3EPN.sh` script. The script supports running up to 3 independent FLP chains (CRU emulator, SufBuilder, StfSender) and up to 3 EPN TfBuilders on the local machine.
For supported options see `start_Emulator-3FLP-3EPN.sh --help`

To use `readout.exe` as the CRU emulator:
  - Make sure the Readout module is loaded in the environment (make sure the `readout.exe` executable exists).
  - pass `--readout` parameter.

O2Device channel configuration is in `config/readout-emu-flp-epn-chain.json`.  If using CRU emulation mode of the `readout.exe` process, configuration of the emulator is read from `config/readout_cfg/readout_emu.cfg` (emulation is controlled by sections `[consumer-fmq-wp5]`, `[equipment-emulator-1]`, and `[equipment-emulator-2]`). To enable testing with two equipments, set the `[equipment-emulator-2]::enabled`  option to `1`.

## Contact

Gvozden Neskovic <neskovic@compeng.uni-frankfurt.de>
