## Standalone run

Standalone run is useful in following schenarios:
 - Running SubTimeFrame building with readout emulators (NO CRU HARDWARE REQUIRED)
 - Replaying recorded (Sub)TimeFrame data for full processing chain (DPL)

Run the chain with the `datadist_start_standalone.sh` script.
For supported options see `datadist_start_standalone.sh --help`

### Emulated readout data run

To use `readout.exe` as the CRU emulator:
  - Make sure the Readout module is loaded in the environment (or the `readout.exe` executable exists in the PATH).
  - pass `--readout` parameter

### Replaying recorded (Sub)TimeFrame data and DPL gateway configuration

  - Use `--data-source-dir` parameter to select the directory with (Sub)TimeFrame files previously recorded with StfBuilder or TfBuilder.
  - Use `--dpl-channel` parameter to select name of the DPL channel, defined [here](datadist_standalone_chain.json#L47-L58), e.g. `--dpl-channel dpl-stf-channel`

FMQDevice channel configuration is in `script/rdatadist_standalone_chain.json`.
If using CRU emulation mode of the `readout.exe` process, configuration of the emulator is read from `readout_cfg/readout_emu.cfg`.

`StfBuilder` component is used to read and inject previously recorded SubTimeFrames or TimeFrames (same file and data structure).
