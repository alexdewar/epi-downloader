# EPI downloader

This is a tool for downloading data from IHME's [EPI visualisation website]. It is a
Python script with dependencies managed by [Poetry].

While you can run the script directly, provided you have installed the dependencies with
Poetry, there are bundled executables provided on the [releases page] of this repo.

Once you have downloaded the application for your OS, you can run it directly from the
console.

First you will want to download metadata and generate an example config file, which you
can then modify to select the parameters you want to download data for. To do this, run,
e.g. on Windows:

```shell
.\epi_downloader_windows.exe --dump-config
```

This will create two files: `metadata.json` and `example_config.json`. The
`metadata.json` file contains a list of the valid options for each of the parameters.
Choose the ones you want and edit the config file accordingly. Here is an example:

```json
{
  "model": ["Diabetes mellitus"],
  "measure": ["Prevalence"],
  "year": ["2015", "2019"],
  "age": ["20-24 years"],
  "sex": ["Male", "Female"]
}
```

Once you have finished, you can download all the datasets for these parameters and
combine them into a single CSV file. To do this, run, e.g.:

```shell
.\epi_downloader_windows.exe -c my_config_file.json -o data.csv
```

This will create a new file `data.csv` which contains the combined datasets.

[EPI visualisation website]: https://vizhub.healthdata.org/epi
[Poetry]: https://python-poetry.org/
[releases page]: ../../releases

## Caching

Downloaded datasets are cached on disk to speed up future searches. If for some reason
you want to ensure the server is being queried directly (e.g. to check for new
datasets), pass the `--no-cache` flag, e.g.:

```shell
.\epi_downloader_windows.exe -c my_config_file.json -o data.csv --no-cache
```
